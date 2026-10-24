import asyncio
import base64
import re
import time
import urllib.parse
from . import runtime

from .runtime import (
    GATEWAY_AUTH_ENABLED,
    GATEWAY_PASSWORD,
    GATEWAY_ROTATION_INTERVAL,
    GATEWAY_ROTATION_MODE,
    NODE_ACTIVITY,
    NODE_ACTIVITY_TTL,
    RELAY_TIMEOUT,
    SESSION_MAP,
    SHUTDOWN_TIMEOUT,
    SOCKS5_AUTH_ENABLED,
    STATE_LOCK,
    _build_auth_session_params,
    GATEWAY_MAX_ATTEMPTS,
    _get_valid_nodes_snapshot,
    _is_ip_whitelisted,
    _mark_port_active,
    _release_gateway_slot,
    report_gateway_failure,
    report_gateway_success,
    _store_session,
    _try_acquire_gateway_slot,
    delay_index,
    rotation_index,
)

def _pick_upstream(session_params):
    return _pick_upstream_excluding(session_params, set())

def _pick_upstream_excluding(session_params, excluded_ports):
    global rotation_index, delay_index
    valid = _get_valid_nodes_snapshot()
    if excluded_ports:
        valid = [node for node in valid if node["port"] not in excluded_ports]
    if not valid: return None, None
    valid_ports = {node["port"] for node in valid}

    session_key = session_params.get("session_key") if session_params else None
    user_ttl = session_params.get("ttl") if session_params else None
    ttl = user_ttl if user_ttl is not None else GATEWAY_ROTATION_INTERVAL

    if GATEWAY_ROTATION_MODE == "delay" and session_key and ttl > 0:
        with STATE_LOCK:
            session = SESSION_MAP.get(session_key)
        if session and session["expires_at"] > time.time() and session["node"][1] in valid_ports:
            return session["node"]
        if session:
            with STATE_LOCK:
                SESSION_MAP.pop(session_key, None)
        
        with STATE_LOCK:
            if not valid:
                return None, None
            node = valid[delay_index % len(valid)]
            delay_index += 1
        host, port = "127.0.0.1", node["port"]
        _store_session(session_key, (host, port), time.time() + ttl)
        return host, port

    elif GATEWAY_ROTATION_MODE == "per_session" and session_key:
        with STATE_LOCK:
            session = SESSION_MAP.get(session_key)
        if session and session["node"][1] in valid_ports:
            return session["node"]
        if session:
            with STATE_LOCK:
                SESSION_MAP.pop(session_key, None)
        with STATE_LOCK:
            if not valid:
                return None, None
            node = valid[rotation_index % len(valid)]
            rotation_index += 1
        host, port = "127.0.0.1", node["port"]
        _store_session(session_key, (host, port), float("inf"))
        return host, port

    with STATE_LOCK:
        if not valid:
            return None, None
        node = valid[rotation_index % len(valid)]
        rotation_index += 1
    return "127.0.0.1", node["port"]

def _store_node_activity(port: int, source_kind: str, source_id: str, target_host: str, target_port: int, method: str = ""):
    with STATE_LOCK:
        NODE_ACTIVITY[port] = {
            "source_kind": source_kind,
            "source_id": source_id,
            "target_url": f"{target_host}:{target_port}" if target_port else target_host,
            "method": method,
            "updated_at": time.time(),
            "expires_at": time.time() + NODE_ACTIVITY_TTL
        }

def _describe_source(params, client_ip: str, client_port: int):
    if params and params.get("sessid"):
        return "钱包/会话", params.get("session_key", "")
    if _is_ip_whitelisted(client_ip):
        return "白名单", f"{client_ip}:{client_port}"
    if params and params.get("session_key"):
        return "任务/用户", params.get("session_key", "")
    return "未知", f"{client_ip}:{client_port}"

def _check_auth(raw_headers: bytes, client_ip: str, client_port: int = 0):
    headers_text = raw_headers.decode("utf-8", errors="ignore")
    auth_match = re.search(r"Proxy-Authorization:\s*Basic\s+(\S+)", headers_text, re.IGNORECASE)

    if auth_match:
        try:
            decoded = base64.b64decode(auth_match.group(1)).decode("utf-8")
            if ":" not in decoded:
                return False, {}
            username, password = decoded.split(":", 1)
            if password != GATEWAY_PASSWORD:
                return False, {}
            return True, _build_auth_session_params(username, client_port)
        except Exception:
            return False, {}

    if _is_ip_whitelisted(client_ip):
        return True, {"session_key": f"w:{client_ip}:{client_port}"}
    if not GATEWAY_AUTH_ENABLED:
        return True, {"session_key": f"noauth:{client_port}"}
    return False, {}

def _strip_proxy_authorization(raw_headers: bytes) -> bytes:
    lines = raw_headers.split(b"\r\n")
    filtered = [
        line for line in lines
        if not line.lower().startswith(b"proxy-authorization:")
    ]
    return b"\r\n".join(filtered)


def _safe_close_writer(writer: asyncio.StreamWriter):
    try:
        writer.close()
    except Exception:
        pass

async def _safe_wait_closed(writer: asyncio.StreamWriter):
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=SHUTDOWN_TIMEOUT)
    except Exception:
        pass

async def _async_relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while True:
            data = await asyncio.wait_for(reader.read(65536), timeout=RELAY_TIMEOUT)
            if not data: break
            writer.write(data)
            await writer.drain()
    except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, OSError): pass
    except Exception: pass


async def _open_upstream_with_retry(params, connect_headers=None):
    excluded_ports = set()
    last_error = "no valid nodes"
    for _ in range(max(1, GATEWAY_MAX_ATTEMPTS)):
        host, port = _pick_upstream_excluding(params, excluded_ports)
        if not host:
            break
        excluded_ports.add(port)
        reader = writer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5
            )
            if connect_headers is not None:
                writer.write(connect_headers)
                await writer.drain()
                response = await asyncio.wait_for(
                    reader.readuntil(b"\r\n\r\n"), timeout=10
                )
                status = response.split(b" ", 2)[1] if len(response.split(b" ", 2)) > 1 else b""
                if status != b"200":
                    raise OSError(f"upstream CONNECT returned {status!r}")
            return reader, writer, port, None
        except Exception as error:
            last_error = str(error)
            report_gateway_failure(port, last_error)
            if writer is not None:
                _safe_close_writer(writer)
                await _safe_wait_closed(writer)
    return None, None, None, last_error


async def handle_http_gateway(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
    up_port = None
    up_writer = None
    slot_acquired = _try_acquire_gateway_slot()
    if not slot_acquired:
        client_writer.write(b"HTTP/1.1 503 Service Unavailable\r\n\r\nProxy gateway is busy.")
        await client_writer.drain()
        _safe_close_writer(client_writer)
        await _safe_wait_closed(client_writer)
        return
    try:
        client_ip = client_writer.get_extra_info('peername')[0]
        raw_headers = await client_reader.readuntil(b"\r\n\r\n")
        if not raw_headers: return
        
        first_line = raw_headers.split(b"\r\n")[0].decode("utf-8", errors="ignore")
        method = first_line.split(" ")[0].upper()

        client_port = client_writer.get_extra_info('peername')[1]
        auth_ok, params = _check_auth(raw_headers, client_ip, client_port)
        if not auth_ok:
            client_writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"Proxy Pool\"\r\n\r\n")
            await client_writer.drain()
            return

        upstream_headers = _strip_proxy_authorization(raw_headers)

        connect_headers = upstream_headers if method == "CONNECT" else None
        up_reader, up_writer, up_port, connect_error = await _open_upstream_with_retry(
            params, connect_headers
        )
        if up_writer is None:
            message = connect_error.encode("utf-8", errors="ignore")[:180]
            client_writer.write(
                b"HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n"
                + message
            )
            await client_writer.drain()
            return

        _mark_port_active(up_port, 1)
        
        if method == "CONNECT":
            client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await client_writer.drain()
            connect_target = first_line.split(" ")[1]
            if ":" in connect_target:
                target_host, target_port_text = connect_target.rsplit(":", 1)
                target_port = int(target_port_text) if target_port_text.isdigit() else 443
            else:
                target_host, target_port = connect_target, 443
            source_kind, source_id = _describe_source(params, client_ip, client_port)
            _store_node_activity(up_port, source_kind, source_id, target_host, target_port, method="CONNECT")
            report_gateway_success(up_port)
        else:
            up_writer.write(upstream_headers)
            await up_writer.drain()
            request_target = first_line.split(" ")[1] if len(first_line.split(" ")) >= 2 else ""
            target_host = request_target
            target_port = 0
            source_kind, source_id = _describe_source(params, client_ip, client_port)
            _store_node_activity(up_port, source_kind, source_id, target_host, target_port, method=method)

        await asyncio.gather(
            _async_relay(client_reader, up_writer),
            _async_relay(up_reader, client_writer),
            return_exceptions=True
        )
    except Exception: pass
    finally:
        if up_port:
            _mark_port_active(up_port, -1)
        if up_writer is not None:
            _safe_close_writer(up_writer)
            await _safe_wait_closed(up_writer)
        _safe_close_writer(client_writer)
        await _safe_wait_closed(client_writer)
        if slot_acquired:
            _release_gateway_slot()

async def handle_mixed_gateway(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
    try:
        first_byte = await client_reader.readexactly(1)
        class _PrefixedReader:
            def __init__(self, prefix, reader):
                self._buf, self._reader = prefix, reader
            async def read(self, n=-1):
                if self._buf: data, self._buf = self._buf, b""; return data
                return await self._reader.read(n)
            async def readexactly(self, n):
                if self._buf:
                    head, self._buf = self._buf[:n], self._buf[n:]
                    if len(head) == n: return head
                    return head + await self._reader.readexactly(n - len(head))
                return await self._reader.readexactly(n)
            async def readuntil(self, sep=b"\n"):
                if self._buf: data = self._buf + await self._reader.readuntil(sep); self._buf = b""; return data
                return await self._reader.readuntil(sep)
        prefixed = _PrefixedReader(first_byte, client_reader)
        if first_byte[0] == 0x05:
            await handle_socks5_gateway(prefixed, client_writer)
        else:
            await handle_http_gateway(prefixed, client_writer)
    except Exception:
        pass
    finally:
        _safe_close_writer(client_writer)
        await _safe_wait_closed(client_writer)

async def handle_socks5_gateway(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
    up_port = None
    up_writer = None
    slot_acquired = _try_acquire_gateway_slot()
    if not slot_acquired:
        _safe_close_writer(client_writer)
        await _safe_wait_closed(client_writer)
        return
    try:
        client_ip = client_writer.get_extra_info('peername')[0]
        client_port = client_writer.get_extra_info('peername')[1]
        first_byte = await client_reader.readexactly(1)
        if first_byte[0] != 0x05: return
        
        second_byte = await client_reader.readexactly(1)
        nmethods = second_byte[0]
        methods = await client_reader.readexactly(nmethods)

        params = {}
        if SOCKS5_AUTH_ENABLED and not _is_ip_whitelisted(client_ip):
            if 0x02 not in methods:
                client_writer.write(b"\x05\xff")
                await client_writer.drain()
                return
            client_writer.write(b"\x05\x02")
            await client_writer.drain()

            auth_header = await client_reader.readexactly(2)
            if auth_header[0] != 0x01: return
            
            username = (await client_reader.readexactly(auth_header[1])).decode("utf-8", errors="ignore")
            plen = (await client_reader.readexactly(1))[0]
            password = (await client_reader.readexactly(plen)).decode("utf-8", errors="ignore")

            if password != GATEWAY_PASSWORD:
                client_writer.write(b"\x01\x01")
                await client_writer.drain()
                return
                
            client_writer.write(b"\x01\x00")
            await client_writer.drain()
            params = _build_auth_session_params(username, client_port)
        else:
            if 0x00 not in methods:
                client_writer.write(b"\x05\xff")
                await client_writer.drain()
                return
            client_writer.write(b"\x05\x00")
            await client_writer.drain()
            params = {"session_key": f"socks5:{client_ip}:{client_port}"}

        req = await client_reader.readexactly(4)
        if req[0] != 0x05 or req[1] != 0x01: return
        
        atyp = req[3]
        if atyp == 0x01:
            addr = await client_reader.readexactly(4)
            target_host = ".".join(str(b) for b in addr)
        elif atyp == 0x03:
            dlen = (await client_reader.readexactly(1))[0]
            target_host = (await client_reader.readexactly(dlen)).decode("utf-8", errors="ignore")
        elif atyp == 0x04:
            addr = await client_reader.readexactly(16)
            target_host = ":".join(f"{addr[i]:02x}{addr[i+1]:02x}" for i in range(0, 16, 2))
        else: return
            
        port_bytes = await client_reader.readexactly(2)
        target_port = (port_bytes[0] << 8) | port_bytes[1]
        source_kind, source_id = _describe_source(params, client_ip, client_port)

        target_authority = f"[{target_host}]:{target_port}" if atyp == 0x04 else f"{target_host}:{target_port}"
        connect_req = f"CONNECT {target_authority} HTTP/1.1\r\nHost: {target_authority}\r\n\r\n".encode()
        up_reader, up_writer, up_port, _error = await _open_upstream_with_retry(
            params, connect_req
        )
        if up_writer is None: return

        _mark_port_active(up_port, 1)

        client_writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
        await client_writer.drain()
        _store_node_activity(up_port, source_kind, source_id, target_host, target_port, method="SOCKS5")
        report_gateway_success(up_port)

        await asyncio.gather(
            _async_relay(client_reader, up_writer),
            _async_relay(up_reader, client_writer),
            return_exceptions=True
        )
    except Exception: pass
    finally:
        if up_port:
            _mark_port_active(up_port, -1)
        if up_writer is not None:
            _safe_close_writer(up_writer)
            await _safe_wait_closed(up_writer)
        _safe_close_writer(client_writer)
        await _safe_wait_closed(client_writer)
        if slot_acquired:
            _release_gateway_slot()

