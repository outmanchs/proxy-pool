import json
import os
import sqlite3
import time


# 所有运行数据都保留在 proxy_pool 包目录内。
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "proxy_pool.sqlite3")


def _connect():
    connection = sqlite3.connect(DB_FILE, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


def init_database():
    # 检查并自动平滑迁移可能带有 UNIQUE(source_id, identity) 约束的旧表
    conn = sqlite3.connect(DB_FILE, timeout=10)
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='nodes'"
        ).fetchone()
        if row and row[0] and "UNIQUE(source_id, identity)" in row[0].replace(" ", ""):
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS nodes_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    identity TEXT NOT NULL,
                    proxy_json TEXT NOT NULL,
                    sort_order INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                INSERT INTO nodes_new (id, name, source_id, identity, proxy_json, sort_order, created_at, updated_at)
                SELECT id, name, source_id, identity, proxy_json, sort_order, created_at, updated_at FROM nodes;
                DROP TABLE nodes;
                ALTER TABLE nodes_new RENAME TO nodes;
                CREATE INDEX IF NOT EXISTS idx_nodes_sort_order ON nodes(sort_order, id);
                CREATE INDEX IF NOT EXISTS idx_nodes_identity ON nodes(source_id, identity);
                """
            )
            conn.commit()
    finally:
        conn.close()

    with _connect() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                node_count INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                identity TEXT NOT NULL,
                proxy_json TEXT NOT NULL,
                sort_order INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS node_health (
                node_id INTEGER PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
                delay_ms INTEGER,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                samples_json TEXT NOT NULL DEFAULT '[]',
                last_ok_at INTEGER,
                last_fail_at INTEGER,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS port_bindings (
                port INTEGER PRIMARY KEY,
                node_id INTEGER NOT NULL UNIQUE REFERENCES nodes(id) ON DELETE CASCADE,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_nodes_sort_order ON nodes(sort_order, id);
            CREATE INDEX IF NOT EXISTS idx_nodes_identity ON nodes(source_id, identity);
            CREATE INDEX IF NOT EXISTS idx_node_health_delay ON node_health(delay_ms);
            """
        )


def set_meta(key, value):
    with _connect() as connection:
        connection.execute(
            "INSERT INTO meta(key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            (key, str(value), int(time.time())),
        )


def get_meta(key, default=None):
    with _connect() as connection:
        row = connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default


def migrate_legacy_cache(cache_path, allowed_ports=None):
    """Import the pre-SQLite JSON cache exactly once."""
    import json

    if get_meta("legacy_json_migrated") == "1" or not os.path.exists(cache_path):
        return False
    try:
        with open(cache_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle) or {}
        all_valid_nodes = payload.get("all_valid_nodes") or payload.get("nodes") or []
        save_health_results(all_valid_nodes, payload.get("last_update"))
        bindings = {}
        for node in payload.get("nodes") or []:
            try:
                port = int(node["port"])
            except (KeyError, TypeError, ValueError):
                continue
            if allowed_ports is None or port in allowed_ports:
                bindings[port] = node["name"]
        save_port_bindings(bindings)
        set_meta("legacy_json_migrated", "1")
        return True
    except Exception:
        return False


def save_catalog(records):
    """records contain full Clash proxy dictionaries plus private ordering fields."""
    now = int(time.time())
    source_counts = {}
    with _connect() as connection:
        for sort_order, record in enumerate(records):
            source_name = record.get("_source_file") or "unknown"
            identity = record.get("_identity") or record.get("name")
            source_row = connection.execute(
                "SELECT id FROM sources WHERE name = ?", (source_name,)
            ).fetchone()
            if source_row:
                source_id = source_row["id"]
            else:
                source_id = connection.execute(
                    "INSERT INTO sources(name, node_count, updated_at) VALUES (?, 0, ?)",
                    (source_name, now),
                ).lastrowid
            proxy_record = {key: value for key, value in record.items() if not key.startswith("_")}
            connection.execute(
                """
                INSERT INTO nodes(name, source_id, identity, proxy_json, sort_order, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    source_id = excluded.source_id,
                    identity = excluded.identity,
                    proxy_json = excluded.proxy_json,
                    sort_order = excluded.sort_order,
                    updated_at = excluded.updated_at
                """,
                (
                    record["name"],
                    source_id,
                    str(identity),
                    json.dumps(proxy_record, ensure_ascii=False, separators=(",", ":")),
                    sort_order,
                    now,
                    now,
                ),
            )
            source_counts[source_id] = source_counts.get(source_id, 0) + 1

        current_names = [record["name"] for record in records]
        if current_names:
            placeholders = ",".join("?" for _ in current_names)
            connection.execute(
                f"DELETE FROM nodes WHERE name NOT IN ({placeholders})", current_names
            )
        else:
            connection.execute("DELETE FROM nodes")
        connection.execute("UPDATE sources SET node_count = 0, updated_at = ?", (now,))
        for source_id, count in source_counts.items():
            connection.execute(
                "UPDATE sources SET node_count = ? WHERE id = ?", (count, source_id)
            )


def load_catalog():
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT n.name, n.sort_order, s.name AS source_file, n.proxy_json
            FROM nodes AS n
            JOIN sources AS s ON s.id = n.source_id
            ORDER BY n.sort_order, n.id
            """
        ).fetchall()
    result = []
    for row in rows:
        item = json.loads(row["proxy_json"])
        item["name"] = row["name"]
        item["source_file"] = row["source_file"]
        item["_sort_order"] = row["sort_order"]
        result.append(item)
    return result


def load_port_bindings(available_names, allowed_ports):
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT pb.port, n.name
            FROM port_bindings AS pb
            JOIN nodes AS n ON n.id = pb.node_id
            ORDER BY pb.port
            """
        ).fetchall()
    available_names = set(available_names)
    allowed_ports = set(allowed_ports)
    bindings = {}
    used_names = set()
    for row in rows:
        port = int(row["port"])
        name = row["name"]
        if port not in allowed_ports or name not in available_names or name in used_names:
            continue
        bindings[port] = name
        used_names.add(name)
    return bindings


def save_port_bindings(bindings):
    now = int(time.time())
    with _connect() as connection:
        for port, name in bindings.items():
            node = connection.execute("SELECT id FROM nodes WHERE name = ?", (name,)).fetchone()
            if not node:
                continue
            connection.execute(
                "DELETE FROM port_bindings WHERE node_id = ? AND port <> ?",
                (node["id"], int(port)),
            )
            connection.execute(
                "INSERT INTO port_bindings(port, node_id, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(port) DO UPDATE SET node_id = excluded.node_id, updated_at = excluded.updated_at",
                (int(port), node["id"], now),
            )


def delete_port_binding(port):
    with _connect() as connection:
        connection.execute("DELETE FROM port_bindings WHERE port = ?", (int(port),))


def load_ordered_candidates(exclude_names):
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT n.name, n.sort_order, s.name AS source_file
            FROM nodes AS n
            JOIN sources AS s ON s.id = n.source_id
            ORDER BY n.sort_order, n.id
            """
        ).fetchall()
    excluded = set(exclude_names or ())
    result = []
    seen = set()
    for row in rows:
        name = row["name"]
        if name in excluded or name in seen:
            continue
        seen.add(name)
        result.append({"name": name, "source_file": row["source_file"]})
    return result


def save_health_results(nodes, last_update=None):
    now = int(time.time())
    with _connect() as connection:
        for node in nodes:
            name = node.get("name")
            if not name:
                continue
            delay = node.get("delay")
            failures = int(node.get("consecutive_failures") or 0)
            samples = node.get("_health_samples") or []
            row = connection.execute("SELECT id FROM nodes WHERE name = ?", (name,)).fetchone()
            if not row:
                continue
            connection.execute(
                """
                INSERT INTO node_health(
                    node_id, delay_ms, consecutive_failures, samples_json,
                    last_ok_at, last_fail_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    delay_ms = excluded.delay_ms,
                    consecutive_failures = excluded.consecutive_failures,
                    samples_json = excluded.samples_json,
                    last_ok_at = COALESCE(excluded.last_ok_at, last_ok_at),
                    last_fail_at = COALESCE(excluded.last_fail_at, last_fail_at),
                    updated_at = excluded.updated_at
                """,
                (
                    row["id"],
                    int(delay) if isinstance(delay, (int, float)) else None,
                    failures,
                    json.dumps(samples, separators=(",", ":")),
                    now if delay is not None else None,
                    None if delay is not None else now,
                    now,
                ),
            )
    if last_update:
        set_meta("last_update", last_update)


def load_health_results(available_names):
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT n.name, n.sort_order, s.name AS source_file,
                   h.delay_ms, h.consecutive_failures, h.samples_json
            FROM nodes AS n
            JOIN sources AS s ON s.id = n.source_id
            JOIN node_health AS h ON h.node_id = n.id
            ORDER BY h.consecutive_failures, h.delay_ms IS NULL, h.delay_ms, n.sort_order, n.id
            """
        ).fetchall()
    available_names = set(available_names or ())
    result = []
    for row in rows:
        name = row["name"]
        if available_names and name not in available_names:
            continue
        if row["delay_ms"] is None:
            continue
        result.append(
            {
                "name": name,
                "source_file": row["source_file"],
                "delay": int(row["delay_ms"]),
                "consecutive_failures": int(row["consecutive_failures"]),
                "_health_samples": json.loads(row["samples_json"] or "[]"),
            }
        )
    return result


def load_observability_data():
    """可观测性面板：全量节点健康 + 机场归属 + 时间戳（只读）。"""
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT n.name, n.sort_order, s.name AS source_file,
                   h.delay_ms, h.consecutive_failures,
                   h.last_ok_at, h.last_fail_at, h.updated_at
            FROM nodes AS n
            JOIN sources AS s ON s.id = n.source_id
            LEFT JOIN node_health AS h ON h.node_id = n.id
            ORDER BY n.sort_order, n.id
            """
        ).fetchall()
        bindings = connection.execute(
            """
            SELECT pb.port, n.name
            FROM port_bindings AS pb
            JOIN nodes AS n ON n.id = pb.node_id
            """
        ).fetchall()
    nodes = [dict(r) for r in rows]
    port_by_name = {r["name"]: int(r["port"]) for r in bindings}
    for n in nodes:
        n["port"] = port_by_name.get(n["name"])
    return nodes


def runtime_statistics():
    with _connect() as connection:
        node_count = connection.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        source_count = connection.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
        binding_count = connection.execute("SELECT COUNT(*) FROM port_bindings").fetchone()[0]
        healthy_count = connection.execute(
            "SELECT COUNT(*) FROM node_health WHERE delay_ms IS NOT NULL AND consecutive_failures = 0"
        ).fetchone()[0]
        suspect_count = connection.execute(
            "SELECT COUNT(*) FROM node_health WHERE consecutive_failures > 0"
        ).fetchone()[0]
    return {
        "catalog_nodes": int(node_count),
        "sources": int(source_count),
        "persisted_bindings": int(binding_count),
        "persisted_healthy": int(healthy_count),
        "persisted_suspect": int(suspect_count),
    }
