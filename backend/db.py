# backend/db.py
"""
数据库基础设施：
- SQLite 连接
- 表初始化
- 统一的连接方式（row_factory=dict）
"""
import os
import sqlite3
from typing import Iterator
from contextlib import contextmanager

# 默认数据库路径（可用环境变量覆盖）
DB_PATH = os.environ.get("FUND_DB_PATH", "./fund_assistant.db")


def get_db_path() -> str:
    return DB_PATH


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    """
    上下文数据库连接：
    with get_conn() as conn:
        ...
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    """
    初始化数据库表（幂等）
    """
    with get_conn() as conn:
        cur = conn.cursor()

        # 账户表：只保留 1 行（现金）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS account (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cash REAL NOT NULL DEFAULT 0,
                updated_at TEXT
            );
            """
        )

        # 用户表（邮箱登录）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                expires_at TEXT,
                revoked_at TEXT
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at);"
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO users (id, email, password_hash, created_at, updated_at)
            VALUES (1, 'legacy@local', '', datetime('now','localtime'), datetime('now','localtime'));
            """
        )

        # 多账户表（新）：按 user_id 隔离
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL DEFAULT 1,
                name TEXT NOT NULL,
                avatar TEXT NOT NULL DEFAULT '',
                cash REAL NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                UNIQUE (user_id, name)
            );
            """
        )

        account_info = cur.execute("PRAGMA table_info(accounts)").fetchall()
        has_user_id = any(r["name"] == "user_id" for r in account_info)
        has_avatar = any(r["name"] == "avatar" for r in account_info)
        has_user_name_unique = False
        account_indexes = cur.execute("PRAGMA index_list(accounts)").fetchall()
        for idx in account_indexes:
            if int(idx["unique"] or 0) != 1:
                continue
            idx_name = str(idx["name"])
            idx_cols = cur.execute(f"PRAGMA index_info('{idx_name}')").fetchall()
            cols = [str(c["name"]) for c in idx_cols]
            if cols == ["user_id", "name"]:
                has_user_name_unique = True
                break

        need_rebuild_accounts = (not has_user_id) or (not has_avatar) or (not has_user_name_unique)
        if need_rebuild_accounts:
            cur.execute("ALTER TABLE accounts RENAME TO accounts_legacy_auth_mig")
            cur.execute(
                """
                CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL DEFAULT 1,
                    name TEXT NOT NULL,
                    avatar TEXT NOT NULL DEFAULT '',
                    cash REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                    UNIQUE (user_id, name)
                );
                """
            )

            legacy_info = cur.execute("PRAGMA table_info(accounts_legacy_auth_mig)").fetchall()
            legacy_has_user_id = any(r["name"] == "user_id" for r in legacy_info)
            legacy_has_avatar = any(r["name"] == "avatar" for r in legacy_info)
            user_id_expr = "COALESCE(user_id, 1)" if legacy_has_user_id else "1"
            avatar_expr = "COALESCE(avatar, '')" if legacy_has_avatar else "''"

            cur.execute(
                f"""
                INSERT INTO accounts (id, user_id, name, avatar, cash, created_at, updated_at)
                SELECT
                    id,
                    {user_id_expr},
                    name,
                    {avatar_expr},
                    cash,
                    COALESCE(created_at, datetime('now','localtime')),
                    COALESCE(updated_at, datetime('now','localtime'))
                FROM accounts_legacy_auth_mig;
                """
            )
            cur.execute("DROP TABLE accounts_legacy_auth_mig")

        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_accounts_user ON accounts(user_id);"
        )

        # 当前持仓（多账户）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                account_id INTEGER NOT NULL DEFAULT 1,
                code TEXT NOT NULL,
                shares REAL NOT NULL DEFAULT 0,
                cost REAL NOT NULL DEFAULT 0,
                updated_at TEXT,
                PRIMARY KEY (account_id, code)
            );
            """
        )

        # 交易流水（多账户）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL DEFAULT 1,
                ts TEXT NOT NULL,
                code TEXT NOT NULL,
                action TEXT NOT NULL,
                amount REAL,
                price REAL,
                shares REAL,
                note TEXT
            );
            """
        )

        # 手动录入净值（NAV）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                nav REAL NOT NULL,
                ts TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quotes_code_ts ON quotes(code, ts);")
        # 手动板块覆盖（优先于自动识别）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sector_overrides (
                code TEXT PRIMARY KEY,
                sector TEXT NOT NULL,
                updated_at TEXT
            );
            """
        )
        # 自选基金（按用户隔离）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist_funds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                UNIQUE (user_id, code)
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_watchlist_user ON watchlist_funds(user_id);"
        )
        # 个股 -> 板块映射（可由持仓推导流程自动刷新）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_sector_map (
                stock_code TEXT PRIMARY KEY,
                sector TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_stock_sector_updated ON stock_sector_map(updated_at);"
        )
        # 基金板块画像（由前十大持仓按占比加权）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fund_sector_profile (
                fund_code TEXT PRIMARY KEY,
                dominant_sector TEXT NOT NULL DEFAULT '',
                sector_weights_json TEXT NOT NULL DEFAULT '{}',
                holdings_json TEXT NOT NULL DEFAULT '[]',
                source TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_fund_sector_updated ON fund_sector_profile(updated_at);"
        )
        # 兼容旧库：先确保 legacy account 有默认行
        cur.execute(
            """
            INSERT OR IGNORE INTO account (id, cash, updated_at)
            VALUES (1, 0, datetime('now'));
            """
        )

        # 兼容迁移：legacy account -> accounts(user_id=1)
        legacy_row = cur.execute("SELECT cash FROM account WHERE id = 1").fetchone()
        legacy_cash = float(legacy_row["cash"]) if legacy_row else 0.0
        accounts_count = int(
            cur.execute("SELECT COUNT(1) AS c FROM accounts WHERE user_id = 1").fetchone()["c"]
        )
        if accounts_count == 0:
            cur.execute(
                """
                INSERT INTO accounts (user_id, name, avatar, cash, created_at, updated_at)
                VALUES (1, '默认账户', '', ?, datetime('now','localtime'), datetime('now','localtime'));
                """,
                (legacy_cash,),
            )
        else:
            cur.execute(
                """
                INSERT OR IGNORE INTO accounts (user_id, name, avatar, cash, created_at, updated_at)
                VALUES (1, '默认账户', '', ?, datetime('now','localtime'), datetime('now','localtime'));
                """,
                (legacy_cash,),
            )

        # 兼容迁移：旧 positions 表(主键 code) -> 新 positions(account_id, code)
        pos_info = cur.execute("PRAGMA table_info(positions)").fetchall()
        pos_has_account_id = any(r["name"] == "account_id" for r in pos_info)
        pos_pk_cols = [
            r["name"]
            for r in sorted(pos_info, key=lambda x: int(x["pk"] or 0))
            if int(r["pk"] or 0) > 0
        ]
        need_rebuild_positions = (not pos_has_account_id) or (
            pos_pk_cols != ["account_id", "code"]
        )

        if need_rebuild_positions:
            cur.execute("ALTER TABLE positions RENAME TO positions_legacy_mig")
            cur.execute(
                """
                CREATE TABLE positions (
                    account_id INTEGER NOT NULL DEFAULT 1,
                    code TEXT NOT NULL,
                    shares REAL NOT NULL DEFAULT 0,
                    cost REAL NOT NULL DEFAULT 0,
                    updated_at TEXT,
                    PRIMARY KEY (account_id, code)
                );
                """
            )
            if pos_has_account_id:
                cur.execute(
                    """
                    INSERT INTO positions (account_id, code, shares, cost, updated_at)
                    SELECT COALESCE(account_id, 1), code, shares, cost, updated_at
                    FROM positions_legacy_mig;
                    """
                )
            else:
                cur.execute(
                    """
                    INSERT INTO positions (account_id, code, shares, cost, updated_at)
                    SELECT 1, code, shares, cost, updated_at
                    FROM positions_legacy_mig;
                    """
                )
            cur.execute("DROP TABLE positions_legacy_mig")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_positions_account ON positions(account_id);"
            )

        # 兼容迁移：旧 trades 没有 account_id 时补列
        trade_info = cur.execute("PRAGMA table_info(trades)").fetchall()
        trade_has_account_id = any(r["name"] == "account_id" for r in trade_info)
        if not trade_has_account_id:
            cur.execute("ALTER TABLE trades ADD COLUMN account_id INTEGER NOT NULL DEFAULT 1")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_trades_account_ts ON trades(account_id, ts);"
        )
