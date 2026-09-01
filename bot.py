#!/usr/bin/env python3
"""
🔮 telegram-multichain-monitor — Optimized
High-performance multi-chain wallet monitor.
25 chains | All tokens | IN+OUT | Burn detection | Auto chain | Admin
"""

from __future__ import annotations

import asyncio
import argparse
import base64
import html
import inspect
import json
import logging
import os
import re
import sqlite3
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from typing import Dict, List, Optional, Set, Tuple, Any
from zoneinfo import ZoneInfo, available_timezones

import httpx
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command
from aiogram.types import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from web3 import AsyncWeb3, Web3
from web3.middleware import ExtraDataToPOAMiddleware
from web3.providers import AsyncHTTPProvider

# ═══════════════════════════════════════════════════
# ENVIRONMENT
# ═══════════════════════════════════════════════════

def _load_env():
    path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip("""'""")
            if k:
                os.environ.setdefault(k, v)

_load_env()

# ═══════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════

class Config:
    TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    ADMIN_ID: Optional[int] = int(os.getenv("ADMIN_CHAT_ID")) if os.getenv("ADMIN_CHAT_ID") else None
    DB_PATH: str = os.getenv("DB_PATH", "data/monitor.db")
    TON_API_KEY: str = os.getenv("TON_API_KEY", "")
    EXTRA_EVM: str = os.getenv("EXTRA_EVM_CHAINS", "")
    AUTO_DELETE: int = int(os.getenv("AUTO_DELETE_DELAY", "30"))
    TELEGRAM_TIMEOUT: int = int(os.getenv("TELEGRAM_REQUEST_TIMEOUT", "60"))
    MAX_HISTORY: int = 50
    NOTIF_BATCH: int = 100
    ADDR_CACHE_TTL: int = 30  # seconds
    USER_CACHE_TTL: int = 60  # seconds

# ═══════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════

class _RedactFilter(logging.Filter):
    _id_pat = re.compile(r"\bid=\d+\b")
    _url_pat = re.compile(r"https?://[^\s)]+")
    def filter(self, rec: logging.LogRecord) -> bool:
        if ("Unclosed client session" in rec.getMessage()
                or "Unclosed connector" in rec.getMessage()
                or rec.getMessage().startswith("client_session:")
                or "Successfully disconnected from:" in rec.getMessage()):
            return False
        if rec.levelno == logging.WARNING:
            return False
        rec.msg = self._url_pat.sub("[rpc]", self._id_pat.sub("id=***", rec.getMessage()))
        rec.args = ()
        return True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
# Keep routine framework polling/update messages out of the console.
logging.getLogger("aiogram").setLevel(logging.WARNING)
for h in logging.getLogger().handlers:
    h.addFilter(_RedactFilter())
logger = logging.getLogger("tmm")


def _asyncio_exception_handler(loop, context):
    """Ignore expected Windows socket resets during remote disconnects."""
    exc = context.get("exception")
    winerror = getattr(exc, "winerror", None)
    if isinstance(exc, OSError) and winerror in {10053, 10054, 995}:
        return
    loop.default_exception_handler(context)

# ═══════════════════════════════════════════════════
# CHAIN REGISTRY
# ═══════════════════════════════════════════════════

@dataclass(frozen=True)
class ChainCfg:
    key: str
    name: str
    emoji: str
    type: str
    native: str
    poll: int
    rpc: str = ""
    rpc_fallbacks: Tuple[str, ...] = ()
    api: str = ""
    explorer: str = ""
    decimals: int = 18
    chain_id: Optional[int] = None

_CHAINS: List[ChainCfg] = [
    ChainCfg("ethereum",  "Ethereum",   "💠", "evm", "ETH",  15, "https://eth.llamarpc.com",                    ("https://ethereum.publicnode.com","https://1rpc.io/eth","https://rpc.flashbots.net"),                   "", "https://etherscan.io/tx/"),
    ChainCfg("bsc",       "BSC",        "🟡", "evm", "BNB",   3, "https://bsc-dataseed.binance.org/",           ("https://bsc.publicnode.com","https://1rpc.io/bnb"),                                                   "", "https://bscscan.com/tx/"),
    ChainCfg("polygon",   "Polygon",    "🟣", "evm", "MATIC", 2, "https://polygon.llamarpc.com",                ("https://polygon.drpc.org","https://polygon.publicnode.com","https://1rpc.io/matic"),                  "", "https://polygonscan.com/tx/"),
    ChainCfg("arbitrum",  "Arbitrum",   "🔵", "evm", "ETH",   2, "https://arb1.arbitrum.io/rpc",                ("https://arbitrum-one.publicnode.com","https://1rpc.io/arb"),                                          "", "https://arbiscan.io/tx/"),
    ChainCfg("optimism",  "Optimism",   "🔴", "evm", "ETH",   2, "https://mainnet.optimism.io",                 ("https://optimism.publicnode.com","https://1rpc.io/op"),                                               "", "https://optimistic.etherscan.io/tx/"),
    ChainCfg("base",      "Base",       "🔷", "evm", "ETH",   2, "https://mainnet.base.org",                    ("https://base.publicnode.com","https://1rpc.io/base"),                                                 "", "https://basescan.org/tx/"),
    ChainCfg("avalanche", "Avalanche",  "❄️", "evm", "AVAX",  2, "https://api.avax.network/ext/bc/C/rpc",       ("https://avalanche-c-chain.publicnode.com","https://1rpc.io/avax"),                                    "", "https://snowtrace.io/tx/"),
    ChainCfg("fantom",    "Fantom",     "👻", "evm", "FTM",   3, "https://rpc.ftm.tools",                       ("https://rpcapi.fantom.network/","https://rpc.ankr.com/fantom/","https://fantom-mainnet.public.blastapi.io/","https://fantom.publicnode.com","https://1rpc.io/ftm","https://fantom.blockpi.network/v1/rpc/public","https://fantom.drpc.org/","https://rpc.fantom.gateway.fm","https://endpoints.omniatech.io/v1/fantom/mainnet/public"),                                                "", "https://ftmscan.com/tx/"),
    ChainCfg("zksync",    "zkSync",     "⚡", "evm", "ETH",   5, "https://mainnet.era.zksync.io",               ("https://zksync-era.publicnode.com","https://1rpc.io/zksync"),                                         "", "https://explorer.zksync.io/tx/"),
    ChainCfg("linea",     "Linea",      "📐", "evm", "ETH",   5, "https://rpc.linea.build",                     ("https://linea-mainnet.publicnode.com","https://1rpc.io/linea"),                                       "", "https://lineascan.build/tx/"),
    ChainCfg("scroll",    "Scroll",     "📜", "evm", "ETH",   5, "https://rpc.scroll.io",                       ("https://scroll.publicnode.com","https://1rpc.io/scroll"),                                             "", "https://scrollscan.com/tx/"),
    ChainCfg("mantle",    "Mantle",     "🧱", "evm", "MNT",   5, "https://rpc.mantle.xyz",                      ("https://mantle.publicnode.com","https://1rpc.io/mantle"),                                             "", "https://mantlescan.xyz/tx/"),
    ChainCfg("gnosis",    "Gnosis",     "🦉", "evm", "xDAI",  5, "https://rpc.gnosischain.com",                 ("https://gnosis.publicnode.com","https://1rpc.io/gnosis"),                                             "", "https://gnosisscan.io/tx/"),
    ChainCfg("celo",      "Celo",       "🌍", "evm", "CELO",  5, "https://forno.celo.org",                      ("https://celo.publicnode.com","https://1rpc.io/celo"),                                                 "", "https://celoscan.io/tx/"),
    ChainCfg("cronos",    "Cronos",     "🦁", "evm", "CRO",   5, "https://evm.cronos.org",                      ("https://cronos-evm-rpc.publicnode.com","https://1rpc.io/cro"),                                        "", "https://cronoscan.com/tx/"),
    ChainCfg("moonbeam",  "Moonbeam",   "🌙", "evm", "GLMR",  5, "https://rpc.api.moonbeam.network",            ("https://moonbeam.api.onfinality.io/public","https://moonbeam.unitedbloc.com","https://moonbeam.publicnode.com","https://1rpc.io/glmr"),                                             "", "https://moonscan.io/tx/"),
    ChainCfg("robinhood", "Robinhood",  "RH", "evm", "ETH",   2, "https://rpc.mainnet.chain.robinhood.com",     (),                                                                                                       "", "https://robinhoodchain.blockscout.com/tx/", chain_id=4663),
    ChainCfg("merlin",    "Merlin",     "🔥", "evm", "BTC",   3, "https://rpc.merlinchain.io",                  (),                                                                                                       "", "https://scan.merlinchain.io/tx/", chain_id=4200),
    ChainCfg("hyperevm",  "HyperEVM",   "H",  "evm", "HYPE",  2, "https://rpc.hyperliquid.xyz/evm",             (),                                                                                                       "", "https://hyperevmscan.io/tx/", chain_id=999),
    ChainCfg("ton",       "TON",        "💎", "ton", "TON",  10, "", (), "https://tonapi.io/v2",                    "https://tonviewer.com/transaction/", 9),
    ChainCfg("xlm",       "Stellar",    "✨", "xlm", "XLM",  10, "", (), "https://horizon.stellar.org",             "https://stellar.expert/explorer/public/tx/", 7),
    ChainCfg("sol",       "Solana",     "🟣", "sol", "SOL",   5, "https://api.mainnet-beta.solana.com",         (),                                                                                                       "", "https://solscan.io/tx/", 9),
    ChainCfg("tron",      "TRON",       "🔴", "tron","TRX",   5, "", (), "https://api.trongrid.io/v1",              "https://tronscan.org/#/transaction/", 6),
    ChainCfg("btc",       "Bitcoin",    "🟠", "btc", "BTC",  30, "", (), "https://mempool.space/api",               "https://mempool.space/tx/", 8),
    ChainCfg("sui",       "Sui",        "SUI","sui", "SUI",   5, "https://fullnode.mainnet.sui.io:443",         (),                                                                                                       "", "https://suivision.xyz/txblock/", 9),
]

# Parse extra EVM chains from env
if Config.EXTRA_EVM:
    try:
        for k, v in json.loads(Config.EXTRA_EVM).items():
            if not isinstance(v, dict):
                continue
            _CHAINS.append(ChainCfg(
                key=k.lower(), name=v["name"], emoji=v.get("emoji", "⛓️"),
                type="evm", native=v["native"], poll=v.get("poll", 5),
                rpc=v["rpc"], explorer=v["explorer"],
                rpc_fallbacks=tuple(v.get("rpc_fallbacks", [])),
                chain_id=v.get("chain_id"),
            ))
    except Exception as exc:
        raise RuntimeError("EXTRA_EVM_CHAINS invalid JSON") from exc

# Build lookup maps for O(1) access
CHAINS_BY_KEY: Dict[str, ChainCfg] = {c.key: c for c in _CHAINS}
EVM_KEYS: Tuple[str, ...] = tuple(c.key for c in _CHAINS if c.type == "evm")

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "name",     "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol",   "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}],  "type": "function"},
]

POPULAR_TZS = [
    "UTC", "Asia/Kolkata", "Asia/Dubai", "Asia/Singapore", "Asia/Tokyo",
    "Asia/Shanghai", "Europe/London", "Europe/Paris", "Europe/Berlin",
    "America/New_York", "America/Los_Angeles", "America/Chicago",
    "Australia/Sydney", "Africa/Lagos", "America/Sao_Paulo",
]

# ═══════════════════════════════════════════════════
# BURN DETECTION (O(1) lookup via frozenset)
# ═══════════════════════════════════════════════════

_BURN_EVM = frozenset(a.lower() for a in {
    "0x0000000000000000000000000000000000000000",
    "0x0000000000000000000000000000000000000001",
    "0x0000000000000000000000000000000000000002",
    "0x0000000000000000000000000000000000000003",
    "0x0000000000000000000000000000000000000004",
    "0x0000000000000000000000000000000000000005",
    "0x0000000000000000000000000000000000000006",
    "0x0000000000000000000000000000000000000007",
    "0x0000000000000000000000000000000000000008",
    "0x0000000000000000000000000000000000000009",
    "0x000000000000000000000000000000000000000a",
    "0x000000000000000000000000000000000000000b",
    "0x000000000000000000000000000000000000000c",
    "0x000000000000000000000000000000000000000d",
    "0x000000000000000000000000000000000000000e",
    "0x000000000000000000000000000000000000000f",
    "0x000000000000000000000000000000000000dead",
    "0x000000000000000000000000000000000000dEaD",
    "0x000000000000000000000000000000000000DeAd",
    "0x000000000000000000000000000000000000DEAD",
    "0x0000000000000000000000000000000000001111",
    "0x0000000000000000000000000000000000000000000000000000000000000000",
    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
})
_BURN_SOL = frozenset(a.lower() for a in {
    "11111111111111111111111111111111",
    "So11111111111111111111111111111111111111112",
    "1nc1nerator11111111111111111111111111111111",
})
_BURN_TRON = frozenset(a.lower() for a in {
    "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb",
    "TNullAddress1111111111111111111111111111111",
})
_BURN_BTC = frozenset(a.lower() for a in {
    "1BitcoinEaterAddressDontSendf59kuE",
    "1111111111111111111114oLvT2",
})

_BURN_MAP: Dict[str, frozenset] = {
    "evm": _BURN_EVM, "sol": _BURN_SOL, "tron": _BURN_TRON, "btc": _BURN_BTC,
}

def is_burn(chain: str, addr: str) -> bool:
    return addr.lower().strip() in _BURN_MAP.get(chain, frozenset())

def burn_warn(chain_key: str, addr: str) -> str:
    cfg = CHAINS_BY_KEY.get(chain_key)
    name = cfg.name if cfg else chain_key.upper()
    return (
        f"⚠️ <b>Burn Address Detected!</b>\n\n"
        f"<code>{addr}</code>\n\n"
        f"This is a known burn/dead address on <b>{name}</b>.\n"
        f"Tokens sent here are permanently lost and cannot be recovered.\n\n"
        f"❌ <b>This address cannot be monitored.</b>"
    )

# ═══════════════════════════════════════════════════
# AUTO-DELETE
# ═══════════════════════════════════════════════════

async def _schedule_delete(bot: Bot, chat: int, umsg: int, bmsg: int, delay: int):
    await asyncio.sleep(delay)
    for mid in (umsg, bmsg):
        try:
            await bot.delete_message(chat, mid)
        except Exception:
            pass

async def _telegram_call(factory, attempts: int = 3):
    """Run a Telegram request with bounded retries for transient network errors."""
    for attempt in range(1, attempts + 1):
        try:
            return await factory()
        except TelegramRetryAfter as exc:
            if attempt == attempts:
                logger.warning("Telegram rate limit persisted after %d attempts", attempts)
                return None
            await asyncio.sleep(exc.retry_after)
        except (TelegramNetworkError, TelegramServerError, asyncio.TimeoutError) as exc:
            if attempt == attempts:
                logger.warning("Telegram request failed after %d attempts: %s", attempts, exc)
                return None
            await asyncio.sleep(min(2 ** attempt, 8))


async def reply_del(msg: types.Message, text: str, delay: int = Config.AUTO_DELETE, **kw) -> Optional[types.Message]:
    reply = await _telegram_call(lambda: msg.answer(text, **kw))
    if reply is None:
        return None
    asyncio.create_task(_schedule_delete(msg.bot, msg.chat.id, msg.message_id, reply.message_id, delay))
    return reply

# ═══════════════════════════════════════════════════
# DATABASE (optimized with WAL mode, batch ops)
# ═══════════════════════════════════════════════════

class DB:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        self._lock = asyncio.Lock()
        self._init()

    def _init(self):
        cur = self._conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
                timezone TEXT DEFAULT 'UTC', notifications_enabled INTEGER DEFAULT 1,
                is_admin INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL,
                address TEXT NOT NULL COLLATE NOCASE, chain TEXT DEFAULT 'evm',
                label TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS chain_state (
                chain TEXT PRIMARY KEY, last_state TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL,
                chain TEXT NOT NULL, tx_hash TEXT NOT NULL, log_index INTEGER DEFAULT -1,
                memo TEXT, block_number INTEGER, asset TEXT, amount TEXT,
                direction TEXT DEFAULT 'incoming', notification_timezone TEXT DEFAULT 'UTC',
                contract_addr TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_addr ON addresses(chat_id, chain);
            CREATE INDEX IF NOT EXISTS idx_addr_addr ON addresses(address);
            CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(chat_id, created_at);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_notif_unique 
                ON notifications(chat_id, chain, tx_hash, log_index);
        """)
        # migrations
        for col, dtype, tbl in [
            ("notifications_enabled", "INTEGER DEFAULT 1", "users"),
            ("is_admin", "INTEGER DEFAULT 0", "users"),
            ("is_banned", "INTEGER DEFAULT 0", "users"),
            ("direction", "TEXT DEFAULT 'incoming'", "notifications"),
            ("notification_timezone", "TEXT DEFAULT 'UTC'", "notifications"),
            ("contract_addr", "TEXT", "notifications"),
        ]:
            try:
                cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {dtype}")
            except sqlite3.OperationalError:
                pass
        self._conn.commit()

    async def execute(self, sql: str, params: tuple = ()):
        async with self._lock:
            return await asyncio.to_thread(self._exec, sql, params)

    def _exec(self, sql: str, params: tuple):
        cur = self._conn.execute(sql, params)
        self._conn.commit()
        return cur

    async def fetchall(self, sql: str, params: tuple = ()) -> List[sqlite3.Row]:
        async with self._lock:
            def _f():
                return self._conn.execute(sql, params).fetchall()
            return await asyncio.to_thread(_f)

    async def fetchone(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        async with self._lock:
            def _f():
                return self._conn.execute(sql, params).fetchone()
            return await asyncio.to_thread(_f)

    # Batch operations for performance
    async def batch_insert_notifs(self, rows: List[tuple]):
        if not rows:
            return
        async with self._lock:
            def _ins():
                self._conn.executemany(
                    "INSERT OR IGNORE INTO notifications (chat_id,chain,tx_hash,log_index,memo,block_number,asset,amount,direction,notification_timezone,contract_addr) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    rows
                )
                self._conn.commit()
            await asyncio.to_thread(_ins)

    async def get_addresses_by_chain(self, chain: str) -> List[sqlite3.Row]:
        return await self.fetchall(
            "SELECT chat_id, address, label FROM addresses WHERE chain=? COLLATE NOCASE",
            (chain,)
        )

    async def get_all_evm_addresses(self) -> List[sqlite3.Row]:
        return await self.fetchall(
            "SELECT chat_id, address, label FROM addresses WHERE chain='evm'",
            ()
        )

    async def get_user_prefs(self, chat_id: int) -> dict:
        row = await self.fetchone(
            "SELECT timezone, notifications_enabled, is_admin FROM users WHERE chat_id=?",
            (chat_id,)
        )
        if row:
            return {"tz": row["timezone"], "enabled": row["notifications_enabled"], "admin": row["is_admin"]}
        return {"tz": "UTC", "enabled": 1, "admin": 0}

    async def dedup_batch(self, chat_id: int, chain: str, items: List[Tuple[str, int]]) -> Set[Tuple[str, int]]:
        """Return which (tx_hash, log_index) pairs already exist."""
        if not items:
            return set()
        placeholders = ",".join("(?,?)" for _ in items)
        params = []
        for tx, li in items:
            params.extend([tx, li])
        rows = await self.fetchall(
            f"SELECT tx_hash, log_index FROM notifications WHERE chat_id=? AND chain=? AND (tx_hash, log_index) IN ({placeholders})",
            (chat_id, chain, *params)
        )
        return {(r["tx_hash"], r["log_index"]) for r in rows}

# ═══════════════════════════════════════════════════
# TOKEN CACHES
# ═══════════════════════════════════════════════════

class EVMCache:
    __slots__ = ("_cache",)
    def __init__(self):
        self._cache: Dict[str, dict] = {}

    async def info(self, w3: AsyncWeb3, addr: str) -> dict:
        a = addr.lower()
        if a in self._cache:
            return self._cache[a]
        try:
            c = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=ERC20_ABI)
            name, sym, dec = await asyncio.gather(
                c.functions.name().call(),
                c.functions.symbol().call(),
                c.functions.decimals().call(),
            )
            info = {"name": name, "symbol": sym, "decimals": dec}
        except Exception as exc:
            logger.debug("Token meta fail %s: %s", addr, exc)
            info = {"name": "Unknown", "symbol": "???", "decimals": 18}
        self._cache[a] = info
        return info

class SPLCache:
    __slots__ = ("_cache", "_client")
    def __init__(self):
        self._cache: Dict[str, dict] = {}
        self._client: Optional[httpx.AsyncClient] = None

    async def info(self, mint: str) -> dict:
        if mint in self._cache:
            return self._cache[mint]
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10)
        try:
            r = await self._client.get(f"https://tokens.jup.ag/token/{mint}")
            d = r.json() if r.status_code == 200 else {}
            info = {
                "name": d.get("name", "Unknown"),
                "symbol": d.get("symbol", mint[:4]),
                "decimals": d.get("decimals", 9),
            }
        except Exception as exc:
            logger.debug("Jupiter fail %s: %s", mint, exc)
            info = {"name": "Unknown", "symbol": mint[:4], "decimals": 9}
        self._cache[mint] = info
        return info

    async def close(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None

# ═══════════════════════════════════════════════════
# TIMEZONE (async, cached)
# ═══════════════════════════════════════════════════

class TZHelper:
    __slots__ = ("_db", "_cache", "_ttl")
    def __init__(self, db: DB):
        self._db = db
        self._cache: Dict[int, Tuple[str, float]] = {}
        self._ttl = Config.USER_CACHE_TTL

    async def get(self, chat_id: int) -> str:
        now = time.monotonic()
        if chat_id in self._cache:
            tz, ts = self._cache[chat_id]
            if now - ts < self._ttl:
                return tz
        row = await self._db.fetchone("SELECT timezone FROM users WHERE chat_id=?", (chat_id,))
        tz = row["timezone"] if row and row["timezone"] else "UTC"
        self._cache[chat_id] = (tz, now)
        return tz

    async def now(self, chat_id: int) -> datetime:
        tz = await self.get(chat_id)
        try:
            return datetime.now(ZoneInfo(tz))
        except Exception:
            return datetime.now(timezone.utc)

    async def fmt(self, chat_id: int, dt: Optional[datetime] = None, stored_tz: Optional[str] = None) -> str:
        if dt is None:
            dt = await self.now(chat_id)
        elif stored_tz:
            try:
                dt = dt.replace(tzinfo=ZoneInfo(stored_tz))
            except Exception:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

    def invalidate(self, chat_id: int):
        self._cache.pop(chat_id, None)

# ═══════════════════════════════════════════════════
# NOTIFICATION BUILDER
# ═══════════════════════════════════════════════════

class Direction(Enum):
    IN = "incoming"
    OUT = "outgoing"

@dataclass
class Alert:
    chat_id: int
    chain_key: str
    title: str
    amount_str: str
    asset: str
    from_addr: str
    to_addr: str
    tx_hash: str
    direction: Direction
    memo: Optional[str] = None
    block: Optional[int] = None
    wallet_addr: Optional[str] = None
    log_index: int = -1
    contract_addr: Optional[str] = None

class Notifier:
    __slots__ = ("_db", "_tz", "_bot", "_user_cache")
    def __init__(self, db: DB, tz: TZHelper, bot: Bot):
        self._db = db
        self._tz = tz
        self._bot = bot
        self._user_cache: Dict[int, dict] = {}

    async def _pref(self, chat_id: int) -> dict:
        now = time.monotonic()
        if chat_id in self._user_cache:
            pref, ts = self._user_cache[chat_id]
            if now - ts < Config.USER_CACHE_TTL:
                return pref
        pref = await self._db.get_user_prefs(chat_id)
        self._user_cache[chat_id] = (pref, now)
        return pref

    async def send(self, alert: Alert) -> bool:
        pref = await self._pref(alert.chat_id)
        if not pref.get("enabled", 1):
            return True
        cfg = CHAINS_BY_KEY[alert.chain_key]
        label = "My Wallet"
        if alert.wallet_addr:
            row = await self._db.fetchone(
                "SELECT label FROM addresses WHERE chat_id=? AND chain=? AND address=? COLLATE NOCASE",
                (alert.chat_id, "evm" if cfg.type == "evm" else alert.chain_key, alert.wallet_addr.lower())
            )
            if row and row["label"]:
                label = row["label"]

        d = alert.direction
        badge = "🟢 IN" if d == Direction.IN else "🔴 OUT"
        de = "📥" if d == Direction.IN else "📤"
        oe = "📤" if d == Direction.IN else "📥"
        ol = "From" if d == Direction.IN else "To"
        sl = "To" if d == Direction.IN else "From"
        tstr = await self._tz.fmt(alert.chat_id)
        watched_addr = alert.wallet_addr or (alert.to_addr if d == Direction.IN else alert.from_addr)

        text = (
            f"<b>{badge} {alert.title}</b>\n\n"
            f"<b>{cfg.emoji} {cfg.name}</b>\n"
            f"<b>Wallet:</b> <code>{watched_addr}</code> <i>({label})</i>\n"
            f"<b>💰 Amount:</b> <code>{alert.amount_str} {alert.asset}</code>\n"
            f"<b>{oe} {ol}:</b> <code>{alert.from_addr}</code>\n"
            f"<b>{de} {sl}:</b> <code>{alert.to_addr}</code> <i>({label})</i>\n"
        )
        if alert.contract_addr:
            text += f"<b>Token contract:</b> <code>{alert.contract_addr}</code>\n"
        if alert.memo:
            text += f"<b>📝 Memo:</b> <code>{alert.memo}</code>\n"
        if alert.block is not None:
            text += f"<b>📦 Block:</b> <code>{alert.block}</code>\n"
        text += (
            f"<b>🔗 Tx:</b> <a href='{cfg.explorer}{alert.tx_hash}'>{alert.tx_hash}</a>\n"
            f"<b>⏰ Time:</b> <code>{tstr}</code>"
        )

        for attempt in range(1, 4):
            try:
                await self._bot.send_message(alert.chat_id, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                return True
            except Exception as exc:
                if attempt == 3:
                    logger.error("Notify fail %s: %s", alert.chat_id, exc)
                else:
                    await asyncio.sleep(attempt * 1.5)
        return False

    async def mark(self, alerts: List[Alert]):
        if not alerts:
            return
        rows = []
        for a in alerts:
            tz = await self._tz.get(a.chat_id)
            rows.append((a.chat_id, a.chain_key, a.tx_hash, a.log_index, a.memo, a.block, a.asset, a.amount_str, a.direction.value, tz, a.contract_addr))
        await self._db.batch_insert_notifs(rows)

# ═══════════════════════════════════════════════════
# BASE MONITOR
# ═══════════════════════════════════════════════════

async def _close_web3(w3: Optional[AsyncWeb3]):
    """Close a Web3 HTTP provider and its aiohttp session cache."""
    if w3 is None:
        return
    provider = getattr(w3, "provider", None)
    disconnect = getattr(provider, "disconnect", None)
    if callable(disconnect):
        try:
            result = disconnect()
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            logger.debug("Web3 provider close failed: %s", exc)

class Monitor(ABC):
    __slots__ = ("key", "cfg", "db", "notifier", "running")
    def __init__(self, key: str, cfg: ChainCfg, db: DB, notifier: Notifier):
        self.key = key
        self.cfg = cfg
        self.db = db
        self.notifier = notifier
        self.running = False

    @abstractmethod
    async def run(self): ...

    async def stop(self):
        self.running = False
        client = getattr(self, "_client", None)
        if client is not None:
            await client.aclose()
            self._client = None

        w3 = getattr(self, "_w3", None)
        if w3 is not None:
            await _close_web3(w3)
            self._w3 = None

# ═══════════════════════════════════════════════════
# EVM MONITOR (optimized: batch dedup, concurrent)
# ═══════════════════════════════════════════════════

class EVMMonitor(Monitor):
    __slots__ = ("_cache", "_w3", "_rpc_url", "_rpc_failures")
    def __init__(self, key: str, cfg: ChainCfg, db: DB, notifier: Notifier, cache: EVMCache):
        super().__init__(key, cfg, db, notifier)
        self._cache = cache
        self._w3: Optional[AsyncWeb3] = None
        self._rpc_url: Optional[str] = None
        self._rpc_failures = 0

    async def _connect(self) -> bool:
        endpoints = (self.cfg.rpc, *self.cfg.rpc_fallbacks)
        for ep in dict.fromkeys(endpoints):
            if not ep:
                continue
            provider = AsyncHTTPProvider(ep)
            candidate = AsyncWeb3(provider)
            candidate.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            try:
                if await candidate.is_connected():
                    self._w3 = candidate
                    self._rpc_url = ep
                    self._rpc_failures = 0
                    return True
            except Exception as exc:
                logger.debug("%s RPC fail %s: %s", self.cfg.name, ep, exc)
            finally:
                if self._w3 is not candidate:
                    await _close_web3(candidate)
        self._w3 = None
        return False

    async def run(self):
        self.running = True
        while self.running:
            if self._w3 is None and not await self._connect():
                self._rpc_failures += 1
                if self._rpc_failures == 1 or self._rpc_failures % 10 == 0:
                    delay = min(30 * (2 ** min(self._rpc_failures - 1, 3)), 300)
                    logger.error("❌ %s all RPC down, retry in %ss", self.cfg.name, delay)
                else:
                    delay = min(30 * (2 ** min(self._rpc_failures - 1, 3)), 300)
                await asyncio.sleep(delay)
                continue
            try:
                await self._tick()
            except Exception as exc:
                logger.warning("%s tick err: %s", self.key, exc)
                await _close_web3(self._w3)
                self._w3 = None
                await asyncio.sleep(5)

    async def _tick(self):
        current = await self._w3.eth.block_number
        row = await self.db.fetchone("SELECT last_state FROM chain_state WHERE chain=?", (self.key,))
        last = int(row["last_state"]) if row and row["last_state"] else current - 1
        if current <= last:
            await asyncio.sleep(self.cfg.poll)
            return
        fblock, tblock = last + 1, min(current, last + 10)
        addrs = await self.db.get_all_evm_addresses()
        if not addrs:
            await self.db.execute("INSERT OR REPLACE INTO chain_state (chain,last_state) VALUES (?,?)", (self.key, str(tblock)))
            await asyncio.sleep(self.cfg.poll)
            return

        addr_set = {r["address"].lower(): r for r in addrs}
        delivered = await asyncio.gather(
            self._scan_erc20(fblock, tblock, addr_set),
            self._scan_native(fblock, tblock, addr_set),
        )
        if not all(delivered):
            # Do not move the cursor when Telegram delivery failed. The same
            # block range will be retried on the next poll.
            await asyncio.sleep(5)
            return
        await self.db.execute("INSERT OR REPLACE INTO chain_state (chain,last_state) VALUES (?,?)", (self.key, str(tblock)))
        await asyncio.sleep(self.cfg.poll)

    async def _scan_erc20(self, f: int, t: int, addr_map: Dict[str, sqlite3.Row]) -> bool:
        try:
            logs = await self._w3.eth.get_logs({"fromBlock": f, "toBlock": t, "topics": [TRANSFER_TOPIC]})
        except Exception as exc:
            if t > f:
                m = (f + t) // 2
                results = await asyncio.gather(
                    self._scan_erc20(f, m, addr_map),
                    self._scan_erc20(m + 1, t, addr_map),
                )
                return all(results)
            raise

        # Group by tx_hash for batch dedup
        tx_items: Dict[str, List[dict]] = defaultdict(list)
        for log in logs:
            if len(log["topics"]) < 3:
                continue
            fr = "0x" + log["topics"][1].hex()[-40:]
            to = "0x" + log["topics"][2].hex()[-40:]
            fr_w = fr.lower() in addr_map
            to_w = to.lower() in addr_map
            if not fr_w and not to_w:
                continue
            data = log["data"].hex() if isinstance(log["data"], bytes) else log["data"]
            tx_hash = log["transactionHash"].hex()
            tx_items[tx_hash].append({
                "fr": fr, "to": to, "fr_w": fr_w, "to_w": to_w,
                "amount": int(data, 16), "token": log["address"],
                "li": log["logIndex"], "block": log["blockNumber"],
            })

        alerts: List[Alert] = []
        for tx_hash, items in tx_items.items():
            for item in items:
                directions = []
                if item["fr_w"]:
                    directions.append((Direction.OUT, item["fr"], item["li"]))
                if item["to_w"] and not item["fr_w"]:
                    directions.append((Direction.IN, item["to"], item["li"] + 100000))
                elif item["to_w"] and item["fr_w"]:
                    directions.append((Direction.IN, item["to"], item["li"] + 100000))

                for direction, watched, dedup_li in directions:
                    chat_id = addr_map[watched.lower()]["chat_id"]
                    info = await self._cache.info(self._w3, item["token"])
                    amt = Decimal(item["amount"]) / Decimal(10 ** info["decimals"])
                    amt_str = f"{amt:,.6f}".rstrip("0").rstrip(".")
                    asset = f"{info['name']} ({info['symbol']})"
                    title = "Outgoing ERC-20" if direction == Direction.OUT else "Incoming ERC-20"
                    alerts.append(Alert(
                        chat_id=chat_id, chain_key=self.key, title=title,
                        amount_str=amt_str, asset=asset, from_addr=item["fr"], to_addr=item["to"],
                        tx_hash=tx_hash, direction=direction, block=item["block"], wallet_addr=watched,
                        log_index=dedup_li,
                        contract_addr=item["token"],
                    ))

        return await self._send_batch(alerts)

    async def _scan_native(self, f: int, t: int, addr_map: Dict[str, sqlite3.Row]) -> bool:
        alerts: List[Alert] = []
        for n in range(f, t + 1):
            try:
                blk = await self._w3.eth.get_block(n, full_transactions=True)
            except Exception:
                raise
            for tx in blk.transactions:
                to = tx.get("to")
                fr = tx.get("from")
                if not to or not fr or tx.get("value", 0) == 0:
                    continue
                fr_w = fr.lower() in addr_map
                to_w = to.lower() in addr_map
                if not fr_w and not to_w:
                    continue
                txh = tx["hash"].hex()
                directions = []
                if fr_w:
                    directions.append((Direction.OUT, fr, -1))
                if to_w and not fr_w:
                    directions.append((Direction.IN, to, -2))
                elif to_w and fr_w:
                    directions.append((Direction.IN, to, -2))
                for direction, watched, dli in directions:
                    chat_id = addr_map[watched.lower()]["chat_id"]
                    amt = Decimal(tx["value"]) / Decimal(10 ** 18)
                    amt_str = f"{amt:,.6f}".rstrip("0").rstrip(".")
                    title = "Outgoing Native" if direction == Direction.OUT else "Incoming Native"
                    alerts.append(Alert(
                        chat_id=chat_id, chain_key=self.key, title=title,
                        amount_str=amt_str, asset=self.cfg.native, from_addr=fr, to_addr=to,
                        tx_hash=txh, direction=direction, block=n, wallet_addr=watched,
                        log_index=dli,
                    ))
        return await self._send_batch(alerts)

    async def _send_batch(self, alerts: List[Alert]) -> bool:
        if not alerts:
            return True
        # Dedup check
        by_chat: Dict[int, List[Alert]] = defaultdict(list)
        for a in alerts:
            by_chat[a.chat_id].append(a)
        final: List[Alert] = []
        for chat_id, als in by_chat.items():
            items = [(a.tx_hash, a.log_index) for a in als]
            existing = await self.db.dedup_batch(chat_id, self.key, items)
            for a in als:
                if (a.tx_hash, a.log_index) not in existing:
                    final.append(a)
        if not final:
            return True
        delivered = await asyncio.gather(*(self.notifier.send(a) for a in final))
        await self.notifier.mark([a for a, ok in zip(final, delivered) if ok])
        return all(delivered)

# ═══════════════════════════════════════════════════
# TON MONITOR
# ═══════════════════════════════════════════════════

class TONMonitor(Monitor):
    __slots__ = ("_client",)
    def __init__(self, key: str, cfg: ChainCfg, db: DB, notifier: Notifier):
        super().__init__(key, cfg, db, notifier)
        self._client: Optional[httpx.AsyncClient] = None

    async def run(self):
        self._client = httpx.AsyncClient(timeout=30)
        self.running = True
        headers = {"Authorization": f"Bearer {Config.TON_API_KEY}"} if Config.TON_API_KEY else {}
        while self.running:
            try:
                await self._tick(headers)
            except Exception as exc:
                logger.exception("TON: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self, headers: dict):
        rows = await self.db.get_addresses_by_chain("ton")
        if not rows:
            await asyncio.sleep(self.cfg.poll)
            return
        alerts: List[Alert] = []
        for r in rows:
            addr = r["address"]
            try:
                resp = await self._client.get(f"{self.cfg.api}/accounts/{addr}/events", params={"limit": 20}, headers=headers)
                data = resp.json()
            except Exception as exc:
                logger.debug("TON API %s: %s", addr, exc)
                continue
            for event_index, event in enumerate(data.get("events", [])):
                eid = event.get("event_id")
                if not eid:
                    continue
                for action_index, action in enumerate(event.get("actions", [])):
                    atype = action.get("type")
                    if atype == "TonTransfer":
                        t = action.get("TonTransfer", {})
                        rcpt = t.get("recipient", {}).get("address", {}).get("address")
                        sender = t.get("sender", {}).get("address", {}).get("address", "?")
                        amt = int(t.get("amount", 0))
                        if amt == 0:
                            continue
                        direction = Direction.IN if rcpt == addr else (Direction.OUT if sender == addr else None)
                        if direction is None:
                            continue
                        a = Decimal(amt) / Decimal(10 ** self.cfg.decimals)
                        alerts.append(Alert(
                            chat_id=r["chat_id"], chain_key=self.key,
                            title="Outgoing TON" if direction == Direction.OUT else "Incoming TON",
                            amount_str=f"{a:,.6f}".rstrip("0").rstrip("."),
                            asset=self.cfg.native,
                            from_addr=addr if direction == Direction.OUT else sender,
                            to_addr=rcpt if direction == Direction.OUT else addr,
                            tx_hash=eid, direction=direction, memo=t.get("comment"),
                            log_index=event_index * 1000 + action_index,
                        ))
                    elif atype == "JettonTransfer":
                        t = action.get("JettonTransfer", {})
                        rcpt = t.get("recipient", {}).get("address", {}).get("address")
                        sender = t.get("sender", {}).get("address", {}).get("address", "?")
                        amt_s = t.get("amount", "0")
                        if amt_s == "0" or not amt_s:
                            continue
                        direction = Direction.IN if rcpt == addr else (Direction.OUT if sender == addr else None)
                        if direction is None:
                            continue
                        jet = t.get("jetton", {})
                        dec = jet.get("decimals", 9)
                        a = Decimal(amt_s) / Decimal(10 ** dec)
                        alerts.append(Alert(
                            chat_id=r["chat_id"], chain_key=self.key,
                            title="Outgoing Jetton" if direction == Direction.OUT else "Incoming Jetton",
                            amount_str=f"{a:,.6f}".rstrip("0").rstrip("."),
                            asset=f"{jet.get('name', 'Unknown')} ({jet.get('symbol', '???')})",
                            from_addr=addr if direction == Direction.OUT else sender,
                            to_addr=rcpt if direction == Direction.OUT else addr,
                            tx_hash=eid,
                            direction=direction, memo=t.get("comment"),
                            log_index=event_index * 1000 + action_index,
                            contract_addr=jet.get("address") or jet.get("master"),
                        ))
        await self._send_batch(alerts)
        await asyncio.sleep(self.cfg.poll)

    async def _send_batch(self, alerts: List[Alert]):
        if not alerts:
            return
        by_chat = defaultdict(list)
        for a in alerts:
            by_chat[a.chat_id].append(a)
        final = []
        for cid, als in by_chat.items():
            items = [(a.tx_hash, a.log_index) for a in als]
            existing = await self.db.dedup_batch(cid, self.key, items)
            for a in als:
                if (a.tx_hash, a.log_index) not in existing:
                    final.append(a)
        if final:
            delivered = await asyncio.gather(*(self.notifier.send(a) for a in final))
            await self.notifier.mark([a for a, ok in zip(final, delivered) if ok])

# ═══════════════════════════════════════════════════
# XLM MONITOR
# ═══════════════════════════════════════════════════

class XLMMonitor(Monitor):
    __slots__ = ("_client",)
    def __init__(self, key: str, cfg: ChainCfg, db: DB, notifier: Notifier):
        super().__init__(key, cfg, db, notifier)
        self._client: Optional[httpx.AsyncClient] = None

    async def run(self):
        self._client = httpx.AsyncClient(timeout=30)
        self.running = True
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("XLM: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self):
        rows = await self.db.get_addresses_by_chain("xlm")
        if not rows:
            await asyncio.sleep(self.cfg.poll)
            return
        alerts: List[Alert] = []
        for r in rows:
            addr = r["address"]
            cursor = None
            st = await self.db.fetchone("SELECT last_state FROM chain_state WHERE chain=?", (f"xlm_{addr}",))
            if st and st["last_state"]:
                cursor = st["last_state"]
            params = {"order": "desc", "limit": 10}
            if cursor:
                params["cursor"] = cursor
            try:
                resp = await self._client.get(f"{self.cfg.api}/accounts/{addr}/payments", params=params)
                data = resp.json()
            except Exception as exc:
                logger.debug("XLM API: %s", exc)
                continue
            records = data.get("_embedded", {}).get("records", [])
            for record_index, rec in enumerate(records):
                txh = rec.get("transaction_hash")
                if not txh:
                    continue
                direction = Direction.IN if rec.get("to") == addr else (Direction.OUT if rec.get("from") == addr else None)
                if direction is None or rec.get("type") not in ("payment", "path_payment"):
                    continue
                memo = None
                try:
                    td = (await self._client.get(f"{self.cfg.api}/transactions/{txh}")).json()
                    if td.get("memo_type") and td.get("memo"):
                        memo = f"{td['memo_type'].upper()}: {td['memo']}"
                except Exception:
                    pass
                atype = rec.get("asset_type")
                if atype == "native":
                    asset = "XLM"
                else:
                    code = rec.get("asset_code", "ASSET")
                    issuer = rec.get("asset_issuer", "")
                    asset = f"{code}" + (f" ({issuer[:4]}...{issuer[-4:]})" if issuer else "")
                amt = Decimal(rec.get("amount", "0"))
                title = "Outgoing XLM" if direction == Direction.OUT else "Incoming XLM"
                if asset != "XLM":
                    title = "Outgoing Asset" if direction == Direction.OUT else "Incoming Asset"
                alerts.append(Alert(
                    chat_id=r["chat_id"], chain_key=self.key, title=title,
                    amount_str=f"{amt:,.7f}".rstrip("0").rstrip("."),
                    asset=asset, from_addr=rec.get("from", "?"), to_addr=rec.get("to", "?"),
                    tx_hash=txh, direction=direction, memo=memo, log_index=record_index,
                    contract_addr=rec.get("asset_issuer") if atype != "native" else None,
                ))
            if records:
                await self.db.execute("INSERT OR REPLACE INTO chain_state (chain,last_state) VALUES (?,?)", (f"xlm_{addr}", records[0].get("paging_token")))
        await self._send_batch(alerts)
        await asyncio.sleep(self.cfg.poll)

    async def _send_batch(self, alerts: List[Alert]):
        if not alerts:
            return
        by_chat = defaultdict(list)
        for a in alerts:
            by_chat[a.chat_id].append(a)
        final = []
        for cid, als in by_chat.items():
            items = [(a.tx_hash, a.log_index) for a in als]
            existing = await self.db.dedup_batch(cid, self.key, items)
            for a in als:
                if (a.tx_hash, a.log_index) not in existing:
                    final.append(a)
        if final:
            delivered = await asyncio.gather(*(self.notifier.send(a) for a in final))
            await self.notifier.mark([a for a, ok in zip(final, delivered) if ok])

# ═══════════════════════════════════════════════════
# SOL MONITOR
# ═══════════════════════════════════════════════════

class SOLMonitor(Monitor):
    __slots__ = ("_client", "_spl")
    def __init__(self, key: str, cfg: ChainCfg, db: DB, notifier: Notifier, spl: SPLCache):
        super().__init__(key, cfg, db, notifier)
        self._client: Optional[httpx.AsyncClient] = None
        self._spl = spl

    async def run(self):
        self._client = httpx.AsyncClient(timeout=30)
        self.running = True
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("SOL: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self):
        rows = await self.db.get_addresses_by_chain("sol")
        if not rows:
            await asyncio.sleep(self.cfg.poll)
            return
        alerts: List[Alert] = []
        for r in rows:
            addr = r["address"]
            last_signature = None
            st = await self.db.fetchone("SELECT last_state FROM chain_state WHERE chain=?", (f"sol_{addr}",))
            if st and st["last_state"]:
                last_signature = st["last_state"]
            sig_params = {"limit": 10}
            if last_signature:
                # Fetch signatures newer than the last processed signature.
                sig_params["until"] = last_signature
            try:
                resp = await self._client.post(self.cfg.rpc, json={
                    "jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                    "params": [addr, sig_params]
                })
                sigs = resp.json().get("result", [])
            except Exception as exc:
                logger.debug("SOL RPC: %s", exc)
                continue
            if not sigs:
                continue
            for si in sigs:
                sig = si["signature"]
                try:
                    tx = (await self._client.post(self.cfg.rpc, json={
                        "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                        "params": [sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}]
                    })).json().get("result")
                except Exception:
                    continue
                if not tx or tx.get("meta", {}).get("err"):
                    continue
                meta = tx.get("meta", {})
                msg = tx.get("transaction", {}).get("message", {})
                keys = msg.get("accountKeys", [])
                if addr not in keys:
                    continue
                idx = keys.index(addr)
                memo = self._extract_memo(msg, keys)
                pre = meta.get("preBalances", [])[idx] if idx < len(meta.get("preBalances", [])) else 0
                post = meta.get("postBalances", [])[idx] if idx < len(meta.get("postBalances", [])) else 0
                diff = post - pre
                if diff != 0:
                    direction = Direction.IN if diff > 0 else Direction.OUT
                    a = Decimal(abs(diff)) / Decimal(10 ** self.cfg.decimals)
                    alerts.append(Alert(
                        chat_id=r["chat_id"], chain_key=self.key,
                        title="Outgoing SOL" if direction == Direction.OUT else "Incoming SOL",
                        amount_str=f"{a:,.9f}".rstrip("0").rstrip("."),
                        asset=self.cfg.native,
                        from_addr=addr if direction == Direction.OUT else "Network",
                        to_addr="Network" if direction == Direction.OUT else addr,
                        tx_hash=sig, direction=direction, memo=memo, log_index=0,
                    ))
                pre_t = {tb["accountIndex"]: tb for tb in meta.get("preTokenBalances", []) if tb.get("owner") == addr}
                post_t = {tb["accountIndex"]: tb for tb in meta.get("postTokenBalances", []) if tb.get("owner") == addr}
                for tidx in set(pre_t) | set(post_t):
                    pre_amt = Decimal(pre_t.get(tidx, {}).get("uiTokenAmount", {}).get("amount", "0"))
                    post_amt = Decimal(post_t.get(tidx, {}).get("uiTokenAmount", {}).get("amount", "0"))
                    td = post_amt - pre_amt
                    if td == 0:
                        continue
                    direction = Direction.IN if td > 0 else Direction.OUT
                    token = post_t.get(tidx) or pre_t[tidx]
                    mint = token["mint"]
                    dec = token["uiTokenAmount"]["decimals"]
                    info = await self._spl.info(mint)
                    a = abs(td) / Decimal(10 ** dec)
                    alerts.append(Alert(
                        chat_id=r["chat_id"], chain_key=self.key,
                        title="Outgoing SPL" if direction == Direction.OUT else "Incoming SPL",
                        amount_str=f"{a:,.6f}".rstrip("0").rstrip("."),
                        asset=f"{info['name']} ({info['symbol']})",
                        from_addr=addr if direction == Direction.OUT else "Network",
                        to_addr="Network" if direction == Direction.OUT else addr,
                        tx_hash=sig, direction=direction, memo=memo, log_index=100000 + tidx,
                        contract_addr=mint,
                    ))
            # Advance only after all fetched transactions were inspected. If an
            # API call fails, the same signatures are retried on the next poll.
            await self.db.execute("INSERT OR REPLACE INTO chain_state (chain,last_state) VALUES (?,?)", (f"sol_{addr}", sigs[0].get("signature")))
        await self._send_batch(alerts)
        await asyncio.sleep(self.cfg.poll)

    def _extract_memo(self, msg: dict, keys: List[str]) -> Optional[str]:
        for inst in msg.get("instructions", []):
            pidx = inst.get("programIdIndex")
            if pidx is not None and pidx < len(keys):
                prog = keys[pidx]
                if prog in ("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr", "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"):
                    data = inst.get("data")
                    if data:
                        try:
                            return base64.b64decode(data).decode("utf-8", errors="ignore")
                        except Exception:
                            return str(data)
        return None

    async def _send_batch(self, alerts: List[Alert]):
        if not alerts:
            return
        by_chat = defaultdict(list)
        for a in alerts:
            by_chat[a.chat_id].append(a)
        final = []
        for cid, als in by_chat.items():
            items = [(a.tx_hash, a.log_index) for a in als]
            existing = await self.db.dedup_batch(cid, self.key, items)
            for a in als:
                if (a.tx_hash, a.log_index) not in existing:
                    final.append(a)
        if final:
            delivered = await asyncio.gather(*(self.notifier.send(a) for a in final))
            await self.notifier.mark([a for a, ok in zip(final, delivered) if ok])

# ═══════════════════════════════════════════════════
# TRON MONITOR
# ═══════════════════════════════════════════════════

class TRONMonitor(Monitor):
    __slots__ = ("_client",)
    def __init__(self, key: str, cfg: ChainCfg, db: DB, notifier: Notifier):
        super().__init__(key, cfg, db, notifier)
        self._client: Optional[httpx.AsyncClient] = None

    async def run(self):
        self._client = httpx.AsyncClient(timeout=30, headers={"Accept": "application/json"})
        self.running = True
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("TRON: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self):
        rows = await self.db.get_addresses_by_chain("tron")
        if not rows:
            await asyncio.sleep(self.cfg.poll)
            return
        alerts: List[Alert] = []
        for r in rows:
            addr = r["address"]
            # Native TRX
            try:
                data = (await self._client.get(f"{self.cfg.api}/accounts/{addr}/transactions", params={"limit": 10, "order_by": "block_timestamp,desc"})).json()
            except Exception as exc:
                logger.debug("TRON native: %s", exc)
                data = {"data": []}
            for tx_index, tx in enumerate(data.get("data", [])):
                raw = tx.get("raw_data", {})
                c = raw.get("contract", [{}])[0]
                p = c.get("parameter", {}).get("value", {})
                to_addr = p.get("to_address")
                owner = p.get("owner_address")
                amount = p.get("amount", 0)
                txh = tx.get("txID")
                if not txh or amount == 0:
                    continue
                direction = Direction.IN if to_addr == addr else (Direction.OUT if owner == addr else None)
                if direction is None:
                    continue
                a = Decimal(amount) / Decimal(10 ** self.cfg.decimals)
                alerts.append(Alert(
                    chat_id=r["chat_id"], chain_key=self.key,
                    title="Outgoing TRX" if direction == Direction.OUT else "Incoming TRX",
                    amount_str=f"{a:,.6f}".rstrip("0").rstrip("."),
                    asset=self.cfg.native, from_addr=owner or "?", to_addr=to_addr or addr,
                    tx_hash=txh, direction=direction, log_index=tx_index,
                ))
            # TRC20
            try:
                data = (await self._client.get(f"{self.cfg.api}/accounts/{addr}/transactions/trc20", params={"limit": 10, "order_by": "block_timestamp,desc"})).json()
            except Exception as exc:
                logger.debug("TRON TRC20: %s", exc)
                data = {"data": []}
            for tx_index, tx in enumerate(data.get("data", [])):
                txh = tx.get("transaction_id")
                if not txh:
                    continue
                to_addr = tx.get("to")
                fr_addr = tx.get("from")
                direction = Direction.IN if to_addr == addr else (Direction.OUT if fr_addr == addr else None)
                if direction is None:
                    continue
                token = tx.get("token_info", {})
                dec = token.get("decimals", 6)
                a = Decimal(tx.get("value", "0")) / Decimal(10 ** dec)
                alerts.append(Alert(
                    chat_id=r["chat_id"], chain_key=self.key,
                    title="Outgoing TRC-20" if direction == Direction.OUT else "Incoming TRC-20",
                    amount_str=f"{a:,.6f}".rstrip("0").rstrip("."),
                    asset=f"{token.get('name', 'Unknown')} ({token.get('symbol', '???')})",
                    from_addr=fr_addr or "?", to_addr=to_addr or addr,
                    tx_hash=txh, direction=direction, log_index=100000 + tx_index,
                    contract_addr=token.get("address"),
                ))
        await self._send_batch(alerts)
        await asyncio.sleep(self.cfg.poll)

    async def _send_batch(self, alerts: List[Alert]):
        if not alerts:
            return
        by_chat = defaultdict(list)
        for a in alerts:
            by_chat[a.chat_id].append(a)
        final = []
        for cid, als in by_chat.items():
            items = [(a.tx_hash, a.log_index) for a in als]
            existing = await self.db.dedup_batch(cid, self.key, items)
            for a in als:
                if (a.tx_hash, a.log_index) not in existing:
                    final.append(a)
        if final:
            delivered = await asyncio.gather(*(self.notifier.send(a) for a in final))
            await self.notifier.mark([a for a, ok in zip(final, delivered) if ok])

# ═══════════════════════════════════════════════════
# BTC MONITOR
# ═══════════════════════════════════════════════════

class BTCMonitor(Monitor):
    __slots__ = ("_client",)
    def __init__(self, key: str, cfg: ChainCfg, db: DB, notifier: Notifier):
        super().__init__(key, cfg, db, notifier)
        self._client: Optional[httpx.AsyncClient] = None

    async def run(self):
        self._client = httpx.AsyncClient(timeout=30)
        self.running = True
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("BTC: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self):
        rows = await self.db.get_addresses_by_chain("btc")
        if not rows:
            await asyncio.sleep(self.cfg.poll)
            return
        alerts: List[Alert] = []
        for r in rows:
            addr = r["address"]
            try:
                txs = (await self._client.get(f"{self.cfg.api}/address/{addr}/txs")).json()
            except Exception as exc:
                logger.debug("BTC API: %s", exc)
                continue
            for tx in txs:
                txh = tx.get("txid")
                if not txh:
                    continue
                received = sum(v.get("value", 0) for v in tx.get("vout", []) if addr in [v.get("scriptpubkey_address"), v.get("address")])
                sent = sum(vin.get("prevout", {}).get("value", 0) for vin in tx.get("vin", []) if addr in [vin.get("prevout", {}).get("scriptpubkey_address"), vin.get("prevout", {}).get("address")])
                directions = []
                if received > 0:
                    directions.append((Direction.IN, received))
                if sent > 0:
                    directions.append((Direction.OUT, sent))
                if not directions:
                    continue
                sender = "Unknown"
                vins = tx.get("vin", [])
                if vins:
                    sender = vins[0].get("prevout", {}).get("scriptpubkey_address", "Unknown")
                for direction, amount in directions:
                    a = Decimal(amount) / Decimal(10 ** self.cfg.decimals)
                    alerts.append(Alert(
                        chat_id=r["chat_id"], chain_key=self.key,
                        title="Outgoing BTC" if direction == Direction.OUT else "Incoming BTC",
                        amount_str=f"{a:,.8f}".rstrip("0").rstrip("."),
                        asset=self.cfg.native, from_addr=sender, to_addr=addr,
                        tx_hash=txh, direction=direction, log_index=0 if direction == Direction.IN else 1,
                    ))
        await self._send_batch(alerts)
        await asyncio.sleep(self.cfg.poll)

    async def _send_batch(self, alerts: List[Alert]):
        if not alerts:
            return
        by_chat = defaultdict(list)
        for a in alerts:
            by_chat[a.chat_id].append(a)
        final = []
        for cid, als in by_chat.items():
            items = [(a.tx_hash, a.log_index) for a in als]
            existing = await self.db.dedup_batch(cid, self.key, items)
            for a in als:
                if (a.tx_hash, a.log_index) not in existing:
                    final.append(a)
        if final:
            delivered = await asyncio.gather(*(self.notifier.send(a) for a in final))
            await self.notifier.mark([a for a, ok in zip(final, delivered) if ok])

# ═══════════════════════════════════════════════════
# SUI MONITOR
# ═══════════════════════════════════════════════════

class SUIMonitor(Monitor):
    __slots__ = ("_client",)
    def __init__(self, key: str, cfg: ChainCfg, db: DB, notifier: Notifier):
        super().__init__(key, cfg, db, notifier)
        self._client: Optional[httpx.AsyncClient] = None

    async def run(self):
        self._client = httpx.AsyncClient(timeout=30)
        self.running = True
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("SUI: %s", exc)
                await asyncio.sleep(5)

    async def _rpc(self, method: str, params: list) -> dict:
        r = await self._client.post(self.cfg.rpc, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        r.raise_for_status()
        p = r.json()
        if "error" in p:
            raise RuntimeError(p["error"])
        return p.get("result", {})

    async def _tick(self):
        rows = await self.db.get_addresses_by_chain("sui")
        alerts: List[Alert] = []
        for r in rows:
            addr = r["address"]
            try:
                result = await self._rpc("suix_queryTransactionBlocks", [{"ToAddress": addr}, {
                    "showInput": True, "showEffects": True, "showBalanceChanges": True,
                }, None, 50, True])
            except Exception as exc:
                logger.debug("SUI RPC: %s", exc)
                continue
            for tx in result.get("data", []):
                digest = tx.get("digest")
                if not digest:
                    continue
                for i, change in enumerate(tx.get("balanceChanges") or []):
                    owner = change.get("owner")
                    oa = owner.get("AddressOwner") if isinstance(owner, dict) else owner
                    if not oa or oa.lower() != addr.lower():
                        continue
                    raw = int(change.get("amount", "0"))
                    if raw == 0:
                        continue
                    direction = Direction.IN if raw > 0 else Direction.OUT
                    ctype = change.get("coinType", "0x2::sui::SUI")
                    is_sui = ctype.endswith("::sui::SUI")
                    dec = self.cfg.decimals if is_sui else 9
                    sym = self.cfg.native if is_sui else ctype.rsplit("::", 1)[-1].upper()
                    a = Decimal(abs(raw)) / Decimal(10 ** dec)
                    sender = (tx.get("transaction", {}).get("data", {}) or {}).get("sender", "Unknown")
                    alerts.append(Alert(
                        chat_id=r["chat_id"], chain_key=self.key,
                        title="Outgoing Sui" if direction == Direction.OUT else "Incoming Sui",
                        amount_str=f"{a:,.9f}".rstrip("0").rstrip("."),
                        asset=sym, from_addr=sender, to_addr=addr,
                        tx_hash=digest, direction=direction, log_index=i,
                        contract_addr=None if is_sui else ctype,
                    ))
        await self._send_batch(alerts)
        await asyncio.sleep(self.cfg.poll)

    async def _send_batch(self, alerts: List[Alert]):
        if not alerts:
            return
        by_chat = defaultdict(list)
        for a in alerts:
            by_chat[a.chat_id].append(a)
        final = []
        for cid, als in by_chat.items():
            items = [(a.tx_hash, a.log_index) for a in als]
            existing = await self.db.dedup_batch(cid, self.key, items)
            for a in als:
                if (a.tx_hash, a.log_index) not in existing:
                    final.append(a)
        if final:
            delivered = await asyncio.gather(*(self.notifier.send(a) for a in final))
            await self.notifier.mark([a for a, ok in zip(final, delivered) if ok])

# ═══════════════════════════════════════════════════
# TELEGRAM HANDLERS
# ═══════════════════════════════════════════════════

class RateLimiter:
    __slots__ = ("max_upd", "window", "_events", "_last_notice")
    def __init__(self, max_upd: int = 30, window: int = 60):
        self.max_upd = max_upd
        self.window = window
        self._events: Dict[int, list] = defaultdict(list)
        self._last_notice: Dict[int, float] = {}

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        chat = getattr(event, "chat", None)
        uid = getattr(user, "id", None)
        cid = getattr(chat, "id", None)
        if uid is None:
            return await handler(event, data)

        admin = bool(Config.ADMIN_ID and str(cid) == str(Config.ADMIN_ID))
        if not admin and "db" in globals():
            row = await db.fetchone("SELECT is_admin, is_banned FROM users WHERE chat_id=?", (cid,))
            admin = bool(row and row["is_admin"])
            if row and row["is_banned"]:
                await event.answer("Access denied. Your account is banned.")
                return
        if admin:
            return await handler(event, data)
        now = time.monotonic()
        bucket = self._events[uid]
        bucket[:] = [t for t in bucket if now - t <= self.window]
        if len(bucket) >= self.max_upd:
            if now - self._last_notice.get(uid, 0) > 30:
                self._last_notice[uid] = now
                await event.answer("⏳ Too many requests. Please wait.")
            return
        bucket.append(now)
        return await handler(event, data)


router = Router()
rate_limiter = RateLimiter()
router.message.middleware(rate_limiter)
router.callback_query.middleware(rate_limiter)

db: DB
tz_helper: TZHelper
notifier: Notifier
evm_cache: EVMCache
spl_cache: SPLCache
monitors: Dict[str, Monitor] = {}
app_start: Optional[datetime] = None
ask_sessions: Dict[int, float] = {}

# ── Helpers ────────────────────────────────────────

_RE_EVM = re.compile(r"^0x[0-9a-fA-F]{40}$")
_RE_TON = re.compile(r"^(EQ|UQ|0:)[A-Za-z0-9_-]{30,}$")
_RE_XLM = re.compile(r"^G[A-Z0-9]{55}$")
_RE_TRON = re.compile(r"^T[A-Za-z0-9]{33}$")
_RE_BTC = re.compile(r"^(bc1|[13])[a-zA-HJ-NP-Z0-9]{24,62}$")
_RE_SUI = re.compile(r"^0x[0-9a-fA-F]{64}$")

CHAIN_ALIASES = {"rh": "robinhood", "robinhoodchain": "robinhood", "hyper": "hyperevm",
                 "hyper-evm": "hyperevm", "merlinchain": "merlin", "bitcoin": "btc",
                 "solana": "sol", "stellar": "xlm"}

def detect_chain(addr: str) -> Optional[str]:
    a = addr.strip()
    if _RE_EVM.match(a):
        return "evm"
    if _RE_TON.match(a):
        return "ton"
    if _RE_XLM.match(a):
        return "xlm"
    if _RE_TRON.match(a):
        return "tron"
    if _RE_BTC.match(a):
        return "btc"
    if _RE_SUI.match(a):
        return "sui"
    if 32 <= len(a) <= 44 and not a.startswith(("0x", "T")):
        return "sol"
    return None

def auto_label(addr: str, chain: str) -> str:
    cfg = CHAINS_BY_KEY.get(chain)
    name = cfg.name if cfg else chain.upper()
    return f"{name} {addr[:6]}...{addr[-4:]}"

def mask_address(addr: str, start: int = 8, end: int = 7) -> str:
    """Show only the beginning and end of an address in admin views."""
    if len(addr) <= start + end + 3:
        return addr
    return f"{addr[:start]}...{addr[-end:]}"

TELEGRAM_MAX = 4096

def chunk_text(text: str, limit: int = TELEGRAM_MAX) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks = []
    current = ""
    for block in text.split("\n\n"):
        piece = block + "\n\n"
        if len(current) + len(piece) > limit:
            if current:
                chunks.append(current.rstrip("\n"))
                current = ""
            while len(piece) > limit:
                chunks.append(piece[:limit])
                piece = piece[limit:]
        current += piece
    if current.strip():
        chunks.append(current.rstrip("\n"))
    return chunks

async def send_long(msg: types.Message, text: str, **kw):
    for c in chunk_text(text):
        await msg.answer(c, **kw)

async def is_admin(cid: int) -> bool:
    if Config.ADMIN_ID and str(cid) == str(Config.ADMIN_ID):
        return True
    pref = await db.get_user_prefs(cid)
    return bool(pref.get("admin", 0))

def local_project_answer(question: str) -> str:
    """Answer project questions locally from the bot's configured features."""
    q = re.sub(r"[^a-z0-9?\s-]+", " ", question.lower()).strip()
    # Normalize a few common user typos so the offline assistant remains forgiving.
    for wrong, right in {
        "quesiton": "question", "qusetion": "question", "walet": "wallet",
        "transection": "transaction", "notifcation": "notification",
        "recieve": "receive", "recieving": "receiving", "recieveing": "receiving", "suppoted": "supported",
    }.items():
        q = re.sub(rf"\b{wrong}\b", right, q)
    chain_count = len(_CHAINS)
    evm_count = len(EVM_KEYS)
    other_names = ", ".join(c.name for c in _CHAINS if c.type != "evm")

    if q in {"hi", "hello", "hey", "good morning", "good evening"}:
        return "<b>Project Assistant</b>\n\nHello! Ask me about wallets, chains, transfers, alerts, commands, or troubleshooting."
    if any(word in q for word in ("what is this", "what does this", "how does this", "about this bot", "about the bot")):
        return (
            "<b>About this monitor</b>\n\n"
            "This Telegram bot watches public wallet addresses across multiple blockchains and sends alerts for "
            "incoming and outgoing native-coin and token transfers. It is read-only and does not need private keys.\n\n"
            "Start with <code>/add &lt;address&gt; [label]</code>, then check <code>/status</code>."
        )
    if (any(word in q for word in ("supported", "support", "chain", "network"))
            and not any(word in q for word in ("token", "asset", "coin", "jetton", "spl", "erc20", "erc-20", "trc20", "trc-20"))):
        return (
            f"<b>Supported networks</b>\n\n"
            f"The monitor supports <b>{chain_count} networks</b>: {evm_count} EVM networks plus {other_names}.\n\n"
            "EVM wallets are added once and monitored across every configured EVM network."
        )
    if (re.search(r"\b(add|monitor|watch)\b", q)
            and ("wallet" in q or "address" in q)):
        return (
            "<b>Add a wallet</b>\n\n"
            "Use automatic detection:\n"
            "<code>/add 0xYourAddress My Wallet</code>\n\n"
            "Or specify a chain:\n"
            "<code>/add ethereum 0xYourAddress My Wallet</code>\n\n"
            "The final text is saved as your custom wallet label."
        )
    if any(word in q for word in ("label", "rename", "name wallet")):
        return (
            "<b>Custom wallet labels</b>\n\n"
            "Add a label while adding a wallet:\n"
            "<code>/add 0xYourAddress Treasury</code>\n\n"
            "Change it later with:\n"
            "<code>/label ethereum 0xYourAddress Treasury</code>"
        )
    if any(word in q for word in ("incoming", "outgoing", "in/out", "transfer")):
        return (
            "<b>Transfer monitoring</b>\n\n"
            "The bot detects incoming and outgoing native-asset and token transfers. "
            "Alerts include the amount, sender, receiver, transaction link, block, time, and token contract when available."
        )
    if (any(word in q for word in ("native", "coin", "erc-20", "erc20", "trc-20", "trc20", "jetton", "spl", "move coin", "token"))
            and "contract" not in q and "address" not in q):
        return (
            "<b>Assets monitored</b>\n\n"
            "The bot monitors native coins and supported token standards, including ERC-20, TRC-20, TON Jettons, "
            "Stellar assets, Solana SPL tokens, and Sui Move coins. Token alerts show the asset name and contract or mint when available."
        )
    if "contract" in q or "token address" in q or " ca " in f" {q} ":
        return (
            "<b>Token contract addresses</b>\n\n"
            "Token alerts include the contract or token identifier. Native coins do not show a contract because they are network assets."
        )
    if any(word in q for word in ("admin", "ban", "unban", "spam")):
        return (
            "<b>Admin controls</b>\n\n"
            "Admins can use <code>/admin</code>, <code>/admin_user &lt;chat_id&gt;</code>, "
            "<code>/ban &lt;chat_id&gt;</code>, <code>/unban &lt;chat_id&gt;</code>, and "
            "<code>/broadcast message</code>. Anti-spam protection does not apply to admins."
        )
    if any(word in q for word in ("timezone", "time", "ist", "utc")):
        return (
            "<b>Timezone</b>\n\n"
            "Set your display timezone with:\n"
            "<code>/timezone Asia/Kolkata</code>\n\n"
            "Alert timestamps are formatted using your saved timezone."
        )
    if any(word in q for word in ("history", "past alert", "previous alert")):
        return "<b>Alert history</b>\n\nUse <code>/history [limit]</code> to view up to 50 saved alerts, or <code>/clearhistory</code> to remove them."
    if any(word in q for word in ("list wallet", "show wallet", "my wallet", "my address")):
        return "<b>View wallets</b>\n\nUse <code>/list</code> to view your monitored wallets, or <code>/list ethereum</code> for one chain."
    if any(word in q for word in ("remove wallet", "delete wallet", "stop monitor", "unmonitor")):
        return "<b>Remove a wallet</b>\n\nUse <code>/remove &lt;chain&gt; &lt;address&gt;</code> to stop monitoring a wallet."
    if any(word in q for word in ("pause", "resume", "stop alert", "start alert")):
        return "<b>Notification controls</b>\n\nUse <code>/pause</code> to pause alerts and <code>/resume</code> to enable them again."
    if any(word in q for word in ("report", "statistics", "stats", "count", "how many alert")):
        return "<b>Statistics</b>\n\nUse <code>/stats</code> for your totals and <code>/report</code> for alert counts by wallet."
    if any(word in q for word in ("export", "download address")):
        return "<b>Export</b>\n\nUse <code>/export</code> to export your monitored addresses and labels."
    if any(word in q for word in ("test alert", "testing", "test notification")):
        return "<b>Test notifications</b>\n\nUse <code>/test</code> to send a sample alert and verify Telegram delivery."
    if any(word in q for word in ("not receive", "not receiving", "no alert", "alerts not", "not working", "bug", "error")):
        return "<b>Troubleshooting</b>\n\nCheck <code>/status</code>, confirm the wallet is listed with <code>/list</code>, and send <code>/test</code>. RPC backups retry automatically."
    if any(word in q for word in ("rpc", "backup", "connection", "offline", "reconnect")):
        return (
            "<b>RPC reliability</b>\n\n"
            "Each EVM network has a primary RPC and multiple backup endpoints. "
            "The monitor retries failed connections with backoff and resumes automatically when an endpoint recovers. "
            "Use <code>/status</code> to check network health."
        )
    if any(word in q for word in ("private key", "security", "safe", "read only")):
        return (
            "<b>Security</b>\n\n"
            "The monitor is read-only. It does not request, store, or use private keys and never signs transactions. "
            "Keep your <code>.env</code> file private."
        )
    if any(word in q for word in ("status", "health", "running", "online", "connected")):
        return "<b>Monitor status</b>\n\nUse <code>/status</code> to see whether each chain is polling, reconnecting, or unavailable."
    if any(word in q for word in ("auto delete", "autodelete", "delete message", "message deleted")):
        return "<b>Auto-delete</b>\n\nBot replies can be automatically deleted after <code>AUTO_DELETE_DELAY</code> seconds. Configure that value in <code>.env</code>."
    if any(word in q for word in ("env", "environment", "bot token", "api key", "configuration", "configure")):
        return "<b>Configuration</b>\n\nSet <code>TELEGRAM_BOT_TOKEN</code> and optional values such as <code>ADMIN_CHAT_ID</code>, <code>DB_PATH</code>, <code>TON_API_KEY</code>, and <code>AUTO_DELETE_DELAY</code> in <code>.env</code>."
    if any(word in q for word in ("database", "sqlite", "store data", "save data")):
        return "<b>Database</b>\n\nThe bot stores users, monitored addresses, chain cursors, preferences, and alert history in SQLite. The default path is <code>data/monitor.db</code>."
    if any(word in q for word in ("burn", "blacklist", "dead address")):
        return "<b>Burn-address protection</b>\n\nKnown burn addresses are filtered so burn transfers do not create normal wallet alerts."
    if any(word in q for word in ("api", "artificial intelligence", "ai", "chatbot")):
        return "<b>Offline assistant</b>\n\nThis assistant uses local project rules and runtime configuration only. It does not call an AI API or send your question outside the bot."
    if any(word in q for word in ("command", "help", "what can", "how use")):
        return "<b>Available commands</b>\n\nUse <code>/help</code> for the full command list. Start with <code>/add &lt;address&gt; [label]</code>, then use <code>/status</code> and <code>/history</code>."
    return (
        "I can answer questions about this monitor locally—supported networks, wallet setup, labels, "
        "IN/OUT alerts, token contracts, history, timezones, RPC backups, admin controls, and security.\n\n"
        "Try: <code>/ask how do I add a wallet?</code>"
    )

@router.message(Command("ask"))
async def cmd_ask(msg: types.Message):
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        ask_sessions[msg.chat.id] = time.monotonic()
        await reply_del(
            msg,
            "<b>💬 Project Assistant</b>\n\n"
            "What is your question about this bot?\n"
            "You can ask about wallets, chains, IN/OUT transfers, alerts, history, RPC backups, or admin features.\n\n"
            "<i>Type your question in the next message.</i>",
            parse_mode=ParseMode.HTML,
        )
        return
    question = parts[1].strip()
    if question.casefold() in {"cancel", "stop", "exit"}:
        ask_sessions.pop(msg.chat.id, None)
        await reply_del(msg, "<b>Project Assistant closed.</b>", parse_mode=ParseMode.HTML)
        return
    ask_sessions[msg.chat.id] = time.monotonic()
    await reply_del(msg, format_project_answers(question), parse_mode=ParseMode.HTML)


def format_project_answers(question: str) -> str:
    """Format one or several local FAQ answers for a conversational reply."""
    parts = [p.strip() for p in re.split(r"(?:\?\s+|[\r\n]+|;\s+)", question) if p.strip()]
    parts = parts[:4]
    answers = []
    for index, part in enumerate(parts, 1):
        answer = local_project_answer(part)
        if len(parts) > 1:
            answers.append(f"<b>{index}. {html.escape(part[:240])}</b>\n\n{answer}")
        else:
            answers.append(answer)
    return (
        "<b>💬 Project Assistant</b>\n\n"
        + "\n\n".join(answers)
        + "\n\n<i>Ask another project question, or send /ask cancel to close.</i>"
    )


def _is_pending_ask(msg: types.Message) -> bool:
    """Match the next normal text message after a user starts /ask."""
    started = ask_sessions.get(msg.chat.id)
    if not started:
        return False
    if time.monotonic() - started > 300:
        ask_sessions.pop(msg.chat.id, None)
        return False
    return bool(msg.text and not msg.text.lstrip().startswith("/"))


@router.message(_is_pending_ask)
async def cmd_ask_question(msg: types.Message):
    # Keep the session alive so users can ask follow-up questions without /ask.
    ask_sessions[msg.chat.id] = time.monotonic()
    question = (msg.text or "").strip()
    if question.casefold() in {"cancel", "stop", "exit"}:
        ask_sessions.pop(msg.chat.id, None)
        await reply_del(msg, "<b>Project Assistant closed.</b>", parse_mode=ParseMode.HTML)
        return
    answer = local_project_answer(question)
    await reply_del(
        msg,
        f"<b>💬 Project Assistant</b>\n\n"
        f"<b>Your question:</b> <i>{html.escape(question[:300])}</i>\n\n"
        f"{answer}\n\n"
        "<i>Ask another question directly, or send /ask cancel to close.</i>",
        parse_mode=ParseMode.HTML,
    )

# ── /start ─────────────────────────────────────────

@router.message(Command("start"))
async def cmd_start(msg: types.Message):
    cid = msg.chat.id
    u = msg.from_user
    await db.execute("INSERT OR IGNORE INTO users (chat_id, username, first_name) VALUES (?,?,?)",
                     (cid, u.username, u.first_name))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Address", callback_data="m_add"),
         InlineKeyboardButton(text="📋 My List", callback_data="m_list")],
        [InlineKeyboardButton(text="📜 History", callback_data="m_hist"),
         InlineKeyboardButton(text="⚙️ Settings", callback_data="m_set")],
        [InlineKeyboardButton(text="❓ Help", callback_data="m_help")],
    ])
    evm_n = len(EVM_KEYS)
    other = [f"{c.emoji} {c.name}" for c in _CHAINS if c.type != "evm"]
    await msg.answer(
        f"<b>🔔 Welcome to Multi-Chain Monitor</b>\n\n"
        f"Professional wallet activity alerts for <b>{len(_CHAINS)} blockchain networks</b>.\n"
        f"Track <b>incoming and outgoing</b> native-asset and token transfers in real time.\n\n"
        f"<b>What you get</b>\n"
        f"• Automatic chain detection\n"
        f"• Native coins and tokens\n"
        f"• Transaction links and contract addresses\n"
        f"• Custom wallet labels and timezone support\n"
        f"• Privacy-first, read-only monitoring\n\n"
        f"<b>Get started</b>\n"
        f"1. Tap <b>Add Address</b> below.\n"
        f"2. Send a wallet address, optionally followed by a custom label.\n"
        f"3. Receive alerts whenever activity is detected.\n\n"
        f"<b>Useful commands</b>\n"
        f"<code>/add 0xYourAddress My Wallet</code>\n"
        f"<code>/status</code> — monitor health\n"
        f"<code>/help</code> — all commands\n\n"
        f"<i>Your wallet is monitored in read-only mode. Private keys are never requested.</i>",
        parse_mode=ParseMode.HTML, reply_markup=kb,
    )

# ── Callbacks ──────────────────────────────────────

@router.callback_query(F.data == "m_add")
async def cb_add(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "<b>➕ Add Address</b>\n\n"
        "Bot <b>auto-detects</b> chain from address format!\n\n"
        "<code>/add 0x71C7...8976F</code> — EVM\n"
        "<code>/add EQAbCd...abcdef</code> — TON\n"
        "<code>/add GABC123...Exchange</code> — Stellar\n"
        "<code>/add 7xabc...defg</code> — Solana\n"
        "<code>/add TAbCdE...TRXWallet</code> — TRON\n"
        "<code>/add bc1qabc...defg</code> — Bitcoin\n\n"
        "Optional label at end:\n<code>/add 0x... MyWallet</code>",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()

@router.callback_query(F.data == "m_list")
async def cb_list(cb: types.CallbackQuery):
    cid = cb.message.chat.id
    rows = await db.fetchall("SELECT chain, address, label FROM addresses WHERE chat_id=? ORDER BY chain", (cid,))
    if not rows:
        await cb.message.edit_text("📭 No addresses. Use <code>/add</code>.", parse_mode=ParseMode.HTML)
        return
    text = "<b>📋 Your Addresses</b>\n\n"
    cur = ""
    for r in rows:
        if r["chain"] != cur:
            cur = r["chain"]
            cfg = CHAINS_BY_KEY.get(cur)
            emoji = "⛓️" if cur == "evm" else (cfg.emoji if cfg else "⬜")
            name = "All EVM" if cur == "evm" else (cfg.name if cfg else cur)
            text += f"\n{emoji} <b>{name}</b>\n"
        text += f"  ├ <code>{r['address']}</code> — <i>{r['label'] or 'No Label'}</i>\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back", callback_data="m_back")]])
    await cb.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "m_hist")
async def cb_hist(cb: types.CallbackQuery):
    cid = cb.message.chat.id
    rows = await db.fetchall(
        "SELECT chain, tx_hash, asset, amount, direction, notification_timezone, contract_addr, created_at FROM notifications "
        "WHERE chat_id=? ORDER BY created_at DESC LIMIT 10", (cid,)
    )
    if not rows:
        await cb.message.edit_text("📭 No history yet.", parse_mode=ParseMode.HTML)
        return
    text = "<b>📜 Recent History</b>\n\n"
    for i, r in enumerate(rows, 1):
        cfg = CHAINS_BY_KEY.get(r["chain"])
        emoji = cfg.emoji if cfg else "⬜"
        badge = "🔴 OUT" if r["direction"] == "outgoing" else "🟢 IN"
        dt = datetime.fromisoformat(r["created_at"])
        ts = await tz_helper.fmt(cid, dt, stored_tz=r["notification_timezone"])
        if r["contract_addr"]:
            text += f"Token contract: <code>{r['contract_addr']}</code>\n"
        text += f"{i}. {badge} {emoji} <code>{r['asset']}</code> | <code>{r['amount']}</code>\n   └ <a href='{(cfg.explorer if cfg else '')}{r['tx_hash']}'>{r['tx_hash']}</a> | {ts}\n\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ Clear", callback_data="clr_conf"),
         InlineKeyboardButton(text="🔙 Back", callback_data="m_back")]
    ])
    await cb.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
    await cb.answer()

@router.callback_query(F.data == "m_set")
async def cb_set(cb: types.CallbackQuery):
    cid = cb.message.chat.id
    tz = await tz_helper.get(cid)
    ac = await db.fetchone("SELECT COUNT(*) as c FROM addresses WHERE chat_id=?", (cid,))
    nc = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=?", (cid,))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌍 Timezone", callback_data="m_tz")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="m_back")],
    ])
    await cb.message.edit_text(
        f"<b>⚙️ Settings</b>\n\n"
        f"<b>🌍 Timezone:</b> <code>{tz}</code>\n"
        f"<b>📬 Addresses:</b> <code>{ac['c']}</code>\n"
        f"<b>🔔 Notifications:</b> <code>{nc['c']}</code>\n\n"
        f"<i>Use /timezone to set local time.</i>",
        parse_mode=ParseMode.HTML, reply_markup=kb,
    )
    await cb.answer()

@router.callback_query(F.data == "m_tz")
async def cb_tz(cb: types.CallbackQuery):
    btns = [[InlineKeyboardButton(text=f"🌍 {tz}", callback_data=f"tz_{tz}")] for tz in POPULAR_TZS]
    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data="m_set")])
    await cb.message.edit_text(
        "<b>🌍 Select Timezone</b>\n\nTap below or use:\n<code>/timezone Europe/Paris</code>",
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns),
    )
    await cb.answer()

@router.callback_query(F.data.startswith("tz_"))
async def cb_tzset(cb: types.CallbackQuery):
    tz = cb.data[3:]
    cid = cb.message.chat.id
    if tz not in available_timezones():
        await cb.answer("Invalid", show_alert=True)
        return
    await db.execute("UPDATE users SET timezone=? WHERE chat_id=?", (tz, cid))
    tz_helper.invalidate(cid)
    sample = datetime.now(ZoneInfo(tz)).strftime("%Y-%m-%d %H:%M:%S %Z")
    await cb.message.edit_text(
        f"✅ <b>Timezone: {tz}</b>\nSample: <code>{sample}</code>",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer("Updated!")

@router.callback_query(F.data == "m_help")
async def cb_help(cb: types.CallbackQuery):
    await cb.message.edit_text(
        "<b>📖 Help</b>\n\n"
        "<b>/add</b> <code>&lt;addr&gt; [label]</code> — Auto-detects chain\n"
        "<b>/remove</b> <code>&lt;chain&gt; &lt;addr&gt;</code>\n"
        "<b>/rename</b> <code>&lt;chain&gt; &lt;addr&gt; &lt;label&gt;</code>\n"
        "<b>/list</b> [chain] — Show addresses\n"
        "<b>/report</b> — Wallet alert counts\n"
        "<b>/history</b> [limit] — History\n"
        "<b>/clearhistory</b> — Clear history\n"
        "<b>/pause</b> — Pause alerts\n"
        "<b>/resume</b> — Resume alerts\n"
        "<b>/timezone</b> <code>&lt;zone&gt;</code>\n"
        "<b>/stats</b> — Your stats\n"
        "<b>/status</b> — Health check\n"
        "<b>/test</b> — Sample alert\n"
        "<b>/export</b> — Export addresses\n"
        "<b>/chains</b> — Supported chains\n"
        "<b>/help</b> — This message\n\n"
        "<i>💡 Paste any address — bot figures out the chain!</i>\n"
        "<i>💡 Burn addresses blocked on all chains.</i>\n"
        "<i>💡 Full IN+OUT detection on ALL chains.</i>",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()

@router.callback_query(F.data == "m_back")
async def cb_back(cb: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add", callback_data="m_add"),
         InlineKeyboardButton(text="📋 List", callback_data="m_list")],
        [InlineKeyboardButton(text="📜 History", callback_data="m_hist"),
         InlineKeyboardButton(text="⚙️ Settings", callback_data="m_set")],
        [InlineKeyboardButton(text="❓ Help", callback_data="m_help")],
    ])
    await cb.message.edit_text("<b>👋 Multi-Chain Monitor</b>\n\n<i>Tap a button:</i>",
                               parse_mode=ParseMode.HTML, reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "clr_conf")
async def cb_clr(cb: types.CallbackQuery):
    cid = cb.message.chat.id
    await db.execute("DELETE FROM notifications WHERE chat_id=?", (cid,))
    await cb.message.edit_text("🗑️ <b>History cleared.</b>", parse_mode=ParseMode.HTML)
    await cb.answer("Cleared", show_alert=True)

# ── /add ───────────────────────────────────────────

@router.message(Command("add"))
async def cmd_add(msg: types.Message):
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 2:
        await reply_del(
            msg,
            "❌ <b>Usage:</b> <code>/add &lt;address&gt; [label]</code>\n\n"
            "<b>Examples:</b>\n"
            "<code>/add 0x71C7...8976F</code> — EVM\n"
            "<code>/add EQAbCd...abcdef</code> — TON\n"
            "<code>/add GABC123...Exchange</code> — Stellar\n"
            "<code>/add 7xabc...defg Phantom</code> — Solana\n"
            "<code>/add TAbCdE...TRXWallet</code> — TRON\n"
            "<code>/add bc1qabc...defg ColdStorage</code> — Bitcoin\n\n"
            "<i>💡 Bot auto-detects chain from address format!</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    first = parts[1].strip()
    chain_in = CHAIN_ALIASES.get(first.lower(), first.lower())

    if chain_in in CHAINS_BY_KEY or chain_in == "evm":
        if len(parts) < 3:
            await reply_del(msg, "❌ <code>/add &lt;chain&gt; &lt;address&gt; [label]</code>\nOr: <code>/add &lt;address&gt; [label]</code>", parse_mode=ParseMode.HTML)
            return
        chain = "evm" if chain_in in CHAINS_BY_KEY and CHAINS_BY_KEY[chain_in].type == "evm" else chain_in
        addr = parts[2].strip().split()[0]
        label = " ".join(parts[2].strip().split()[1:]) if len(parts[2].strip().split()) > 1 else None
    else:
        addr = first
        label = parts[2].strip() if len(parts) > 2 else None
        detected = detect_chain(addr)
        if not detected:
            await reply_del(
                msg,
                "❌ Could not auto-detect chain.\n\n"
                "Use explicit format:\n<code>/add &lt;chain&gt; &lt;address&gt; [label]</code>\n"
                "Valid: <code>evm, ton, xlm, sol, tron, btc, sui</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        chain = detected

    # Validate
    cfg = CHAINS_BY_KEY.get(chain)
    if chain == "evm":
        valid = _RE_EVM.match(addr) is not None
    elif chain == "ton":
        valid = _RE_TON.match(addr) is not None
    elif chain == "xlm":
        valid = _RE_XLM.match(addr) is not None
    elif chain == "sol":
        valid = 32 <= len(addr) <= 44 and not addr.startswith("0x")
    elif chain == "tron":
        valid = _RE_TRON.match(addr) is not None
    elif chain == "btc":
        valid = _RE_BTC.match(addr) is not None
    elif chain == "sui":
        valid = _RE_SUI.match(addr) is not None
    else:
        valid = False

    if not valid:
        await reply_del(msg, f"❌ Invalid address for <b>{cfg.name if cfg else chain}</b>.", parse_mode=ParseMode.HTML)
        return

    # Burn check
    if is_burn(chain, addr):
        await reply_del(msg, burn_warn(chain, addr), parse_mode=ParseMode.HTML)
        return

    # Label
    if label and len(label) > 80:
        await reply_del(msg, "❌ Label max 80 chars.", parse_mode=ParseMode.HTML)
        return
    label = html.escape(label, quote=False) if label else auto_label(addr, chain)

    cid = msg.chat.id
    exists = await db.fetchone("SELECT 1 FROM addresses WHERE chat_id=? AND chain=? AND address=? COLLATE NOCASE", (cid, chain, addr))
    if exists:
        await reply_del(msg, "⚠️ Already monitored.", parse_mode=ParseMode.HTML)
        return

    await db.execute("INSERT INTO addresses (chat_id, address, chain, label) VALUES (?,?,?,?)", (cid, addr, chain, label))
    name = "All EVM" if chain == "evm" else (cfg.name if cfg else chain)
    emoji = "⛓️" if chain == "evm" else (cfg.emoji if cfg else "⬜")
    await reply_del(
        msg,
        f"✅ <b>Added</b>\n\n{emoji} <b>{name}</b>\n"
        f"<b>📍</b> <code>{addr}</code>\n"
        f"<b>🏷️</b> {label}\n\n"
        f"Alerts active for all tokens on this chain.",
        parse_mode=ParseMode.HTML,
    )

# ── /rename ────────────────────────────────────────

@router.message(Command("rename", "label"))
async def cmd_rename(msg: types.Message):
    parts = msg.text.split(maxsplit=3)
    if len(parts) < 4:
        await reply_del(msg, "❌ <code>/rename &lt;chain&gt; &lt;address&gt; &lt;label&gt;</code>", parse_mode=ParseMode.HTML)
        return
    chain_in = CHAIN_ALIASES.get(parts[1].lower(), parts[1].lower())
    chain = "evm" if chain_in in CHAINS_BY_KEY and CHAINS_BY_KEY[chain_in].type == "evm" else chain_in
    addr = parts[2].strip().lower()
    label = html.escape(parts[3].strip(), quote=False)
    if len(label) > 80:
        await reply_del(msg, "❌ Label max 80 chars.", parse_mode=ParseMode.HTML)
        return
    cur = await db.execute("UPDATE addresses SET label=? WHERE chat_id=? AND chain=? AND address=? COLLATE NOCASE", (label, msg.chat.id, chain, addr))
    if cur.rowcount:
        await reply_del(msg, f"✅ Label: <b>{label}</b>", parse_mode=ParseMode.HTML)
    else:
        await reply_del(msg, "❌ Not found. Check <code>/list</code>.", parse_mode=ParseMode.HTML)

# ── /test ──────────────────────────────────────────

@router.message(Command("test"))
async def cmd_test(msg: types.Message):
    cid = msg.chat.id
    cfg = CHAINS_BY_KEY["base"]
    sample = (
        f"<b>🟢 IN Incoming Native Transfer</b>\n\n"
        f"<b>{cfg.emoji} {cfg.name}</b>\n"
        f"<b>💰 Amount:</b> <code>0.05 ETH</code>\n"
        f"<b>📤 From:</b> <code>0xA1B2...G7H8</code>\n"
        f"<b>📥 To:</b> <code>0xYourWallet...</code> <i>(Sample)</i>\n"
        f"<b>📦 Block:</b> <code>0</code>\n"
        f"<b>🔗 Tx:</b> <code>0xsample...</code>\n"
        f"<b>⏰ Time:</b> <code>{await tz_helper.fmt(cid)}</code>"
    )
    await reply_del(
        msg,
        f"✅ <b>Sample Alert</b> (not real)\n\n{sample}\n\n"
        f"<i>If clean, Telegram delivery works. Check /status for chain health.</i>",
        parse_mode=ParseMode.HTML, disable_web_page_preview=True,
    )

# ── /remove ────────────────────────────────────────

@router.message(Command("remove"))
async def cmd_remove(msg: types.Message):
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        cid = msg.chat.id
        rows = await db.fetchall("SELECT id, chain, address, label FROM addresses WHERE chat_id=? ORDER BY chain", (cid,))
        if not rows:
            await reply_del(msg, "📭 Nothing to remove.", parse_mode=ParseMode.HTML)
            return
        btns = []
        for r in rows:
            cfg = CHAINS_BY_KEY.get(r["chain"])
            emoji = "⛓️" if r["chain"] == "evm" else (cfg.emoji if cfg else "⬜")
            lab = r["label"] or "No Label"
            short = f"{r['address'][:8]}...{r['address'][-4:]}"
            btns.append([InlineKeyboardButton(text=f"{emoji} {short} ({lab})", callback_data=f"rm_{r['id']}")])
        btns.append([InlineKeyboardButton(text="❌ Cancel", callback_data="m_back")])
        await msg.answer("<b>🗑️ Tap to remove:</b>\n\nOr: <code>/remove &lt;chain&gt; &lt;address&gt;</code>",
                         parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
        return

    chain_in = CHAIN_ALIASES.get(parts[1].lower(), parts[1].lower())
    chain = "evm" if chain_in in CHAINS_BY_KEY and CHAINS_BY_KEY[chain_in].type == "evm" else chain_in
    addr = parts[2].strip().lower()
    cid = msg.chat.id
    cur = await db.execute("DELETE FROM addresses WHERE chat_id=? AND chain=? AND address=? COLLATE NOCASE", (cid, chain, addr))
    if cur.rowcount:
        name = "All EVM" if chain == "evm" else (CHAINS_BY_KEY[chain].name if chain in CHAINS_BY_KEY else chain)
        await reply_del(msg, f"✅ Removed from {name}.", parse_mode=ParseMode.HTML)
    else:
        await reply_del(msg, "❌ Not found.", parse_mode=ParseMode.HTML)

@router.callback_query(F.data.startswith("rm_"))
async def cb_rm(cb: types.CallbackQuery):
    aid = int(cb.data.split("_")[1])
    cid = cb.message.chat.id
    row = await db.fetchone("SELECT address FROM addresses WHERE id=? AND chat_id=?", (aid, cid))
    if not row:
        await cb.answer("Already removed", show_alert=True)
        return
    await db.execute("DELETE FROM addresses WHERE id=? AND chat_id=?", (aid, cid))
    await cb.message.edit_text(f"✅ Removed <code>{row['address']}</code>.", parse_mode=ParseMode.HTML)
    await cb.answer("Removed")

# ── /list ──────────────────────────────────────────

@router.message(Command("list"))
async def cmd_list(msg: types.Message):
    cid = msg.chat.id
    parts = msg.text.split()
    filt = CHAIN_ALIASES.get(parts[1].lower(), parts[1].lower()) if len(parts) > 1 else None
    if filt and filt in CHAINS_BY_KEY and CHAINS_BY_KEY[filt].type == "evm":
        filt = "evm"
    if filt and filt not in CHAINS_BY_KEY and filt != "evm":
        await reply_del(msg, "❌ Unknown chain. Use /chains.", parse_mode=ParseMode.HTML)
        return
    sql = "SELECT chain, address, label FROM addresses WHERE chat_id=?" + (" AND chain=?" if filt else "") + " ORDER BY chain"
    params = (cid, filt) if filt else (cid,)
    rows = await db.fetchall(sql, params)
    if not rows:
        await reply_del(msg, "📭 No addresses. Use <code>/add</code>.", parse_mode=ParseMode.HTML)
        return
    text = "<b>📋 Your Addresses</b>\n\n"
    cur = ""
    for r in rows:
        if r["chain"] != cur:
            cur = r["chain"]
            cfg = CHAINS_BY_KEY.get(cur)
            emoji = "⛓️" if cur == "evm" else (cfg.emoji if cfg else "⬜")
            name = "All EVM" if cur == "evm" else (cfg.name if cfg else cur)
            text += f"\n{emoji} <b>{name}</b>\n"
        text += f"  ├ <code>{r['address']}</code> — <i>{r['label'] or 'No Label'}</i>\n"
    text += f"\n<i>Total: {len(rows)}</i>"
    await send_long(msg, text, parse_mode=ParseMode.HTML)

# ── /history ───────────────────────────────────────

@router.message(Command("history"))
async def cmd_history(msg: types.Message):
    cid = msg.chat.id
    parts = msg.text.split(maxsplit=1)
    limit = 10
    if len(parts) > 1:
        try:
            limit = max(1, min(int(parts[1]), Config.MAX_HISTORY))
        except ValueError:
            await reply_del(msg, "❌ <code>/history [1-50]</code>", parse_mode=ParseMode.HTML)
            return
    rows = await db.fetchall(
        "SELECT chain, tx_hash, asset, amount, direction, notification_timezone, contract_addr, created_at FROM notifications "
        "WHERE chat_id=? ORDER BY created_at DESC LIMIT ?", (cid, limit)
    )
    if not rows:
        await reply_del(msg, "📭 No history. Add addresses with <code>/add</code>.", parse_mode=ParseMode.HTML)
        return
    text = f"<b>📜 Last {len(rows)} Notifications</b>\n\n"
    for i, r in enumerate(rows, 1):
        cfg = CHAINS_BY_KEY.get(r["chain"])
        badge = "🔴 OUT" if r["direction"] == "outgoing" else "🟢 IN"
        dt = datetime.fromisoformat(r["created_at"])
        ts = await tz_helper.fmt(cid, dt, stored_tz=r["notification_timezone"])
        text += f"{i}. {badge} {cfg.emoji if cfg else '⬜'} <code>{r['asset']}</code> | <code>{r['amount']}</code>\n   └ <a href='{(cfg.explorer if cfg else '')}{r['tx_hash']}'>{r['tx_hash']}</a> | {ts}\n\n"
    await send_long(msg, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# ── /clearhistory ──────────────────────────────────

@router.message(Command("clearhistory"))
async def cmd_clr(msg: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, clear", callback_data="clr_conf"),
         InlineKeyboardButton(text="❌ Cancel", callback_data="m_set")]
    ])
    await msg.answer("🗑️ <b>Clear History?</b>\n\nThis deletes all notification history.\nAddresses are NOT affected.",
                     parse_mode=ParseMode.HTML, reply_markup=kb)

# ── /pause / resume ────────────────────────────────

@router.message(Command("pause"))
async def cmd_pause(msg: types.Message):
    await db.execute("UPDATE users SET notifications_enabled=0 WHERE chat_id=?", (msg.chat.id,))
    notifier._user_cache.pop(msg.chat.id, None)
    await reply_del(msg, "🔕 <b>Paused.</b> Use <code>/resume</code> to re-enable.", parse_mode=ParseMode.HTML)

@router.message(Command("resume"))
async def cmd_resume(msg: types.Message):
    await db.execute("UPDATE users SET notifications_enabled=1 WHERE chat_id=?", (msg.chat.id,))
    notifier._user_cache.pop(msg.chat.id, None)
    await reply_del(msg, "🔔 <b>Resumed.</b>", parse_mode=ParseMode.HTML)

# ── /timezone ──────────────────────────────────────

@router.message(Command("timezone"))
async def cmd_tz(msg: types.Message):
    cid = msg.chat.id
    parts = msg.text.split(maxsplit=1)
    if len(parts) == 1:
        cur = await tz_helper.get(cid)
        quick = "\n".join(f"  <code>{t}</code>" for t in POPULAR_TZS)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🌍 {t}", callback_data=f"tz_{t}")] for t in POPULAR_TZS[:6]
        ] + [[InlineKeyboardButton(text="🌍 More...", callback_data="m_tz")]])
        await msg.answer(
            f"<b>🌍 Timezone</b>\n\nCurrent: <code>{cur}</code>\n\n<b>Quick picks:</b>\n{quick}\n\n"
            f"Or: <code>/timezone Europe/Paris</code>",
            parse_mode=ParseMode.HTML, reply_markup=kb,
        )
        return
    tz_in = parts[1].strip()
    if tz_in not in available_timezones():
        await reply_del(msg, f"❌ Invalid: <code>{tz_in}</code>\nUse IANA format like <code>Asia/Kolkata</code>.", parse_mode=ParseMode.HTML)
        return
    await db.execute("UPDATE users SET timezone=? WHERE chat_id=?", (tz_in, cid))
    tz_helper.invalidate(cid)
    sample = datetime.now(ZoneInfo(tz_in)).strftime("%Y-%m-%d %H:%M:%S %Z")
    await reply_del(msg, f"✅ <b>Timezone: {tz_in}</b>\nSample: <code>{sample}</code>", parse_mode=ParseMode.HTML)

# ── /stats ─────────────────────────────────────────

@router.message(Command("stats"))
async def cmd_stats(msg: types.Message):
    cid = msg.chat.id
    ac = await db.fetchone("SELECT COUNT(*) as c FROM addresses WHERE chat_id=?", (cid,))
    nc = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=?", (cid,))
    n24 = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=? AND created_at > datetime('now', '-1 day')", (cid,))
    inn = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=? AND direction='incoming'", (cid,))
    out = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=? AND direction='outgoing'", (cid,))
    tz = await tz_helper.get(cid)
    cbreak = await db.fetchall("SELECT chain, COUNT(*) as c FROM addresses WHERE chat_id=? GROUP BY chain", (cid,))
    ctext = "\n".join(f"  {CHAINS_BY_KEY.get(r['chain'], ChainCfg(r['chain'], r['chain'], '⬜', '', '', 0)).emoji} {CHAINS_BY_KEY.get(r['chain'], ChainCfg(r['chain'], r['chain'], '⬜', '', '', 0)).name}: <code>{r['c']}</code>" for r in cbreak)
    await reply_del(
        msg,
        f"<b>📊 Stats</b>\n\n"
        f"<b>📬 Addresses:</b> <code>{ac['c']}</code>\n"
        f"<b>🔔 Notifications:</b> <code>{nc['c']}</code>\n"
        f"  ├ 🟢 IN: <code>{inn['c']}</code>\n"
        f"  └ 🔴 OUT: <code>{out['c']}</code>\n"
        f"<b>📈 24h:</b> <code>{n24['c']}</code>\n"
        f"<b>🌍 Timezone:</b> <code>{tz}</code>\n\n"
        f"<b>By Chain:</b>\n{ctext}",
        parse_mode=ParseMode.HTML,
    )

# ── /status ────────────────────────────────────────

@router.message(Command("status"))
async def cmd_status(msg: types.Message):
    cid = msg.chat.id
    pref = await db.get_user_prefs(cid)
    ac = await db.fetchone("SELECT COUNT(*) as c FROM addresses WHERE chat_id=?", (cid,))
    n24 = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=? AND created_at > datetime('now', '-1 day')", (cid,))
    in24 = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=? AND direction='incoming' AND created_at > datetime('now', '-1 day')", (cid,))
    out24 = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=? AND direction='outgoing' AND created_at > datetime('now', '-1 day')", (cid,))
    latest = await db.fetchone("SELECT chain, asset, amount, direction, created_at FROM notifications WHERE chat_id=? ORDER BY created_at DESC LIMIT 1", (cid,))
    uptime = "not started"
    if app_start:
        s = max(0, int((datetime.now() - app_start).total_seconds()))
        uptime = f"{s // 3600}h {(s % 3600) // 60}m"
    online = sum(1 for m in monitors.values() if m.running and (getattr(m, "_w3", True) is not None))
    offline = max(0, len(monitors) - online)

    text = (
        f"<b>📊 Status</b>\n\n"
        f"<b>Addresses:</b> <code>{ac['c']}</code>\n"
        f"<b>Alerts (24h):</b> <code>{n24['c']}</code>\n"
        f"  IN: <code>{in24['c']}</code> | OUT: <code>{out24['c']}</code>\n"
        f"<b>Notifications:</b> <code>{'ON' if pref['enabled'] else 'PAUSED'}</code>\n"
        f"<b>Networks:</b> <code>{len(_CHAINS)}</code>\n"
        f"<b>Health:</b> <code>{online} online</code> / <code>{offline} reconnecting</code>\n"
        f"<b>Uptime:</b> <code>{uptime}</code>\n"
    )
    if latest:
        badge = "🔴 OUT" if latest["direction"] == "outgoing" else "🟢 IN"
        text += f"\n<b>Latest:</b> {badge} <code>{latest['amount']} {latest['asset']}</code> ({latest['chain']})\n"
    user_chains = set(r["chain"] for r in await db.fetchall("SELECT DISTINCT chain FROM addresses WHERE chat_id=?", (cid,)))
    if "evm" in user_chains:
        user_chains.update(EVM_KEYS)
    if user_chains:
        text += "\n<b>Your networks:</b>\n"
        for k in sorted(user_chains):
            cfg = CHAINS_BY_KEY.get(k)
            if not cfg:
                continue
            m = monitors.get(k)
            state = "🟢 polling" if m and m.running and (getattr(m, "_w3", True) is not None) else "🔴 reconnecting"
            text += f"{cfg.emoji} <b>{cfg.name}</b>: {state}\n"
    else:
        text += "\n<i>No addresses monitored yet.</i>"
    await msg.answer(text, parse_mode=ParseMode.HTML)

# ── /report ────────────────────────────────────────

@router.message(Command("report", "wallets"))
async def cmd_report(msg: types.Message):
    cid = msg.chat.id
    rows = await db.fetchall("SELECT chain, address, label FROM addresses WHERE chat_id=? ORDER BY chain", (cid,))
    if not rows:
        await reply_del(msg, "📭 No wallets. Use <code>/add</code>.", parse_mode=ParseMode.HTML)
        return
    text = "<b>👛 Wallet Report</b>\n\n"
    cur = None
    for r in rows:
        if r["chain"] != cur:
            cur = r["chain"]
            cfg = CHAINS_BY_KEY.get(cur)
            cc = await db.fetchone("SELECT COUNT(*) as c FROM addresses WHERE chat_id=? AND chain=?", (cid, cur))
            text += f"<b>{cfg.emoji if cfg else '⛓️'} {cfg.name if cfg else cur}</b> — {cc['c']} wallet(s)\n"
        inn = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=? AND chain=? AND direction='incoming'", (cid, cur))
        out = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=? AND chain=? AND direction='outgoing'", (cid, cur))
        text += f"  <code>{r['address']}</code> — <i>{r['label'] or 'No label'}</i> — 🟢{inn['c']} / 🔴{out['c']}\n"
    text += "\n<i>Use /list or /rename.</i>"
    await send_long(msg, text, parse_mode=ParseMode.HTML)

# ── /export ────────────────────────────────────────

@router.message(Command("export"))
async def cmd_export(msg: types.Message):
    cid = msg.chat.id
    rows = await db.fetchall("SELECT chain, address, label FROM addresses WHERE chat_id=? ORDER BY chain", (cid,))
    if not rows:
        await reply_del(msg, "📭 Nothing to export.", parse_mode=ParseMode.HTML)
        return
    lines = ["# Multi-Chain Monitor Export", f"# User: {cid}", f"# Time: {await tz_helper.fmt(cid)}", ""]
    for r in rows:
        cfg = CHAINS_BY_KEY.get(r["chain"])
        name = "EVM" if r["chain"] == "evm" else (cfg.name if cfg else r["chain"])
        lines.append(f"[{name}] {r['address']}  # {r['label'] or 'No Label'}")
    text = "\n".join(lines)
    await msg.answer(f"<b>📤 Export</b>\n\n<pre>{text}</pre>\n\n<i>Copy to back up your addresses.</i>", parse_mode=ParseMode.HTML)

# ── /chains ────────────────────────────────────────

@router.message(Command("chains"))
async def cmd_chains(msg: types.Message):
    evm = [c for c in _CHAINS if c.type == "evm"]
    other = [c for c in _CHAINS if c.type != "evm"]
    text = (
        f"<b>⛓️ Supported Chains ({len(_CHAINS)})</b>\n\n"
        f"<b>⛓️ EVM</b> — add once, monitored across all {len(evm)} networks:\n"
        f"{', '.join(f'{c.emoji} {c.name}' for c in evm)}\n"
        f"<i>All ERC-20 tokens auto-detected.</i>\n\n"
        f"<b>Other Networks:</b>\n"
    )
    for c in other:
        note = {"ton": "Native + All Jettons", "xlm": "Native + All Assets", "sol": "Native + All SPLs",
                "tron": "Native + All TRC-20s", "btc": "Native BTC only", "sui": "Native + All Move Coins"}.get(c.key, "")
        text += f"{c.emoji} <b>{c.name}</b> — <code>{c.key}</code> — <code>{c.native}</code> — <i>{note}</i>\n"
    await send_long(msg, text, parse_mode=ParseMode.HTML)

# ── Local project assistant ────────────────────────

def local_project_answer_legacy(question: str) -> str:
    """Unused legacy assistant implementation kept out of the router."""
    q = question.casefold().strip()
    chain_names = ", ".join(c.name for c in _CHAINS)

    if any(word in q for word in ("chain", "network", "support")):
        return (
            f"<b>Supported networks ({len(_CHAINS)})</b>\n\n"
            f"{html.escape(chain_names)}\n\n"
            "Use <code>/chains</code> for the complete formatted list."
        )
    if any(word in q for word in ("add", "monitor", "wallet", "address")):
        return (
            "<b>Add a wallet</b>\n\n"
            "<code>/add &lt;address&gt; [label]</code>\n"
            "Example: <code>/add 0x1234... Main wallet</code>\n\n"
            "For an ambiguous address, use "
            "<code>/add &lt;chain&gt; &lt;address&gt; [label]</code>."
        )
    if any(word in q for word in ("label", "rename", "name")):
        return (
            "Use <code>/label &lt;chain&gt; &lt;address&gt; &lt;label&gt;</code> "
            "to create or change a custom wallet label."
        )
    if any(word in q for word in ("incoming", "outgoing", "outbound", "in and out", "transaction")):
        return (
            "The monitor detects both incoming and outgoing activity. "
            "ERC-20, native transfers, and supported chain token transfers "
            "include the sender, recipient, amount, transaction link, and contract address when available."
        )
    if any(word in q for word in ("contract", "ca", "token address")):
        return "Token alerts include the token contract address as <b>CA</b> when the network provides it."
    if any(word in q for word in ("admin", "ban", "unban", "spam")):
        return (
            "Administrators can use <code>/admin</code>, <code>/admin_user &lt;chat_id&gt;</code>, "
            "<code>/ban &lt;chat_id&gt;</code>, and <code>/unban &lt;chat_id&gt;</code>. "
            "Anti-spam limits apply to regular users and never to admins."
        )
    if any(word in q for word in ("history", "status", "alert", "notification")):
        return (
            "Use <code>/history</code> for recent alerts, <code>/status</code> for monitor health, "
            "and <code>/stats</code> for your activity totals."
        )
    if any(word in q for word in ("timezone", "time", "ist", "utc")):
        return "Use <code>/timezone &lt;zone&gt;</code>, for example <code>/timezone Asia/Kolkata</code>."
    if any(word in q for word in ("rpc", "backup", "fallback", "connection")):
        return (
            "EVM networks use multiple RPC endpoints with retry and backoff. "
            "If one endpoint fails, the monitor tries its configured backups automatically."
        )
    if any(word in q for word in ("private key", "security", "safe", "privacy")):
        return (
            "This bot is read-only: it monitors public wallet addresses and never asks for or stores private keys. "
            "Keep your <code>.env</code> file private."
        )
    return (
        "I can answer questions about this project locally. Try:\n"
        "<code>/ask how do I add a wallet?</code>\n"
        "<code>/ask which networks are supported?</code>\n"
        "<code>/ask how are incoming transfers detected?</code>"
    )


# Legacy handler intentionally not registered; the primary /ask handler is above.
async def cmd_ask_legacy(msg: types.Message):
    question = (msg.text or "").partition(" ")[2].strip()
    if not question:
        await reply_del(msg, "Usage: <code>/ask your question</code>", parse_mode=ParseMode.HTML)
        return
    await reply_del(msg, local_project_answer(question), parse_mode=ParseMode.HTML)


# ── /help ──────────────────────────────────────────

@router.message(Command("help"))
async def cmd_help(msg: types.Message):
    # /ask is also registered in Telegram's command menu.
    await msg.answer(
        "<b>📖 Commands</b>\n\n"
        "<b>➕ /add</b> <code>&lt;addr&gt; [label]</code> — Auto-detects chain\n"
        "<b>🗑️ /remove</b> <code>&lt;chain&gt; &lt;addr&gt;</code> — Or tap inline\n"
        "<b>✏️ /rename</b> <code>&lt;chain&gt; &lt;addr&gt; &lt;label&gt;</code>\n"
        "<b>📋 /list</b> [chain] — Show addresses\n"
        "<b>👛 /report</b> — Per-wallet alert counts\n"
        "<b>📜 /history</b> [limit] — History (max 50)\n"
        "<b>🗑️ /clearhistory</b> — Clear history\n"
        "<b>🔕 /pause</b> — Pause alerts\n"
        "<b>🔔 /resume</b> — Resume alerts\n"
        "<b>🌍 /timezone</b> <code>&lt;zone&gt;</code> — Set timezone\n"
        "<b>📊 /stats</b> — Your statistics\n"
        "<b>📊 /status</b> — Monitor health\n"
        "<b>🧪 /test</b> — Sample alert\n"
        "<b>📤 /export</b> — Export addresses\n"
        "<b>⛓️ /chains</b> — List networks\n"
        "<b>❓ /help</b> — This message\n\n"
        "<i>💡 Paste any address — bot auto-detects chain!</i>\n"
        "<i>💡 Burn addresses blocked on all chains.</i>\n"
        "<i>💡 Full IN+OUT detection on ALL chains.</i>\n"
        "<i>💡 Notifications use YOUR timezone.</i>",
        parse_mode=ParseMode.HTML,
    )

# ═══════════════════════════════════════════════════
# ADMIN
# ═══════════════════════════════════════════════════

@router.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    cid = msg.chat.id
    if not await is_admin(cid):
        await reply_del(msg, "❌ Unauthorized.", parse_mode=ParseMode.HTML)
        return
    tu = await db.fetchone("SELECT COUNT(*) as c FROM users")
    ta = await db.fetchone("SELECT COUNT(*) as c FROM addresses")
    tn = await db.fetchone("SELECT COUNT(*) as c FROM notifications")
    au = await db.fetchone("SELECT COUNT(DISTINCT chat_id) as c FROM addresses")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Stats", callback_data="a_stats"),
         InlineKeyboardButton(text="📢 Broadcast", callback_data="a_bc")],
        [InlineKeyboardButton(text="👥 Users", callback_data="a_users"),
         InlineKeyboardButton(text="🔥 Burn List", callback_data="a_burn")],
        [InlineKeyboardButton(text="🔄 Restart", callback_data="a_restart")],
    ])
    await msg.answer(
        f"<b>🔐 Admin Panel</b>\n\n"
        f"<b>👥 Users:</b> <code>{tu['c']}</code> (active: {au['c']})\n"
        f"<b>📬 Addresses:</b> <code>{ta['c']}</code>\n"
        f"<b>🔔 Notifications:</b> <code>{tn['c']}</code>\n\n"
        f"<i>Select action:</i>",
        parse_mode=ParseMode.HTML, reply_markup=kb,
    )

@router.callback_query(F.data == "a_stats")
async def cb_astats(cb: types.CallbackQuery):
    if not await is_admin(cb.message.chat.id):
        await cb.answer("No", show_alert=True); return
    tu = await db.fetchone("SELECT COUNT(*) as c FROM users")
    ta = await db.fetchone("SELECT COUNT(*) as c FROM addresses")
    tn = await db.fetchone("SELECT COUNT(*) as c FROM notifications")
    n24 = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE created_at > datetime('now', '-1 day')")
    cbreak = await db.fetchall("SELECT chain, COUNT(*) as c FROM addresses GROUP BY chain ORDER BY c DESC")
    ctext = "\n".join(f"  {CHAINS_BY_KEY.get(r['chain'], ChainCfg(r['chain'], r['chain'], '⬜', '', '', 0)).emoji} {CHAINS_BY_KEY.get(r['chain'], ChainCfg(r['chain'], r['chain'], '⬜', '', '', 0)).name}: <code>{r['c']}</code>" for r in cbreak)
    await cb.message.edit_text(
        f"<b>📊 Global Stats</b>\n\n"
        f"<b>👥 Users:</b> <code>{tu['c']}</code>\n"
        f"<b>📬 Addresses:</b> <code>{ta['c']}</code>\n"
        f"<b>🔔 Total:</b> <code>{tn['c']}</code>\n"
        f"<b>📈 24h:</b> <code>{n24['c']}</code>\n\n"
        f"<b>By Chain:</b>\n{ctext}",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()

@router.callback_query(F.data == "a_bc")
async def cb_abc(cb: types.CallbackQuery):
    if not await is_admin(cb.message.chat.id):
        await cb.answer("No", show_alert=True); return
    await cb.message.edit_text(
        "<b>📢 Broadcast</b>\n\nUse:\n<code>/broadcast Your message...</code>\nSends to ALL users.",
        parse_mode=ParseMode.HTML,
    )
    await cb.answer()

@router.message(Command("broadcast"))
async def cmd_broadcast(msg: types.Message):
    cid = msg.chat.id
    if not await is_admin(cid):
        await reply_del(msg, "❌ Unauthorized.", parse_mode=ParseMode.HTML); return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await reply_del(msg, "❌ <code>/broadcast message...</code>", parse_mode=ParseMode.HTML); return
    text = parts[1]
    users = await db.fetchall("SELECT chat_id FROM users")
    sent = failed = 0
    for u in users:
        try:
            await msg.bot.send_message(u["chat_id"], f"<b>📢 Announcement</b>\n\n{text}", parse_mode=ParseMode.HTML)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await reply_del(msg, f"✅ Broadcast: <code>{sent}</code> sent, <code>{failed}</code> failed.", parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "a_users")
async def cb_ausers(cb: types.CallbackQuery):
    if not await is_admin(cb.message.chat.id):
        await cb.answer("No", show_alert=True); return
    users = await db.fetchall(
        "SELECT u.chat_id, u.username, u.first_name, u.timezone, u.notifications_enabled, u.is_admin, u.is_banned, u.created_at, "
        "(SELECT COUNT(*) FROM addresses a WHERE a.chat_id=u.chat_id) AS wallet_count, "
        "(SELECT COUNT(*) FROM notifications n WHERE n.chat_id=u.chat_id AND n.direction='incoming') AS in_count, "
        "(SELECT COUNT(*) FROM notifications n WHERE n.chat_id=u.chat_id AND n.direction='outgoing') AS out_count, "
        "(SELECT MAX(created_at) FROM notifications n WHERE n.chat_id=u.chat_id) AS last_alert "
        "FROM users u ORDER BY u.created_at DESC LIMIT 10"
    )
    text = "<b>👥 Recent Users</b>\n\n"
    for u in users:
        username = f"@{html.escape(u['username'])}" if u["username"] else "not set"
        full_name = html.escape(u["first_name"] or "not set")
        role = "admin" if u["is_admin"] else "user"
        ban_state = "BANNED" if u["is_banned"] else "active"
        last_alert = u["last_alert"] or "no alerts"
        text += f"ID: <code>{u['chat_id']}</code> | Name: <code>{full_name}</code> | Username: <code>{username}</code>\n"
        text += f"Role: <code>{role}</code> | State: <code>{ban_state}</code> | Timezone: <code>{u['timezone']}</code> | Notifications: <code>{'ON' if u['notifications_enabled'] else 'PAUSED'}</code>\n"
        text += f"Wallets: <code>{u['wallet_count']}</code> | IN: <code>{u['in_count']}</code> | OUT: <code>{u['out_count']}</code> | Last alert: <code>{last_alert}</code>\n"
        name = u["first_name"] or u["username"] or "Unknown"
        st = "🔔" if u["notifications_enabled"] else "🔕"
        text += f"• <code>{u['chat_id']}</code> — <b>{name}</b> {st} {u['timezone']}\n"
    await cb.message.edit_text(text, parse_mode=ParseMode.HTML)
    await cb.answer()

@router.message(Command("admin_user"))
async def cmd_admin_user(msg: types.Message):
    if not await is_admin(msg.chat.id):
        await reply_del(msg, "Unauthorized.", parse_mode=ParseMode.HTML)
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().lstrip("-").isdigit():
        await reply_del(msg, "Usage: <code>/admin_user &lt;chat_id&gt;</code>", parse_mode=ParseMode.HTML)
        return
    target = int(parts[1].strip())
    user = await db.fetchone(
        "SELECT chat_id, username, first_name, timezone, notifications_enabled, is_admin, is_banned, created_at "
        "FROM users WHERE chat_id=?", (target,)
    )
    if not user:
        await reply_del(msg, "User not found.", parse_mode=ParseMode.HTML)
        return
    wallets = await db.fetchall(
        "SELECT chain, address, label, created_at FROM addresses WHERE chat_id=? ORDER BY chain, created_at",
        (target,)
    )
    counts = await db.fetchone(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN direction='incoming' THEN 1 ELSE 0 END) AS incoming, "
        "SUM(CASE WHEN direction='outgoing' THEN 1 ELSE 0 END) AS outgoing, "
        "MAX(created_at) AS last_alert FROM notifications WHERE chat_id=?", (target,)
    )
    username = f"@{html.escape(user['username'])}" if user["username"] else "not set"
    name = html.escape(user["first_name"] or "not set")
    text = (
        f"<b>Admin User Details</b>\n\n"
        f"<b>Chat ID:</b> <code>{user['chat_id']}</code>\n"
        f"<b>Name:</b> <code>{name}</code>\n"
        f"<b>Username:</b> <code>{username}</code>\n"
        f"<b>Role:</b> <code>{'admin' if user['is_admin'] else 'user'}</code>\n"
        f"<b>State:</b> <code>{'BANNED' if user['is_banned'] else 'active'}</code>\n"
        f"<b>Timezone:</b> <code>{user['timezone']}</code>\n"
        f"<b>Notifications:</b> <code>{'ON' if user['notifications_enabled'] else 'PAUSED'}</code>\n"
        f"<b>Registered:</b> <code>{user['created_at']}</code>\n"
        f"<b>Alerts:</b> <code>{counts['total'] or 0}</code> total | "
        f"<code>{counts['incoming'] or 0}</code> IN | <code>{counts['outgoing'] or 0}</code> OUT\n"
        f"<b>Last alert:</b> <code>{counts['last_alert'] or 'none'}</code>\n\n"
        f"<i>Wallet addresses are partially hidden for privacy.</i>\n"
        f"<b>Wallets ({len(wallets)}):</b>\n"
    )
    if wallets:
        for wallet in wallets:
            wallet = dict(wallet)
            wallet["address"] = mask_address(wallet["address"])
            cfg = CHAINS_BY_KEY.get(wallet["chain"])
            chain_name = cfg.name if cfg else wallet["chain"]
            label = html.escape(wallet["label"] or "No label")
            text += f"{chain_name}: <code>{wallet['address']}</code> — <i>{label}</i>\n"
    else:
        text += "None\n"
    await send_long(msg, text, parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "a_burn")
async def cb_aburn(cb: types.CallbackQuery):
    if not await is_admin(cb.message.chat.id):
        await cb.answer("No", show_alert=True); return
    text = "<b>🔥 Burn Addresses</b>\n\n"
    for chain, addrs in _BURN_MAP.items():
        if addrs:
            cfg = CHAINS_BY_KEY.get(chain)
            name = cfg.name if cfg else chain.upper()
            text += f"<b>{name}</b> ({len(addrs)})\n"
            for a in list(addrs)[:5]:
                text += f"  <code>{a}</code>\n"
            if len(addrs) > 5:
                text += f"  ... +{len(addrs) - 5} more\n"
            text += "\n"
    await cb.message.edit_text(text, parse_mode=ParseMode.HTML)
    await cb.answer()

@router.callback_query(F.data == "a_restart")
async def cb_arestart(cb: types.CallbackQuery):
    if not await is_admin(cb.message.chat.id):
        await cb.answer("No", show_alert=True); return
    for m in monitors.values():
        m.running = False
    await cb.message.edit_text("🔄 <b>Monitors stopped.</b> They'll auto-restart. Check /status.", parse_mode=ParseMode.HTML)
    await cb.answer("Restarted")

@router.message(Command("admin_promote"))
async def cmd_promote(msg: types.Message):
    if not await is_admin(msg.chat.id):
        await reply_del(msg, "❌ Unauthorized.", parse_mode=ParseMode.HTML); return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await reply_del(msg, "❌ <code>/admin_promote &lt;chat_id&gt;</code>", parse_mode=ParseMode.HTML); return
    try:
        target = int(parts[1].strip())
    except ValueError:
        await reply_del(msg, "❌ Invalid chat_id.", parse_mode=ParseMode.HTML); return
    await db.execute("UPDATE users SET is_admin=1 WHERE chat_id=?", (target,))
    await reply_del(msg, f"✅ <code>{target}</code> promoted to admin.", parse_mode=ParseMode.HTML)

# ── Error Handler ──────────────────────────────────

@router.message(Command("ban"))
async def cmd_ban(msg: types.Message):
    if not await is_admin(msg.chat.id):
        await reply_del(msg, "Unauthorized.", parse_mode=ParseMode.HTML)
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().lstrip("-").isdigit():
        await reply_del(msg, "Usage: <code>/ban &lt;chat_id&gt;</code>", parse_mode=ParseMode.HTML)
        return
    target = int(parts[1].strip())
    if target == msg.chat.id or (Config.ADMIN_ID and target == Config.ADMIN_ID):
        await reply_del(msg, "You cannot ban an administrator.", parse_mode=ParseMode.HTML)
        return
    cur = await db.execute("UPDATE users SET is_banned=1 WHERE chat_id=?", (target,))
    result = "User banned." if cur.rowcount else "User not found."
    await reply_del(msg, f"{result} <code>{target}</code>", parse_mode=ParseMode.HTML)

@router.message(Command("unban"))
async def cmd_unban(msg: types.Message):
    if not await is_admin(msg.chat.id):
        await reply_del(msg, "Unauthorized.", parse_mode=ParseMode.HTML)
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().lstrip("-").isdigit():
        await reply_del(msg, "Usage: <code>/unban &lt;chat_id&gt;</code>", parse_mode=ParseMode.HTML)
        return
    target = int(parts[1].strip())
    cur = await db.execute("UPDATE users SET is_banned=0 WHERE chat_id=?", (target,))
    result = "User unbanned." if cur.rowcount else "User not found."
    await reply_del(msg, f"{result} <code>{target}</code>", parse_mode=ParseMode.HTML)

@router.errors()
async def on_error(event: types.ErrorEvent, bot: Bot):
    update = event.update
    chat = None
    if update.message:
        chat = update.message.chat
    elif update.callback_query and update.callback_query.message:
        chat = update.callback_query.message.chat
    logger.error("Error on update %s: %s", update.update_id, event.exception, exc_info=event.exception)
    if chat:
        try:
            await bot.send_message(chat.id, "⚠️ <b>Something went wrong.</b>\n\nLogged. Try again or check /help.", parse_mode=ParseMode.HTML)
        except Exception:
            pass
    return True

# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Multi-Chain Monitor")
    p.add_argument("--list-chains", action="store_true", help="List networks and exit")
    p.add_argument("--check-rpcs", action="store_true", help="Test RPCs and exit")
    return p

def cli_list():
    for c in _CHAINS:
        eps = [c.rpc, *c.rpc_fallbacks] if c.rpc else []
        print(f"{c.key:<12} {c.name:<20} {c.type:<5} {len(eps)} endpoint(s)")

async def cli_check():
    async with httpx.AsyncClient(timeout=10) as client:
        for c in _CHAINS:
            eps = [c.rpc, *c.rpc_fallbacks] if c.rpc else []
            if not eps:
                print(f"{c.key:<12} SKIP (API)")
                continue
            ok = None
            for ep in dict.fromkeys(eps):
                if not ep:
                    continue
                try:
                    method = "sui_getLatestCheckpointSequenceNumber" if c.type == "sui" else "eth_chainId"
                    r = await client.post(ep, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": []})
                    r.raise_for_status()
                    if "result" in r.json():
                        ok = ep
                        break
                except Exception:
                    continue
            print(f"{c.key:<12} {'OK' if ok else 'FAIL':<5} {ok or '(all down)'}")

async def main():
    global db, tz_helper, notifier, evm_cache, spl_cache, monitors, app_start
    app_start = datetime.now()
    asyncio.get_running_loop().set_exception_handler(_asyncio_exception_handler)

    if not Config.TOKEN or Config.TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("\n❌ Set TELEGRAM_BOT_TOKEN in .env\n")
        return

    db = DB(Config.DB_PATH)
    tz_helper = TZHelper(db)
    evm_cache = EVMCache()
    spl_cache = SPLCache()
    telegram_session = AiohttpSession(timeout=Config.TELEGRAM_TIMEOUT)
    bot = Bot(token=Config.TOKEN, session=telegram_session)
    notifier = Notifier(db, tz_helper, bot)
    dp = Dispatcher()
    dp.include_router(router)

    monitors = {}
    tasks = []
    for c in _CHAINS:
        if c.type == "evm":
            m = EVMMonitor(c.key, c, db, notifier, evm_cache)
        elif c.key == "ton":
            m = TONMonitor(c.key, c, db, notifier)
        elif c.key == "xlm":
            m = XLMMonitor(c.key, c, db, notifier)
        elif c.key == "sol":
            m = SOLMonitor(c.key, c, db, notifier, spl_cache)
        elif c.key == "tron":
            m = TRONMonitor(c.key, c, db, notifier)
        elif c.key == "btc":
            m = BTCMonitor(c.key, c, db, notifier)
        elif c.key == "sui":
            m = SUIMonitor(c.key, c, db, notifier)
        else:
            continue
        monitors[c.key] = m
        tasks.append(asyncio.create_task(m.run(), name=f"monitor:{c.key}"))

    cmds = [
        BotCommand(command="start", description="Main menu"),
        BotCommand(command="ask", description="Ask about the project locally"),
        BotCommand(command="status", description="Monitor health"),
        BotCommand(command="add", description="Add wallet (auto-detects chain)"),
        BotCommand(command="rename", description="Rename wallet label"),
        BotCommand(command="label", description="Set custom wallet label"),
        BotCommand(command="admin_user", description="Admin: view full user details"),
        BotCommand(command="ban", description="Admin: ban a user"),
        BotCommand(command="unban", description="Admin: unban a user"),
        BotCommand(command="remove", description="Remove wallet"),
        BotCommand(command="list", description="List wallets"),
        BotCommand(command="report", description="Wallet report with counts"),
        BotCommand(command="history", description="Alert history"),
        BotCommand(command="clearhistory", description="Clear history"),
        BotCommand(command="pause", description="Pause alerts"),
        BotCommand(command="resume", description="Resume alerts"),
        BotCommand(command="stats", description="Your statistics"),
        BotCommand(command="chains", description="Supported networks"),
        BotCommand(command="timezone", description="Set timezone"),
        BotCommand(command="export", description="Export addresses"),
        BotCommand(command="help", description="Command help"),
        BotCommand(command="test", description="Test alert"),
    ]
    await bot.set_my_commands(cmds)

    if Config.ADMIN_ID:
        try:
            await bot.send_message(
                Config.ADMIN_ID,
                f"🤖 <b>Multi-Chain Monitor Online</b>\n\n"
                f"✅ <b>Networks:</b> {len(_CHAINS)}\n"
                f"✅ <b>Monitoring:</b> native + all tokens\n"
                f"✅ <b>Directions:</b> FULL IN+OUT on ALL chains\n"
                f"✅ <b>Reliability:</b> RPC failover + reconnect\n"
                f"✅ <b>Features:</b> Burn detect, auto chain, auto-delete, admin\n\n"
                f"<code>/admin</code> for panel | <code>/help</code> for commands",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    polling_task = asyncio.create_task(dp.start_polling(bot), name="telegram-polling")
    try:
        await asyncio.gather(polling_task, *tasks)
    finally:
        polling_task.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(polling_task, *tasks, return_exceptions=True)
        await asyncio.gather(*(m.stop() for m in monitors.values()), return_exceptions=True)
        await spl_cache.close()
        await bot.session.close()
        # Give aiohttp time to finish closing SSL transports before the loop exits.
        await asyncio.sleep(0.25)

if __name__ == "__main__":
    args = build_parser().parse_args()
    if args.list_chains:
        cli_list()
    elif args.check_rpcs:
        asyncio.run(cli_check())
    else:
        asyncio.run(main())
