#!/usr/bin/env python3
"""
🔮 telegram-multichain-monitor
Monitors: EVM (19 chains) + TON + XLM + SOL + TRON + BTC + Sui
Detects: ALL tokens — native, ERC-20, Jettons, SPL, TRC-20, Stellar assets
Features: Per-user timezone, notification history, inline keyboards, stats
Author: Assistant | License: MIT
"""

import asyncio
import argparse
import base64
import html as html_lib
from collections import defaultdict, deque
import logging
import os
import re
import sqlite3
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo, available_timezones

import httpx
from aiogram import BaseMiddleware, Bot, Dispatcher, Router, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from web3.middleware import ExtraDataToPOAMiddleware
from web3 import AsyncWeb3, Web3
from web3.providers import AsyncHTTPProvider

# ═══════════════════════════════════════════════════
def load_local_env():
    """Load simple KEY=VALUE entries from the project .env file."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.isfile(env_path):
        return
    with open(env_path, encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if key:
                os.environ.setdefault(key, value)


load_local_env()

# CONFIGURATION
# ═══════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
DB_PATH = os.getenv("DB_PATH", "data/monitor.db")
TON_API_KEY = os.getenv("TON_API_KEY", "")
EXTRA_EVM_CHAINS = os.getenv("EXTRA_EVM_CHAINS", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)


class RedactTelegramIdFilter(logging.Filter):
    """Prevent Telegram bot numeric IDs from appearing in console logs."""
    _pattern = re.compile(r"\bid=\d+\b")
    _url_pattern = re.compile(r"https?://[^\s)]+")

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno == logging.WARNING:
            return False
        message = self._pattern.sub("id=REDACTED", record.getMessage())
        record.msg = self._url_pattern.sub("[rpc]", message)
        record.args = ()
        return True


for _handler in logging.getLogger().handlers:
    _handler.addFilter(RedactTelegramIdFilter())

logger = logging.getLogger("multi-bot")

# ── Chain Registry ─────────────────────────────────
CHAINS: Dict[str, dict] = {
    "ethereum":  {"name": "Ethereum",   "emoji": "💠", "type": "evm", "rpc": "https://eth.llamarpc.com",                    "explorer": "https://etherscan.io/tx/",           "native": "ETH",   "poll": 15},
    "bsc":       {"name": "BSC",        "emoji": "🟡", "type": "evm", "rpc": "https://bsc-dataseed.binance.org/",           "explorer": "https://bscscan.com/tx/",            "native": "BNB",   "poll": 3},
    "polygon":   {"name": "Polygon",    "emoji": "🟣", "type": "evm", "rpc": "https://polygon.llamarpc.com",                "explorer": "https://polygonscan.com/tx/",        "native": "MATIC", "poll": 2},
    "arbitrum":  {"name": "Arbitrum",   "emoji": "🔵", "type": "evm", "rpc": "https://arb1.arbitrum.io/rpc",                "explorer": "https://arbiscan.io/tx/",            "native": "ETH",   "poll": 2},
    "optimism":  {"name": "Optimism",   "emoji": "🔴", "type": "evm", "rpc": "https://mainnet.optimism.io",                 "explorer": "https://optimistic.etherscan.io/tx/","native": "ETH",   "poll": 2},
    "base":      {"name": "Base",       "emoji": "🔷", "type": "evm", "rpc": "https://mainnet.base.org",                    "explorer": "https://basescan.org/tx/",           "native": "ETH",   "poll": 2},
    "avalanche": {"name": "Avalanche",  "emoji": "❄️", "type": "evm", "rpc": "https://api.avax.network/ext/bc/C/rpc",       "explorer": "https://snowtrace.io/tx/",           "native": "AVAX",  "poll": 2},
    "fantom":    {"name": "Fantom",     "emoji": "👻", "type": "evm", "rpc": "https://rpc.ftm.tools",                       "explorer": "https://ftmscan.com/tx/",            "native": "FTM",   "poll": 3},
    "zksync":    {"name": "zkSync",     "emoji": "⚡", "type": "evm", "rpc": "https://mainnet.era.zksync.io",               "explorer": "https://explorer.zksync.io/tx/",     "native": "ETH",   "poll": 5},
    "linea":     {"name": "Linea",      "emoji": "📐", "type": "evm", "rpc": "https://rpc.linea.build",                     "explorer": "https://lineascan.build/tx/",        "native": "ETH",   "poll": 5},
    "scroll":    {"name": "Scroll",     "emoji": "📜", "type": "evm", "rpc": "https://rpc.scroll.io",                       "explorer": "https://scrollscan.com/tx/",         "native": "ETH",   "poll": 5},
    "mantle":    {"name": "Mantle",     "emoji": "🧱", "type": "evm", "rpc": "https://rpc.mantle.xyz",                      "explorer": "https://mantlescan.xyz/tx/",         "native": "MNT",   "poll": 5},
    "gnosis":    {"name": "Gnosis",     "emoji": "🦉", "type": "evm", "rpc": "https://rpc.gnosischain.com",                 "explorer": "https://gnosisscan.io/tx/",          "native": "xDAI",  "poll": 5},
    "celo":      {"name": "Celo",       "emoji": "🌍", "type": "evm", "rpc": "https://forno.celo.org",                      "explorer": "https://celoscan.io/tx/",            "native": "CELO",  "poll": 5},
    "cronos":    {"name": "Cronos",     "emoji": "🦁", "type": "evm", "rpc": "https://evm.cronos.org",                      "explorer": "https://cronoscan.com/tx/",          "native": "CRO",   "poll": 5},
    "moonbeam":  {"name": "Moonbeam",   "emoji": "🌙", "type": "evm", "rpc": "https://rpc.api.moonbeam.network",            "explorer": "https://moonscan.io/tx/",            "native": "GLMR",  "poll": 5},

    "ton":  {"name": "TON",     "emoji": "💎", "type": "ton",  "api": "https://tonapi.io/v2",                      "explorer": "https://tonviewer.com/transaction/", "native": "TON",  "poll": 10, "decimals": 9},
    "xlm":  {"name": "Stellar", "emoji": "✨", "type": "xlm",  "api": "https://horizon.stellar.org",               "explorer": "https://stellar.expert/explorer/public/tx/", "native": "XLM",  "poll": 10, "decimals": 7},
    "sol":  {"name": "Solana",  "emoji": "🟣", "type": "sol",  "rpc": "https://api.mainnet-beta.solana.com",       "explorer": "https://solscan.io/tx/",             "native": "SOL",  "poll": 5,  "decimals": 9},
    "tron": {"name": "TRON",    "emoji": "🔴", "type": "tron", "api": "https://api.trongrid.io/v1",                "explorer": "https://tronscan.org/#/transaction/","native": "TRX",  "poll": 5,  "decimals": 6},
    "btc":  {"name": "Bitcoin", "emoji": "🟠", "type": "btc",  "api": "https://mempool.space/api",                 "explorer": "https://mempool.space/tx/",          "native": "BTC",  "poll": 30, "decimals": 8},
}

# Extra networks are registered after the core table so the registry remains
# easy to scan and future EVM networks can also be supplied through config.
CHAINS.update({
    "robinhood": {"name": "Robinhood Chain", "emoji": "RH", "type": "evm", "rpc": "https://rpc.mainnet.chain.robinhood.com", "explorer": "https://robinhoodchain.blockscout.com/tx/", "native": "ETH", "poll": 2, "chain_id": 4663},
    "merlin": {"name": "Merlin Chain", "emoji": "🔥", "type": "evm", "rpc": "https://rpc.merlinchain.io", "explorer": "https://scan.merlinchain.io/tx/", "native": "BTC", "poll": 3, "chain_id": 4200},
    "hyperevm": {"name": "HyperEVM", "emoji": "H", "type": "evm", "rpc": "https://rpc.hyperliquid.xyz/evm", "explorer": "https://hyperevmscan.io/tx/", "native": "HYPE", "poll": 2, "chain_id": 999},
    "sui": {"name": "Sui", "emoji": "SUI", "type": "sui", "rpc": "https://fullnode.mainnet.sui.io:443", "explorer": "https://suivision.xyz/txblock/", "native": "SUI", "poll": 5, "decimals": 9},
})

# Public RPCs can rate-limit or temporarily return 5xx responses. Keep more
# than one endpoint for the networks with commonly unstable public nodes.
CHAINS["polygon"]["rpc_fallbacks"] = [
    "https://polygon.drpc.org",
    "https://polygon.publicnode.com",
    "https://1rpc.io/matic",
]
CHAINS["cronos"]["rpc_fallbacks"] = [
    "https://cronos-evm-rpc.publicnode.com",
    "https://1rpc.io/cro",
]

# Additional public fallbacks for every EVM network. Operators can override
# or extend any list with the rpc_fallbacks field in EXTRA_EVM_CHAINS.
_publicnode_fallbacks = {
    "ethereum": ["https://ethereum.publicnode.com", "https://1rpc.io/eth", "https://rpc.flashbots.net"],
    "bsc": ["https://bsc.publicnode.com", "https://1rpc.io/bnb"],
    "arbitrum": ["https://arbitrum-one.publicnode.com", "https://1rpc.io/arb"],
    "optimism": ["https://optimism.publicnode.com", "https://1rpc.io/op"],
    "base": ["https://base.publicnode.com", "https://1rpc.io/base"],
    "avalanche": ["https://avalanche-c-chain.publicnode.com", "https://1rpc.io/avax"],
    "fantom": ["https://fantom.publicnode.com", "https://1rpc.io/ftm"],
    "zksync": ["https://zksync-era.publicnode.com", "https://1rpc.io/zksync"],
    "linea": ["https://linea-mainnet.publicnode.com", "https://1rpc.io/linea"],
    "scroll": ["https://scroll.publicnode.com", "https://1rpc.io/scroll"],
    "mantle": ["https://mantle.publicnode.com", "https://1rpc.io/mantle"],
    "gnosis": ["https://gnosis.publicnode.com", "https://1rpc.io/gnosis"],
    "celo": ["https://celo.publicnode.com", "https://1rpc.io/celo"],
    "moonbeam": ["https://moonbeam.publicnode.com", "https://1rpc.io/glmr"],
}
for _chain_key, _fallbacks in _publicnode_fallbacks.items():
    CHAINS[_chain_key].setdefault("rpc_fallbacks", []).extend(
        url for url in _fallbacks if url not in CHAINS[_chain_key].get("rpc_fallbacks", [])
    )

if EXTRA_EVM_CHAINS:
    import json
    try:
        for key, cfg in json.loads(EXTRA_EVM_CHAINS).items():
            if not isinstance(cfg, dict) or not all(cfg.get(k) for k in ("name", "rpc", "explorer", "native")):
                raise ValueError(f"invalid chain config for {key}")
            CHAINS[key.lower()] = {"emoji": "⛓️", "type": "evm", "poll": 5, **cfg}
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("EXTRA_EVM_CHAINS must be a JSON object of chain definitions") from exc

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
# DATABASE
# ═══════════════════════════════════════════════════

class Database:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = asyncio.Lock()
        self._init()

    def _init(self):
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY,
                username TEXT, first_name TEXT,
                timezone TEXT DEFAULT 'UTC',
                notifications_enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                address TEXT NOT NULL,
                chain TEXT DEFAULT 'evm',
                label TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS chain_state (
                chain TEXT PRIMARY KEY,
                last_state TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                chain TEXT NOT NULL,
                tx_hash TEXT NOT NULL,
                log_index INTEGER DEFAULT -1,
                memo TEXT,
                block_number INTEGER,
                asset TEXT,
                amount TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_addr ON addresses(chat_id, chain);
            CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(chat_id, created_at);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_notif_unique 
                ON notifications(chat_id, chain, tx_hash, log_index);
            """
        )
        # Migrate databases created before notification controls existed.
        try:
            cur.execute("ALTER TABLE users ADD COLUMN notifications_enabled INTEGER DEFAULT 1")
        except sqlite3.OperationalError:
            pass
        self.conn.commit()

    async def execute(self, sql: str, params: tuple = ()):
        async with self.lock:
            return await asyncio.to_thread(lambda: self._exec(sql, params))

    def _exec(self, sql, params):
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur

    async def fetchall(self, sql: str, params: tuple = ()):
        async with self.lock:
            return await asyncio.to_thread(lambda: self.conn.execute(sql, params).fetchall())

    async def fetchone(self, sql: str, params: tuple = ()):
        async with self.lock:
            return await asyncio.to_thread(lambda: self.conn.execute(sql, params).fetchone())


# ═══════════════════════════════════════════════════
# TOKEN CACHES
# ═══════════════════════════════════════════════════

class EVMTokenCache:
    def __init__(self):
        self._cache: Dict[str, dict] = {}

    async def get_info(self, w3: AsyncWeb3, token_address: str) -> dict:
        addr = token_address.lower()
        if addr in self._cache:
            return self._cache[addr]
        try:
            c = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
            name = await c.functions.name().call()
            symbol = await c.functions.symbol().call()
            decimals = await c.functions.decimals().call()
            info = {"name": name, "symbol": symbol, "decimals": decimals}
        except Exception as exc:
            logger.warning("Token meta fail %s: %s", token_address, exc)
            info = {"name": "Unknown Token", "symbol": "???", "decimals": 18}
        self._cache[addr] = info
        return info


class SPLTokenCache:
    def __init__(self):
        self._cache: Dict[str, dict] = {}
        self.client: Optional[httpx.AsyncClient] = None

    async def get_info(self, mint: str) -> dict:
        if mint in self._cache:
            return self._cache[mint]
        if not self.client:
            self.client = httpx.AsyncClient(timeout=10)
        try:
            resp = await self.client.get(f"https://tokens.jup.ag/token/{mint}")
            if resp.status_code == 200:
                data = resp.json()
                info = {
                    "name": data.get("name", "Unknown SPL"),
                    "symbol": data.get("symbol", f"SPL-{mint[:4]}"),
                    "decimals": data.get("decimals", 9),
                }
            else:
                info = {"name": "Unknown SPL", "symbol": f"SPL-{mint[:4]}", "decimals": 9}
        except Exception as exc:
            logger.warning("Jupiter API fail %s: %s", mint, exc)
            info = {"name": "Unknown SPL", "symbol": f"SPL-{mint[:4]}", "decimals": 9}
        self._cache[mint] = info
        return info


# ═══════════════════════════════════════════════════
# TIMEZONE HELPERS
# ═══════════════════════════════════════════════════

def get_user_time(db: Database, chat_id: int) -> datetime:
    row = db.conn.execute("SELECT timezone FROM users WHERE chat_id=?", (chat_id,)).fetchone()
    tz = row["timezone"] if row and row["timezone"] else "UTC"
    try:
        zone = ZoneInfo(tz)
    except Exception:
        zone = ZoneInfo("UTC")
    return datetime.now(zone)


def fmt_time(db: Database, chat_id: int, dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = get_user_time(db, chat_id)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


# ═══════════════════════════════════════════════════
# BASE MONITOR
# ═══════════════════════════════════════════════════

class BaseMonitor(ABC):
    def __init__(self, key: str, cfg: dict, db: Database, bot: Bot):
        self.key = key
        self.cfg = cfg
        self.db = db
        self.bot = bot
        self.running = False

    @abstractmethod
    async def start(self): ...

    async def stop(self):
        self.running = False

    async def _notify(self, chat_id: int, title: str, icon: str, amount_str: str,
                      asset: str, from_addr: str, to_addr: str, tx_hash: str,
                      memo: Optional[str] = None, extra: Optional[str] = None,
                      asset_raw: Optional[str] = None, amount_raw: Optional[str] = None,
                      wallet_address: Optional[str] = None, block: Optional[int] = None):
        preference = await self.db.fetchone(
            "SELECT notifications_enabled FROM users WHERE chat_id=?", (chat_id,)
        )
        if preference and preference["notifications_enabled"] == 0:
            return
        address_chain = "evm" if self.cfg.get("type") == "evm" else self.key
        label_row = await self.db.fetchone(
            "SELECT label FROM addresses WHERE chat_id=? AND chain=? AND LOWER(address)=?",
            (chat_id, address_chain, (wallet_address or to_addr).lower()),
        )
        label = label_row["label"] if label_row and label_row["label"] else "My Wallet"

        # Full values make alerts directly copyable for wallet and explorer checks.
        f_short = from_addr
        t_short = to_addr
        tx_short = tx_hash
        time_str = fmt_time(self.db, chat_id)

        text = (
            f"<b>{icon} {title}</b>\n\n"
            f"<b>{self.cfg['emoji']} {self.cfg['name']}</b>\n"
            f"<b>💰 Amount:</b> <code>{amount_str} {asset}</code>\n"
            f"<b>📤 From:</b> <code>{f_short}</code>\n"
            f"<b>📥 To:</b> <code>{t_short}</code> <i>({label})</i>\n"
        )
        if memo:
            text += f"<b>📝 Memo:</b> <code>{memo}</code>\n"
        if extra:
            text += f"{extra}\n"
        if block is not None:
            text += f"<b>📦 Block:</b> <code>{block}</code>\n"
        text += (
            f"<b>🔗 Tx:</b> <a href='{self.cfg['explorer']}{tx_hash}'>{tx_short}</a>\n"
            f"<b>⏰ Time:</b> <code>{time_str}</code>"
        )

        for attempt in range(1, 4):
            try:
                await self.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                return
            except Exception as exc:
                if attempt == 3:
                    logger.error("Notify failed to %s after 3 attempts: %s", chat_id, exc)
                else:
                    logger.warning("Notify attempt %d failed to %s: %s", attempt, chat_id, exc)
                    await asyncio.sleep(attempt * 1.5)

    async def _dedup(self, chat_id: int, tx_hash: str, log_index: int = -1) -> bool:
        row = await self.db.fetchone(
            "SELECT 1 FROM notifications WHERE chat_id=? AND chain=? AND tx_hash=? AND log_index=?",
            (chat_id, self.key, tx_hash, log_index),
        )
        return row is not None

    async def _mark(self, chat_id: int, tx_hash: str, log_index: int = -1,
                    memo: Optional[str] = None, block: Optional[int] = None,
                    asset: Optional[str] = None, amount: Optional[str] = None):
        await self.db.execute(
            "INSERT INTO notifications (chat_id,chain,tx_hash,log_index,memo,block_number,asset,amount) VALUES (?,?,?,?,?,?,?,?)",
            (chat_id, self.key, tx_hash, log_index, memo, block, asset, amount),
        )


# ═══════════════════════════════════════════════════
# EVM MONITOR
# ═══════════════════════════════════════════════════

class EVMMonitor(BaseMonitor):
    def __init__(self, key: str, cfg: dict, db: Database, bot: Bot, cache: EVMTokenCache):
        super().__init__(key, cfg, db, bot)
        self.cache = cache
        self.w3: Optional[AsyncWeb3] = None
        self.rpc_url: Optional[str] = None

    async def _connect(self) -> bool:
        endpoints = [self.cfg["rpc"], *self.cfg.get("rpc_fallbacks", [])]
        for endpoint in dict.fromkeys(endpoints):
            provider = AsyncHTTPProvider(endpoint)
            candidate = AsyncWeb3(provider)
            # Polygon, BSC, and several sidechains include consensus metadata
            # in extraData, which needs the POA compatibility middleware.
            candidate.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            try:
                if await candidate.is_connected():
                    self.w3 = candidate
                    self.rpc_url = endpoint
                    logger.info("▶️  EVM %s connected via %s", self.cfg["name"], endpoint)
                    return True
            except Exception as exc:
                logger.warning("%s RPC failed (%s): %s", self.cfg["name"], endpoint, exc)
        self.w3 = None
        self.rpc_url = None
        return False

    async def start(self):
        # web3.py 6/7 exposes the asynchronous transport as AsyncHTTPProvider.
        self.running = True
        logger.info("▶️  EVM %s started (poll %ss)", self.cfg["name"], self.cfg["poll"])
        while self.running:
            if self.w3 is None:
                if not await self._connect():
                    logger.error("❌ %s RPC unavailable (all endpoints failed); retrying in 30s", self.cfg["name"])
                    await asyncio.sleep(30)
                    continue
            try:
                await self._tick()
            except Exception as exc:
                logger.warning("%s tick failed on %s: %s; trying RPC failover", self.key, self.rpc_url, exc)
                self.w3 = None
                await asyncio.sleep(5)

    async def _tick(self):
        current = await self.w3.eth.block_number
        row = await self.db.fetchone("SELECT last_state FROM chain_state WHERE chain=?", (self.key,))
        last = int(row["last_state"]) if row and row["last_state"] else current - 1
        if current <= last:
            await asyncio.sleep(self.cfg["poll"])
            return
        from_block = last + 1
        to_block = min(current, from_block + 9)
        rows = await self.db.fetchall("SELECT DISTINCT address FROM addresses WHERE chain='evm'")
        if not rows:
            await self.db.execute("INSERT OR REPLACE INTO chain_state (chain,last_state) VALUES (?,?)", (self.key, str(to_block)))
            await asyncio.sleep(self.cfg["poll"])
            return
        addrs = [r["address"].lower() for r in rows]
        addr_set = set(addrs)
        await self._scan_erc20(from_block, to_block, addr_set)
        await self._scan_native(from_block, to_block, addr_set)
        await self.db.execute("INSERT OR REPLACE INTO chain_state (chain,last_state) VALUES (?,?)", (self.key, str(to_block)))
        await asyncio.sleep(self.cfg["poll"])

    async def _scan_erc20(self, f: int, t: int, addr_set: set):
        try:
            logs = await self.w3.eth.get_logs({"fromBlock": f, "toBlock": t, "topics": [TRANSFER_TOPIC]})
        except Exception as exc:
            # RPC providers commonly cap the number of logs per request.
            # Split the range and retry so busy chains remain monitorable.
            if t > f:
                middle = (f + t) // 2
                await self._scan_erc20(f, middle, addr_set)
                await self._scan_erc20(middle + 1, t, addr_set)
                return
            logger.warning("%s logs err: %s", self.key, exc)
            return
        for log in logs:
            if len(log["topics"]) < 3:
                continue
            to_raw = log["topics"][2].hex()
            to_addr = "0x" + to_raw[-40:]
            from_raw = log["topics"][1].hex()
            from_addr = "0x" + from_raw[-40:]
            from_watched = from_addr.lower() in addr_set
            to_watched = to_addr.lower() in addr_set
            if not from_watched and not to_watched:
                continue
            direction = "outgoing" if from_watched else "incoming"
            watched_addr = from_addr if from_watched else to_addr
            data = log["data"].hex() if isinstance(log["data"], bytes) else log["data"]
            amount = int(data, 16)
            token_addr = log["address"]
            tx_hash = log["transactionHash"].hex()
            li = log["logIndex"]
            block = log["blockNumber"]
            users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='evm' AND LOWER(address)=?", (watched_addr.lower(),))
            for u in users:
                cid = u["chat_id"]
                try:
                    if await self._dedup(cid, tx_hash, li):
                        continue
                    info = await self.cache.get_info(self.w3, token_addr)
                    amt = Decimal(amount) / Decimal(10 ** info["decimals"])
                    amt_str = f"{amt:,.6f}".rstrip("0").rstrip(".")
                    asset_name = f"{info['name']} ({info['symbol']})"
                    if direction == "outgoing":
                        asset_name = f"Sent {asset_name}"
                    alert_title = "Outgoing ERC-20 Token" if direction == "outgoing" else "Incoming ERC-20 Token"
                    alert_icon = "🟠" if direction == "outgoing" else "🟢"
                    await self._notify(cid, alert_title, alert_icon, amt_str, asset_name,
                                       from_addr, to_addr, tx_hash, block=block,
                                       wallet_address=watched_addr)
                    await self._mark(cid, tx_hash, li, block=block, asset=asset_name, amount=amt_str)
                except Exception as exc:
                    # A single bad notification must never block state advancement
                    # for the rest of the batch (that's what caused this bug).
                    logger.error("%s ERC-20 notify failed for tx %s (chat %s): %s", self.key, tx_hash, cid, exc)

    async def _scan_native(self, f: int, t: int, addr_set: set):
        for n in range(f, t + 1):
            try:
                block = await self.w3.eth.get_block(n, full_transactions=True)
            except Exception as exc:
                logger.warning("%s block %d err: %s", self.key, n, exc)
                continue
            for tx in block.transactions:
                to = tx.get("to")
                from_addr = tx.get("from")
                if not to or not from_addr or tx.get("value", 0) == 0:
                    continue
                from_watched = from_addr.lower() in addr_set
                to_watched = to.lower() in addr_set
                if not from_watched and not to_watched:
                    continue
                direction = "outgoing" if from_watched else "incoming"
                watched_addr = from_addr if from_watched else to
                tx_hash = tx["hash"].hex()
                users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='evm' AND LOWER(address)=?", (watched_addr.lower(),))
                for u in users:
                    cid = u["chat_id"]
                    try:
                        if await self._dedup(cid, tx_hash, -1):
                            continue
                        amt = Decimal(tx["value"]) / Decimal(10 ** 18)
                        amt_str = f"{amt:,.6f}".rstrip("0").rstrip(".")
                        if direction == "outgoing":
                            amt_str = f"- {amt_str}"
                        alert_title = "Outgoing Native Transfer" if direction == "outgoing" else "Incoming Native Transfer"
                        alert_icon = "🟠" if direction == "outgoing" else "⬇️"
                        await self._notify(cid, alert_title, alert_icon, amt_str, self.cfg["native"],
                                           from_addr, to, tx_hash, block=n,
                                           wallet_address=watched_addr)
                        await self._mark(cid, tx_hash, -1, block=n, asset=self.cfg["native"], amount=amt_str)
                    except Exception as exc:
                        # A single bad notification must never block state advancement
                        # for the rest of the batch (that's what caused this bug).
                        logger.error("%s native notify failed for tx %s (chat %s): %s", self.key, tx_hash, cid, exc)


# ═══════════════════════════════════════════════════
# TON MONITOR (Native + All Jettons)
# ═══════════════════════════════════════════════════

class TONMonitor(BaseMonitor):
    async def start(self):
        self.client = httpx.AsyncClient(timeout=30)
        self.running = True
        logger.info("▶️  TON started (poll %ss)", self.cfg["poll"])
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("TON tick: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self):
        rows = await self.db.fetchall("SELECT DISTINCT address FROM addresses WHERE chain='ton'")
        if not rows:
            await asyncio.sleep(self.cfg["poll"])
            return
        headers = {}
        if TON_API_KEY:
            headers["Authorization"] = f"Bearer {TON_API_KEY}"
        for r in rows:
            addr = r["address"]
            try:
                resp = await self.client.get(f"https://tonapi.io/v2/accounts/{addr}/events", params={"limit": 20}, headers=headers)
                data = resp.json()
            except Exception as exc:
                logger.warning("TonAPI err for %s: %s", addr, exc)
                continue
            if not data.get("events"):
                continue
            for event in data.get("events", []):
                event_id = event.get("event_id")
                if not event_id:
                    continue
                for action in event.get("actions", []):
                    act_type = action.get("type")
                    if act_type == "TonTransfer":
                        ton_tx = action.get("TonTransfer", {})
                        rcpt = ton_tx.get("recipient", {}).get("address", {}).get("address")
                        if rcpt != addr:
                            continue
                        amount = int(ton_tx.get("amount", 0))
                        if amount == 0:
                            continue
                        sender = ton_tx.get("sender", {}).get("address", {}).get("address", "?")
                        memo = ton_tx.get("comment")
                        users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='ton' AND address=?", (addr,))
                        for u in users:
                            cid = u["chat_id"]
                            if await self._dedup(cid, event_id):
                                continue
                            amt = Decimal(amount) / Decimal(10 ** self.cfg["decimals"])
                            amt_str = f"{amt:,.6f}".rstrip("0").rstrip(".")
                            await self._notify(cid, "Incoming TON Transfer", "⬇️", amt_str, self.cfg["native"],
                                               sender, addr, event_id, memo=memo)
                            await self._mark(cid, event_id, memo=memo, asset=self.cfg["native"], amount=amt_str)
                    elif act_type == "JettonTransfer":
                        jet_tx = action.get("JettonTransfer", {})
                        rcpt = jet_tx.get("recipient", {}).get("address", {}).get("address")
                        if rcpt != addr:
                            continue
                        amount_str = jet_tx.get("amount", "0")
                        if amount_str == "0" or not amount_str:
                            continue
                        jetton = jet_tx.get("jetton", {})
                        token_name = jetton.get("name", "Unknown Jetton")
                        token_symbol = jetton.get("symbol", "???")
                        decimals = jetton.get("decimals", 9)
                        sender = jet_tx.get("sender", {}).get("address", {}).get("address", "?")
                        memo = jet_tx.get("comment")
                        users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='ton' AND address=?", (addr,))
                        for u in users:
                            cid = u["chat_id"]
                            if await self._dedup(cid, event_id):
                                continue
                            amt = Decimal(amount_str) / Decimal(10 ** decimals)
                            amt_str = f"{amt:,.6f}".rstrip("0").rstrip(".")
                            asset_name = f"{token_name} ({token_symbol})"
                            await self._notify(cid, "Incoming Jetton Token", "🟢", amt_str, asset_name,
                                               sender, addr, event_id, memo=memo)
                            await self._mark(cid, event_id, memo=memo, asset=asset_name, amount=amt_str)
        await asyncio.sleep(self.cfg["poll"])


# ═══════════════════════════════════════════════════
# XLM MONITOR (Native + All Assets)
# ═══════════════════════════════════════════════════

class XLMMonitor(BaseMonitor):
    async def start(self):
        self.client = httpx.AsyncClient(timeout=30)
        self.running = True
        logger.info("▶️  XLM started (poll %ss)", self.cfg["poll"])
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("XLM tick: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self):
        rows = await self.db.fetchall("SELECT DISTINCT address FROM addresses WHERE chain='xlm'")
        if not rows:
            await asyncio.sleep(self.cfg["poll"])
            return
        for r in rows:
            addr = r["address"]
            cursor = None
            state_row = await self.db.fetchone("SELECT last_state FROM chain_state WHERE chain=?", (f"xlm_{addr}",))
            if state_row and state_row["last_state"]:
                cursor = state_row["last_state"]
            params = {"order": "desc", "limit": 10}
            if cursor:
                params["cursor"] = cursor
            try:
                resp = await self.client.get(f"{self.cfg['api']}/accounts/{addr}/payments", params=params)
                data = resp.json()
            except Exception as exc:
                logger.warning("XLM API err: %s", exc)
                continue
            records = data.get("_embedded", {}).get("records", [])
            if not records:
                continue
            new_cursor = records[0].get("paging_token")
            if new_cursor:
                await self.db.execute("INSERT OR REPLACE INTO chain_state (chain,last_state) VALUES (?,?)", (f"xlm_{addr}", new_cursor))
            for rec in records:
                if rec.get("to") != addr:
                    continue
                if rec.get("type") not in ("payment", "path_payment"):
                    continue
                tx_hash = rec.get("transaction_hash")
                if not tx_hash:
                    continue
                memo = None
                try:
                    tx_resp = await self.client.get(f"{self.cfg['api']}/transactions/{tx_hash}")
                    tx_data = tx_resp.json()
                    memo_type = tx_data.get("memo_type")
                    memo_val = tx_data.get("memo")
                    if memo_type and memo_val:
                        memo = f"{memo_type.upper()}: {memo_val}"
                except Exception:
                    pass
                asset_type = rec.get("asset_type")
                if asset_type == "native":
                    asset = "XLM"
                else:
                    code = rec.get("asset_code", "ASSET")
                    issuer = rec.get("asset_issuer", "")
                    issuer_short = f"{issuer[:4]}...{issuer[-4:]}" if issuer else ""
                    asset = f"{code}" if not issuer_short else f"{code} ({issuer_short})"
                users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='xlm' AND address=?", (addr,))
                for u in users:
                    cid = u["chat_id"]
                    if await self._dedup(cid, tx_hash):
                        continue
                    amt = Decimal(rec.get("amount", "0"))
                    amt_str = f"{amt:,.7f}".rstrip("0").rstrip(".")
                    title = "Incoming XLM Payment" if asset == "XLM" else "Incoming Stellar Asset"
                    icon = "⬇️" if asset == "XLM" else "🟢"
                    await self._notify(cid, title, icon, amt_str, asset,
                                       rec.get("from", "?"), addr, tx_hash, memo=memo)
                    await self._mark(cid, tx_hash, memo=memo, asset=asset, amount=amt_str)
        await asyncio.sleep(self.cfg["poll"])


# ═══════════════════════════════════════════════════
# SOL MONITOR (Native + All SPL)
# ═══════════════════════════════════════════════════

class SOLMonitor(BaseMonitor):
    def __init__(self, key: str, cfg: dict, db: Database, bot: Bot, spl_cache: SPLTokenCache):
        super().__init__(key, cfg, db, bot)
        self.spl_cache = spl_cache

    async def start(self):
        self.client = httpx.AsyncClient(timeout=30)
        self.running = True
        logger.info("▶️  SOL started (poll %ss)", self.cfg["poll"])
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("SOL tick: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self):
        rows = await self.db.fetchall("SELECT DISTINCT address FROM addresses WHERE chain='sol'")
        if not rows:
            await asyncio.sleep(self.cfg["poll"])
            return
        for r in rows:
            addr = r["address"]
            before = None
            state_row = await self.db.fetchone("SELECT last_state FROM chain_state WHERE chain=?", (f"sol_{addr}",))
            if state_row and state_row["last_state"]:
                before = state_row["last_state"]
            payload = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [addr, {"limit": 10, "before": before}]}
            try:
                resp = await self.client.post(self.cfg["rpc"], json=payload)
                sigs = resp.json().get("result", [])
            except Exception as exc:
                logger.warning("SOL RPC err: %s", exc)
                continue
            if not sigs:
                continue
            new_before = sigs[-1].get("signature")
            await self.db.execute("INSERT OR REPLACE INTO chain_state (chain,last_state) VALUES (?,?)", (f"sol_{addr}", new_before))
            for sig_info in sigs:
                sig = sig_info["signature"]
                tx_payload = {"jsonrpc": "2.0", "id": 1, "method": "getTransaction", "params": [sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}]}
                try:
                    tx_resp = await self.client.post(self.cfg["rpc"], json=tx_payload)
                    tx_data = tx_resp.json().get("result")
                except Exception:
                    continue
                if not tx_data or tx_data.get("meta", {}).get("err"):
                    continue
                meta = tx_data.get("meta", {})
                msg = tx_data.get("transaction", {}).get("message", {})
                account_keys = msg.get("accountKeys", [])
                if addr not in account_keys:
                    continue
                idx = account_keys.index(addr)
                memo = None
                for inst in msg.get("instructions", []):
                    pidx = inst.get("programIdIndex")
                    if pidx is not None and pidx < len(account_keys):
                        prog = account_keys[pidx]
                        if prog in ("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr", "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"):
                            data = inst.get("data")
                            if data:
                                try:
                                    memo = base64.b64decode(data).decode("utf-8", errors="ignore")
                                except Exception:
                                    memo = str(data)
                users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='sol' AND address=?", (addr,))
                pre = meta.get("preBalances", [])[idx] if idx < len(meta.get("preBalances", [])) else 0
                post = meta.get("postBalances", [])[idx] if idx < len(meta.get("postBalances", [])) else 0
                diff = post - pre
                if diff > 0:
                    for u in users:
                        cid = u["chat_id"]
                        if await self._dedup(cid, sig):
                            continue
                        amt = Decimal(diff) / Decimal(10 ** self.cfg["decimals"])
                        amt_str = f"{amt:,.9f}".rstrip("0").rstrip(".")
                        await self._notify(cid, "Incoming SOL Transfer", "⬇️", amt_str, self.cfg["native"],
                                           "Network", addr, sig, memo=memo)
                        await self._mark(cid, sig, memo=memo, asset=self.cfg["native"], amount=amt_str)
                pre_tokens = {tb["accountIndex"]: tb for tb in meta.get("preTokenBalances", []) if tb.get("owner") == addr}
                post_tokens = {tb["accountIndex"]: tb for tb in meta.get("postTokenBalances", []) if tb.get("owner") == addr}
                for tidx in post_tokens:
                    if tidx not in pre_tokens:
                        continue
                    pre_amt = Decimal(pre_tokens[tidx]["uiTokenAmount"]["amount"])
                    post_amt = Decimal(post_tokens[tidx]["uiTokenAmount"]["amount"])
                    td = post_amt - pre_amt
                    if td <= 0:
                        continue
                    mint = post_tokens[tidx]["mint"]
                    decimals = post_tokens[tidx]["uiTokenAmount"]["decimals"]
                    info = await self.spl_cache.get_info(mint)
                    sym = info["symbol"]
                    name = info["name"]
                    amt_str = f"{td / Decimal(10**decimals):,.6f}".rstrip("0").rstrip(".")
                    asset_name = f"{name} ({sym})"
                    for u in users:
                        cid = u["chat_id"]
                        if await self._dedup(cid, sig):
                            continue
                        await self._notify(cid, "Incoming SPL Token", "🟢", amt_str, asset_name,
                                           "Network", addr, sig, memo=memo)
                        await self._mark(cid, sig, memo=memo, asset=asset_name, amount=amt_str)
        await asyncio.sleep(self.cfg["poll"])


# ═══════════════════════════════════════════════════
# TRON MONITOR (Native + All TRC20)
# ═══════════════════════════════════════════════════

class TRONMonitor(BaseMonitor):
    async def start(self):
        self.client = httpx.AsyncClient(timeout=30, headers={"Accept": "application/json"})
        self.running = True
        logger.info("▶️  TRON started (poll %ss)", self.cfg["poll"])
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("TRON tick: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self):
        rows = await self.db.fetchall("SELECT DISTINCT address FROM addresses WHERE chain='tron'")
        if not rows:
            await asyncio.sleep(self.cfg["poll"])
            return
        for r in rows:
            addr = r["address"]
            try:
                resp = await self.client.get(f"{self.cfg['api']}/accounts/{addr}/transactions", params={"limit": 10, "order_by": "block_timestamp,desc"})
                data = resp.json()
            except Exception as exc:
                logger.warning("TRON API err: %s", exc)
                continue
            for tx in data.get("data", []):
                raw = tx.get("raw_data", {})
                contract = raw.get("contract", [{}])[0]
                param = contract.get("parameter", {}).get("value", {})
                to_addr = param.get("to_address")
                amount = param.get("amount", 0)
                tx_hash = tx.get("txID")
                if not to_addr or not tx_hash or to_addr != addr or amount == 0:
                    continue
                users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='tron' AND address=?", (addr,))
                for u in users:
                    cid = u["chat_id"]
                    if await self._dedup(cid, tx_hash):
                        continue
                    amt = Decimal(amount) / Decimal(10 ** self.cfg["decimals"])
                    amt_str = f"{amt:,.6f}".rstrip("0").rstrip(".")
                    from_addr = param.get("owner_address", "?")
                    await self._notify(cid, "Incoming TRX Transfer", "⬇️", amt_str, self.cfg["native"],
                                       from_addr, addr, tx_hash)
                    await self._mark(cid, tx_hash, asset=self.cfg["native"], amount=amt_str)
            try:
                resp = await self.client.get(f"{self.cfg['api']}/accounts/{addr}/transactions/trc20", params={"limit": 10, "order_by": "block_timestamp,desc"})
                data = resp.json()
            except Exception as exc:
                logger.warning("TRON TRC20 err: %s", exc)
                continue
            for tx in data.get("data", []):
                if tx.get("to") != addr:
                    continue
                tx_hash = tx.get("transaction_id")
                if not tx_hash:
                    continue
                amount = tx.get("value", "0")
                token = tx.get("token_info", {})
                symbol = token.get("symbol", "???")
                name = token.get("name", "Unknown")
                decimals = token.get("decimals", 6)
                users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='tron' AND address=?", (addr,))
                for u in users:
                    cid = u["chat_id"]
                    if await self._dedup(cid, tx_hash):
                        continue
                    amt = Decimal(amount) / Decimal(10 ** decimals)
                    amt_str = f"{amt:,.6f}".rstrip("0").rstrip(".")
                    from_addr = tx.get("from", "?")
                    asset_name = f"{name} ({symbol})"
                    await self._notify(cid, "Incoming TRC-20 Token", "🟢", amt_str, asset_name,
                                       from_addr, addr, tx_hash)
                    await self._mark(cid, tx_hash, asset=asset_name, amount=amt_str)
        await asyncio.sleep(self.cfg["poll"])


# ═══════════════════════════════════════════════════
# BTC MONITOR
# ═══════════════════════════════════════════════════

class BTCMonitor(BaseMonitor):
    async def start(self):
        self.client = httpx.AsyncClient(timeout=30)
        self.running = True
        logger.info("▶️  BTC started (poll %ss)", self.cfg["poll"])
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("BTC tick: %s", exc)
                await asyncio.sleep(5)

    async def _tick(self):
        rows = await self.db.fetchall("SELECT DISTINCT address FROM addresses WHERE chain='btc'")
        if not rows:
            await asyncio.sleep(self.cfg["poll"])
            return
        for r in rows:
            addr = r["address"]
            try:
                resp = await self.client.get(f"{self.cfg['api']}/address/{addr}/txs")
                txs = resp.json()
            except Exception as exc:
                logger.warning("BTC API err: %s", exc)
                continue
            for tx in txs:
                tx_hash = tx.get("txid")
                if not tx_hash:
                    continue
                received = 0
                for vout in tx.get("vout", []):
                    if addr in [vout.get("scriptpubkey_address"), vout.get("address")]:
                        received += vout.get("value", 0)
                if received == 0:
                    continue
                users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='btc' AND address=?", (addr,))
                for u in users:
                    cid = u["chat_id"]
                    if await self._dedup(cid, tx_hash):
                        continue
                    amt = Decimal(received) / Decimal(10 ** self.cfg["decimals"])
                    amt_str = f"{amt:,.8f}".rstrip("0").rstrip(".")
                    sender = "Unknown"
                    vins = tx.get("vin", [])
                    if vins:
                        sender = vins[0].get("prevout", {}).get("scriptpubkey_address", "Unknown")
                    await self._notify(cid, "Incoming BTC Transfer", "⬇️", amt_str, self.cfg["native"],
                                       sender, addr, tx_hash)
                    await self._mark(cid, tx_hash, asset=self.cfg["native"], amount=amt_str)
        await asyncio.sleep(self.cfg["poll"])


# ═══════════════════════════════════════════════════
# SUI MONITOR (native SUI + fungible Move coins)
# Sui is not EVM-compatible: query transaction blocks addressed to each
# wallet and use net-positive balance changes as incoming transfers.
class SUIMonitor(BaseMonitor):
    async def start(self):
        self.client = httpx.AsyncClient(timeout=30)
        self.running = True
        logger.info("Sui started (poll %ss)", self.cfg["poll"])
        while self.running:
            try:
                await self._tick()
            except Exception as exc:
                logger.exception("Sui tick: %s", exc)
                await asyncio.sleep(5)

    async def _rpc(self, method: str, params: list):
        response = await self.client.post(self.cfg["rpc"], json={
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
        })
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(payload["error"])
        return payload.get("result", {})

    async def _tick(self):
        rows = await self.db.fetchall("SELECT DISTINCT address FROM addresses WHERE chain='sui'")
        for row in rows:
            address = row["address"]
            result = await self._rpc("suix_queryTransactionBlocks", [{"ToAddress": address}, {
                "showInput": True, "showEffects": True, "showBalanceChanges": True,
            }, None, 50, True])
            for tx in result.get("data", []):
                await self._process_tx(tx, address)
        await asyncio.sleep(self.cfg["poll"])

    async def _process_tx(self, tx: dict, address: str):
        digest = tx.get("digest")
        if not digest:
            return
        changes = tx.get("balanceChanges") or []
        users = await self.db.fetchall("SELECT chat_id FROM addresses WHERE chain='sui' AND LOWER(address)=LOWER(?)", (address,))
        for index, change in enumerate(changes):
            owner = change.get("owner")
            owner_address = owner.get("AddressOwner") if isinstance(owner, dict) else owner
            if owner_address and owner_address.lower() != address.lower():
                continue
            raw_amount = int(change.get("amount", "0"))
            if raw_amount <= 0:
                continue
            coin_type = change.get("coinType", "0x2::sui::SUI")
            is_sui = coin_type.endswith("::sui::SUI")
            decimals = self.cfg["decimals"] if is_sui else 9
            symbol = self.cfg["native"] if is_sui else coin_type.rsplit("::", 1)[-1].upper()
            amount = Decimal(raw_amount) / Decimal(10 ** decimals)
            amount_str = f"{amount:,.9f}".rstrip("0").rstrip(".")
            sender = (tx.get("transaction", {}).get("data", {}) or {}).get("sender", "Unknown")
            for user in users:
                chat_id = user["chat_id"]
                log_index = index
                if await self._dedup(chat_id, digest, log_index):
                    continue
                await self._notify(chat_id, "Incoming Sui Transfer", "🟢", amount_str, symbol,
                                   sender, address, digest)
                await self._mark(chat_id, digest, log_index=log_index, asset=symbol, amount=amount_str)


# TELEGRAM HANDLERS
# ═══════════════════════════════════════════════════

class AntiSpamMiddleware(BaseMiddleware):
    """Rate-limit abusive users without restricting the configured admin."""
    def __init__(self, max_updates: int = 30, window_seconds: int = 60):
        self.max_updates = max_updates
        self.window_seconds = window_seconds
        self._events = defaultdict(deque)
        self._last_notice = {}

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        chat = getattr(event, "chat", None)
        user_id = getattr(user, "id", None)
        chat_id = getattr(chat, "id", None)
        if user_id is None or str(chat_id) == str(ADMIN_CHAT_ID):
            return await handler(event, data)

        now = time.monotonic()
        bucket = self._events[user_id]
        while bucket and now - bucket[0] > self.window_seconds:
            bucket.popleft()
        if len(bucket) >= self.max_updates:
            if now - self._last_notice.get(user_id, 0) > 30:
                self._last_notice[user_id] = now
                await event.answer("⏳ Too many requests. Please wait a moment and try again.")
            return
        bucket.append(now)
        return await handler(event, data)


router = Router()
router.message.middleware(AntiSpamMiddleware())
db: Database
evm_cache: EVMTokenCache
spl_cache: SPLTokenCache
active_monitors: Dict[str, BaseMonitor] = {}
app_started_at: Optional[datetime] = None
CHAIN_ALIASES = {
    "rh": "robinhood", "robinhoodchain": "robinhood",
    "hyper": "hyperevm", "hyper-evm": "hyperevm",
    "merlinchain": "merlin", "bitcoin": "btc",
    "solana": "sol", "stellar": "xlm",
}


def validate_address(chain: str, address: str) -> bool:
    if chain == "evm":
        return address.startswith("0x") and len(address) == 42
    elif chain == "ton":
        return (address.startswith(("EQ", "UQ", "0:")) and len(address) >= 40)
    elif chain == "xlm":
        return address.startswith("G") and len(address) == 56
    elif chain == "sol":
        return 32 <= len(address) <= 44 and not address.startswith("0x")
    elif chain == "tron":
        return (address.startswith("T") and len(address) == 34) or (address.startswith("41") and len(address) == 42)
    elif chain == "btc":
        return address.startswith(("bc1", "1", "3")) and 25 <= len(address) <= 62
    elif chain == "sui":
        return bool(re.fullmatch(r"0x[0-9a-fA-F]{1,64}", address))
    return False


TELEGRAM_MAX_LEN = 4096


def _chunk_text(text: str, limit: int = TELEGRAM_MAX_LEN) -> List[str]:
    """Split text into Telegram-safe chunks, breaking on blank-line boundaries
    so a single entry (one address, one history row) is never cut mid-way."""
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    current = ""
    for block in text.split("\n\n"):
        piece = block + "\n\n"
        if len(current) + len(piece) > limit:
            if current:
                chunks.append(current.rstrip("\n"))
                current = ""
            # A single block larger than the whole limit still has to split.
            while len(piece) > limit:
                chunks.append(piece[:limit])
                piece = piece[limit:]
        current += piece
    if current.strip():
        chunks.append(current.rstrip("\n"))
    return chunks


async def send_long(message: types.Message, text: str, **kwargs):
    """message.answer() that transparently splits output over Telegram's
    4096-character limit instead of raising and leaving the user with no
    response at all (see /history, /export, /list, /report)."""
    for chunk in _chunk_text(text):
        await message.answer(chunk, **kwargs)


async def send_long_callback(callback: types.CallbackQuery, text: str, reply_markup=None, **kwargs):
    """Same as send_long, but edits the callback's existing message for the
    first chunk (matching normal inline-menu behavior) and sends any
    overflow as follow-up messages."""
    chunks = _chunk_text(text)
    await callback.message.edit_text(chunks[0], reply_markup=reply_markup if len(chunks) == 1 else None, **kwargs)
    for chunk in chunks[1:]:
        is_last = chunk == chunks[-1]
        await callback.message.answer(chunk, reply_markup=reply_markup if is_last else None, **kwargs)


# ── /start ─────────────────────────────────────────

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    chat_id = message.chat.id
    user = message.from_user
    await db.execute("INSERT OR IGNORE INTO users (chat_id, username, first_name) VALUES (?,?,?)",
                     (chat_id, user.username, user.first_name))

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Address", callback_data="menu_add"),
         InlineKeyboardButton(text="📋 My List", callback_data="menu_list")],
        [InlineKeyboardButton(text="📜 History", callback_data="menu_history"),
         InlineKeyboardButton(text="⚙️ Settings", callback_data="menu_settings")],
        [InlineKeyboardButton(text="❓ Help", callback_data="menu_help")],
    ])

    evm_count = sum(1 for c in CHAINS.values() if c.get("type") == "evm")
    other = [f"{c['emoji']} {c['name']}" for k, c in CHAINS.items() if c.get("type") != "evm"]

    await message.answer(
        f"<b>👋 Welcome to telegram-multichain-monitor</b>\n\n"
        f"I watch your wallets across <b>{len(CHAINS)} chains</b> and alert you for <b>incoming and outgoing transfers</b>.\n\n"
        f"<b>⛓️ EVM:</b> <code>{evm_count} chains</code> (all ERC-20s)\n"
        f"<b>⛓️ Others:</b> {', '.join(other)}\n\n"
        f"<b>Quick start</b>\n"
        f"<code>/add evm 0xYourAddress Wallet</code>\n"
        f"<code>/status</code> — health and latest alert\n"
        f"<code>/help</code> — all commands\n\n"
        f"<i>Tap a button below to get started.</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


# ── Callback router ────────────────────────────────

@router.callback_query(F.data == "menu_add")
async def cb_menu_add(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "<b>➕ Add an Address</b>\n\n"
        "<code>/add &lt;chain&gt; &lt;address&gt; [label]</code>\n\n"
        "Examples:\n"
        "<code>/add evm 0x... MetaMask</code>\n"
        "<code>/add ton EQ... MainTON</code>\n"
        "<code>/add sol 7x... Phantom</code>\n"
        "<code>/add btc bc1... ColdStorage</code>",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


@router.callback_query(F.data == "menu_list")
async def cb_menu_list(callback: types.CallbackQuery):
    await callback.answer()
    chat_id = callback.message.chat.id
    rows = await db.fetchall(
        "SELECT chain, address, label FROM addresses WHERE chat_id=? ORDER BY chain, created_at",
        (chat_id,),
    )
    if not rows:
        await callback.message.edit_text(
            "📭 No addresses monitored.\nUse <code>/add</code> to start.",
            parse_mode=ParseMode.HTML,
        )
        return
    text = "<b>📋 Your Monitored Addresses</b>\n\n"
    current = ""
    for r in rows:
        if r["chain"] != current:
            current = r["chain"]
            cfg = CHAINS.get(current, {})
            emoji = "⛓️" if current == "evm" else cfg.get("emoji", "⬜")
            name = "All EVM Chains" if current == "evm" else cfg.get("name", current)
            text += f"\n{emoji} <b>{name}</b>\n"
        a = r["address"]
        lab = r["label"] or "No Label"
        short = a
        text += f"  ├ <code>{short}</code> — <i>{lab}</i>\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Back", callback_data="menu_back")]
    ])
    await send_long_callback(callback, text, reply_markup=kb, parse_mode=ParseMode.HTML)


@router.callback_query(F.data == "menu_history")
async def cb_menu_history(callback: types.CallbackQuery):
    await callback.answer()
    chat_id = callback.message.chat.id
    rows = await db.fetchall(
        "SELECT chain, tx_hash, asset, amount, memo, created_at FROM notifications "
        "WHERE chat_id=? ORDER BY created_at DESC LIMIT 10",
        (chat_id,),
    )
    if not rows:
        await callback.message.edit_text(
            "📭 No history yet.\nNotifications will appear here once you receive transfers.",
            parse_mode=ParseMode.HTML,
        )
        return
    text = "<b>📜 Recent Notification History</b>\n\n"
    for i, r in enumerate(rows, 1):
        cfg = CHAINS.get(r["chain"], {})
        emoji = cfg.get("emoji", "⬜")
        direction = "OUTGOING" if str(r["asset"] or "").startswith("Sent") or str(r["amount"] or "").startswith("-") else "INCOMING"
        text += f"<b>{direction}</b> — {cfg.get('name', r['chain'])}\n"
        tx_short = r["tx_hash"]
        dt = datetime.fromisoformat(r["created_at"])
        time_str = fmt_time(db, chat_id, dt)
        text += (
            f"{i}. {emoji} <code>{r['asset']}</code> | <code>{r['amount']}</code>\n"
            f"   └ <a href='{cfg.get('explorer', '')}{r['tx_hash']}'>{tx_short}</a> | {time_str}\n\n"
        )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ Clear History", callback_data="clear_history_confirm"),
         InlineKeyboardButton(text="🔙 Back", callback_data="menu_back")]
    ])
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)


@router.callback_query(F.data == "menu_settings")
async def cb_menu_settings(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    row = await db.fetchone("SELECT timezone FROM users WHERE chat_id=?", (chat_id,))
    tz = row["timezone"] if row and row["timezone"] else "UTC"
    addr_count = await db.fetchone("SELECT COUNT(*) as c FROM addresses WHERE chat_id=?", (chat_id,))
    notif_count = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=?", (chat_id,))
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌍 Change Timezone", callback_data="menu_timezone")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="menu_back")],
    ])
    await callback.message.edit_text(
        f"<b>⚙️ Your Settings</b>\n\n"
        f"<b>🌍 Timezone:</b> <code>{tz}</code>\n"
        f"<b>📬 Addresses:</b> <code>{addr_count['c']}</code>\n"
        f"<b>🔔 Notifications:</b> <code>{notif_count['c']}</code>\n\n"
        f"<i>Use /timezone to set your local time.</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )
    await callback.answer()


@router.callback_query(F.data == "menu_timezone")
async def cb_menu_timezone(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "<b>🌍 Set Timezone</b>\n\n"
        "Use the command:\n"
        "<code>/timezone Asia/Kolkata</code>\n"
        "<code>/timezone America/New_York</code>\n"
        "<code>/timezone Europe/London</code>\n\n"
        "Or type <code>/timezone</code> to see quick picks.",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


@router.callback_query(F.data == "menu_help")
async def cb_menu_help(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "<b>📖 Help</b>\n\n"
        "<b>/add</b> <code>&lt;chain&gt; &lt;address&gt; [label]</code>\n"
        "<b>/remove</b> <code>&lt;chain&gt; &lt;address&gt;</code>\n"
        "<b>/rename</b> <code>&lt;chain&gt; &lt;address&gt; &lt;label&gt;</code>\n"
        "<b>/list</b> [chain] — Show your addresses\n"
        "<b>/report</b> — Wallets with alert counts\n"
        "<b>/history</b> [limit] — Notification history\n"
        "<b>/clearhistory</b> — Clear history\n"
        "<b>/pause</b> — Pause notifications\n"
        "<b>/resume</b> — Resume notifications\n"
        "<b>/timezone</b> <code>&lt;zone&gt;</code> — Set timezone\n"
        "<b>/stats</b> — Your stats\n"
        "<b>/status</b> — Monitor health and active networks\n"
        "<b>/test</b> — Send a sample alert\n"
        "<b>/export</b> — Export addresses\n"
        "<b>/chains</b> — Supported chains\n"
        "<b>/help</b> — This message\n\n"
        "<i>EVM addresses monitor all 19 EVM chains at once.</i>",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


@router.callback_query(F.data == "menu_back")
async def cb_menu_back(callback: types.CallbackQuery):
    await callback.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Address", callback_data="menu_add"),
         InlineKeyboardButton(text="📋 My List", callback_data="menu_list")],
        [InlineKeyboardButton(text="📜 History", callback_data="menu_history"),
         InlineKeyboardButton(text="⚙️ Settings", callback_data="menu_settings")],
        [InlineKeyboardButton(text="❓ Help", callback_data="menu_help")],
    ])
    await callback.message.edit_text(
        "<b>👋 telegram-multichain-monitor</b>\n\n<i>Tap a button below:</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


@router.callback_query(F.data == "clear_history_confirm")
async def cb_clear_history(callback: types.CallbackQuery):
    chat_id = callback.message.chat.id
    await db.execute("DELETE FROM notifications WHERE chat_id=?", (chat_id,))
    await callback.message.edit_text("🗑️ <b>History cleared.</b>", parse_mode=ParseMode.HTML)
    await callback.answer("History cleared", show_alert=True)


# ── /add ───────────────────────────────────────────

@router.message(Command("add"))
async def cmd_add(message: types.Message):
    parts = message.text.split(maxsplit=3)
    if len(parts) < 3:
        await message.answer(
            "❌ <b>Usage:</b> <code>/add &lt;chain&gt; &lt;address&gt; [label]</code>\n\n"
            "<b>Examples:</b>\n"
            "<code>/add evm 0x71C7656EC7ab88b098defB751B7401B5f6d8976F MetaMask</code>\n"
            "<code>/add ton EQAbCdEfGhIjKlMnOpQrStUvWxYz1234567890abcdef MainTON</code>\n"
            "<code>/add xlm GABC123DEF456GHI789JKL012MNO345PQR678STU901VWX234YZ Exchange</code>\n"
            "<code>/add sol 7xabc...defg Phantom</code>\n"
            "<code>/add tron TAbCdEfGhIjKlMnOpQrStUvWxYz123456 TRXWallet</code>\n"
            "<code>/add btc bc1qabc...defg ColdStorage</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    chain_input = parts[1].lower().strip()
    chain_input = CHAIN_ALIASES.get(chain_input, chain_input)
    address = parts[2].strip()
    label = parts[3].strip() if len(parts) > 3 else None
    if label and len(label) > 80:
        await message.answer("❌ Label must be 80 characters or fewer.", parse_mode=ParseMode.HTML)
        return
    # Sanitize once at the boundary: an unescaped '<', '>' or '&' in a label
    # would break every future HTML-parsed message that includes it --
    # /list, /rename confirmations, wallet reports, and real transfer alerts.
    if label:
        label = html_lib.escape(label, quote=False)

    chain = "evm" if chain_input in CHAINS and CHAINS[chain_input].get("type") == "evm" else chain_input

    if chain not in CHAINS and chain != "evm":
        valid = "evm, " + ", ".join(k for k, c in CHAINS.items() if c.get("type") != "evm")
        await message.answer(f"❌ Unknown chain. Valid: <code>{valid}</code>", parse_mode=ParseMode.HTML)
        return

    if not validate_address(chain, address):
        await message.answer(f"❌ Invalid address format for <b>{CHAINS.get(chain, {}).get('name', chain)}</b>.", parse_mode=ParseMode.HTML)
        return

    chat_id = message.chat.id
    exists = await db.fetchone(
        "SELECT 1 FROM addresses WHERE chat_id=? AND chain=? AND LOWER(address)=?",
        (chat_id, chain, address.lower()),
    )
    if exists:
        await message.answer("⚠️ This address is already monitored.")
        return

    await db.execute(
        "INSERT INTO addresses (chat_id, address, chain, label) VALUES (?,?,?,?)",
        (chat_id, address, chain, label),
    )

    chain_name = "All EVM Chains" if chain == "evm" else CHAINS[chain]["name"]
    emoji = "⛓️" if chain == "evm" else CHAINS[chain]["emoji"]

    await message.answer(
        f"✅ <b>Address Added</b>\n\n"
        f"{emoji} <b>Chain:</b> {chain_name}\n"
        f"<b>📍 Address:</b> <code>{address}</code>\n"
        f"<b>🏷️ Label:</b> {label or 'N/A'}\n\n"
        f"You will now receive real-time alerts for <b>all tokens</b> on this chain.",
        parse_mode=ParseMode.HTML,
    )


# ── /rename ────────────────────────────────────────

@router.message(Command("rename"))
async def cmd_rename(message: types.Message):
    parts = message.text.split(maxsplit=3)
    if len(parts) < 4:
        await message.answer(
            "❌ Usage: <code>/rename &lt;chain&gt; &lt;address&gt; &lt;new label&gt;</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    chain_input, address, label = parts[1].lower().strip(), parts[2].strip(), parts[3].strip()
    chain = "evm" if chain_input in CHAINS and CHAINS[chain_input].get("type") == "evm" else chain_input
    if not label or len(label) > 80:
        await message.answer("❌ Label must contain 1–80 characters.", parse_mode=ParseMode.HTML)
        return
    # Sanitize once at the boundary -- see /add for why this matters.
    label = html_lib.escape(label, quote=False)
    cur = await db.execute(
        "UPDATE addresses SET label=? WHERE chat_id=? AND chain=? AND LOWER(address)=?",
        (label, message.chat.id, chain, address.lower()),
    )
    if cur.rowcount:
        await message.answer(f"✅ Label updated to <b>{label}</b>.", parse_mode=ParseMode.HTML)
    else:
        await message.answer("❌ Address not found. Check <code>/list</code>.", parse_mode=ParseMode.HTML)


@router.message(Command("test"))
async def cmd_test(message: types.Message):
    chat_id = message.chat.id
    sample_cfg = CHAINS["base"]
    # Mirrors BaseMonitor._notify()'s exact template, so this doubles as a
    # real check that HTML formatting, emoji, and links render correctly --
    # not just that *a* message can be sent.
    sample = (
        f"<b>⬇️ Incoming Native Transfer</b>\n\n"
        f"<b>{sample_cfg['emoji']} {sample_cfg['name']}</b>\n"
        f"<b>💰 Amount:</b> <code>0.05 ETH</code>\n"
        f"<b>📤 From:</b> <code>0x000000000000000000000000000000000000dEaD</code>\n"
        f"<b>📥 To:</b> <code>0xYourWalletAddressHere00000000000000</code> <i>(Sample)</i>\n"
        f"<b>📦 Block:</b> <code>0</code>\n"
        f"<b>🔗 Tx:</b> <code>0xsample000000000000000000000000000000000000000000000000000000</code>\n"
        f"<b>⏰ Time:</b> <code>{fmt_time(db, chat_id)}</code>"
    )
    await message.answer(
        "✅ <b>This is a sample alert</b> — it is NOT a real transfer.\n\n" + sample +
        "\n\n<i>If this rendered cleanly, Telegram delivery is working. "
        "Real alerts fire automatically when a monitored wallet moves funds — "
        "check /status to confirm the relevant chain is online and /list to "
        "confirm the address is actually being watched.</i>",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


@router.message(Command("remove"))
async def cmd_remove(message: types.Message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        chat_id = message.chat.id
        rows = await db.fetchall(
            "SELECT id, chain, address, label FROM addresses WHERE chat_id=? ORDER BY chain",
            (chat_id,),
        )
        if not rows:
            await message.answer("📭 No addresses to remove.", parse_mode=ParseMode.HTML)
            return
        buttons = []
        for r in rows:
            cfg = CHAINS.get(r["chain"], {})
            emoji = "⛓️" if r["chain"] == "evm" else cfg.get("emoji", "⬜")
            lab = r["label"] or "No Label"
            short = f"{r['address'][:8]}...{r['address'][-4:]}"
            buttons.append([InlineKeyboardButton(
                text=f"{emoji} {short} ({lab})",
                callback_data=f"remove_{r['id']}"
            )])
        buttons.append([InlineKeyboardButton(text="❌ Cancel", callback_data="menu_back")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await message.answer(
            "<b>🗑️ Tap an address to remove:</b>\n\n"
            "Or use: <code>/remove &lt;chain&gt; &lt;address&gt;</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return

    chain_input = parts[1].lower().strip()
    chain_input = CHAIN_ALIASES.get(chain_input, chain_input)
    address = parts[2].strip().lower()
    chain = "evm" if chain_input in CHAINS and CHAINS[chain_input].get("type") == "evm" else chain_input
    chat_id = message.chat.id
    cur = await db.execute(
        "DELETE FROM addresses WHERE chat_id=? AND chain=? AND LOWER(address)=?",
        (chat_id, chain, address),
    )
    if cur.rowcount:
        name = "All EVM" if chain == "evm" else CHAINS.get(chain, {}).get("name", chain)
        await message.answer(f"✅ Removed <code>{address}</code> from {name} monitoring.", parse_mode=ParseMode.HTML)
    else:
        await message.answer("❌ Address not found in your list.", parse_mode=ParseMode.HTML)


@router.callback_query(F.data.startswith("remove_"))
async def cb_remove_id(callback: types.CallbackQuery):
    addr_id = int(callback.data.split("_")[1])
    chat_id = callback.message.chat.id
    row = await db.fetchone("SELECT chain, address FROM addresses WHERE id=? AND chat_id=?", (addr_id, chat_id))
    if not row:
        await callback.answer("Already removed", show_alert=True)
        return
    await db.execute("DELETE FROM addresses WHERE id=? AND chat_id=?", (addr_id, chat_id))
    await callback.message.edit_text(
        f"✅ Removed <code>{row['address']}</code> from monitoring.",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer("Removed successfully")


# ── /list ──────────────────────────────────────────

@router.message(Command("list"))
async def cmd_list(message: types.Message):
    chat_id = message.chat.id
    parts = message.text.split()
    chain_filter = CHAIN_ALIASES.get(parts[1].lower(), parts[1].lower()) if len(parts) > 1 else None
    if chain_filter and chain_filter in CHAINS and CHAINS[chain_filter].get("type") == "evm":
        chain_filter = "evm"
    if chain_filter and chain_filter not in CHAINS:
        await message.answer("❌ Unknown chain. Use <code>/chains</code>.", parse_mode=ParseMode.HTML)
        return
    if chain_filter:
        rows = await db.fetchall(
            "SELECT chain, address, label FROM addresses WHERE chat_id=? AND chain=? ORDER BY chain, created_at",
            (chat_id, chain_filter),
        )
    else:
        rows = await db.fetchall(
            "SELECT chain, address, label FROM addresses WHERE chat_id=? ORDER BY chain, created_at",
            (chat_id,),
        )
    if not rows:
        await message.answer(
            "📭 No addresses monitored.\nUse <code>/add &lt;chain&gt; &lt;address&gt; [label]</code> to start.",
            parse_mode=ParseMode.HTML,
        )
        return
    text = "<b>📋 Your Monitored Addresses</b>\n\n"
    current = ""
    for r in rows:
        if r["chain"] != current:
            current = r["chain"]
            cfg = CHAINS.get(current, {})
            emoji = "⛓️" if current == "evm" else cfg.get("emoji", "⬜")
            name = "All EVM Chains" if current == "evm" else cfg.get("name", current)
            text += f"\n{emoji} <b>{name}</b>\n"
        a = r["address"]
        lab = r["label"] or "No Label"
        # Full address, not truncated: a "..." in a <code> block is not
        # copyable into a wallet or explorer, which defeats the point.
        text += f"  ├ <code>{a}</code> — <i>{lab}</i>\n"
    count = len(rows)
    text += f"\n<i>Total: {count} address(es) monitored</i>"
    await send_long(message, text, parse_mode=ParseMode.HTML)


# ── /history ───────────────────────────────────────

@router.message(Command("history"))
async def cmd_history(message: types.Message):
    chat_id = message.chat.id
    parts = message.text.split(maxsplit=1)
    limit = 10
    if len(parts) > 1:
        try:
            limit = max(1, min(int(parts[1]), 50))
        except ValueError:
            await message.answer(
                "❌ Usage: <code>/history [1-50]</code>",
                parse_mode=ParseMode.HTML,
            )
            return

    rows = await db.fetchall(
        "SELECT chain, tx_hash, asset, amount, memo, created_at FROM notifications "
        "WHERE chat_id=? ORDER BY created_at DESC LIMIT ?",
        (chat_id, limit),
    )
    if not rows:
        await message.answer(
            "📭 <b>No history yet.</b>\n\n"
            "Notifications will appear here once you receive transfers.\n"
            "Make sure you've added addresses with <code>/add</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    text = f"<b>📜 Last {len(rows)} Notifications</b>\n\n"
    for i, r in enumerate(rows, 1):
        cfg = CHAINS.get(r["chain"], {})
        emoji = cfg.get("emoji", "⬜")
        tx_short = r["tx_hash"]
        dt = datetime.fromisoformat(r["created_at"])
        time_str = fmt_time(db, chat_id, dt)
        text += (
            f"{i}. {emoji} <code>{r['asset']}</code> | <code>{r['amount']}</code>\n"
            f"   └ <a href='{cfg.get('explorer', '')}{r['tx_hash']}'>{tx_short}</a> | {time_str}\n\n"
        )
    await send_long(message, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


# ── /clearhistory ──────────────────────────────────

@router.message(Command("clearhistory"))
async def cmd_clearhistory(message: types.Message):
    chat_id = message.chat.id
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, clear all", callback_data="clear_history_confirm"),
         InlineKeyboardButton(text="❌ Cancel", callback_data="menu_settings")]
    ])
    await message.answer(
        "🗑️ <b>Clear History?</b>\n\n"
        "This will delete all your notification history.\n"
        "Your monitored addresses will NOT be affected.",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


# ── /timezone ──────────────────────────────────────

@router.message(Command("pause"))
async def cmd_pause(message: types.Message):
    await db.execute("UPDATE users SET notifications_enabled=0 WHERE chat_id=?", (message.chat.id,))
    await message.answer(
        "🔕 <b>Notifications paused.</b>\n\nYour addresses remain monitored. Use <code>/resume</code> to receive alerts again.",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("resume"))
async def cmd_resume(message: types.Message):
    await db.execute("UPDATE users SET notifications_enabled=1 WHERE chat_id=?", (message.chat.id,))
    await message.answer(
        "🔔 <b>Notifications resumed.</b>\n\nYou will receive alerts for your monitored addresses.",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("timezone"))
async def cmd_timezone(message: types.Message):
    chat_id = message.chat.id
    parts = message.text.split(maxsplit=1)

    if len(parts) == 1:
        row = await db.fetchone("SELECT timezone FROM users WHERE chat_id=?", (chat_id,))
        current = row["timezone"] if row and row["timezone"] else "UTC"
        quick = "\n".join(f"  <code>{tz}</code>" for tz in POPULAR_TZS)
        await message.answer(
            f"<b>🌍 Your Timezone</b>\n\n"
            f"Current: <code>{current}</code>\n\n"
            f"<b>Quick picks:</b>\n{quick}\n\n"
            f"Or set any IANA timezone:\n<code>/timezone Europe/Paris</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    tz_input = parts[1].strip()
    if tz_input not in available_timezones():
        await message.answer(
            f"❌ Invalid timezone: <code>{tz_input}</code>\n\n"
            f"Use an IANA timezone like <code>Asia/Kolkata</code> or <code>America/New_York</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    await db.execute("UPDATE users SET timezone=? WHERE chat_id=?", (tz_input, chat_id))
    sample = datetime.now(ZoneInfo(tz_input)).strftime("%Y-%m-%d %H:%M:%S %Z")
    await message.answer(
        f"✅ <b>Timezone Updated</b>\n\n"
        f"<b>New timezone:</b> <code>{tz_input}</code>\n"
        f"<b>Sample time:</b> <code>{sample}</code>\n\n"
        f"All future notifications will use this timezone.",
        parse_mode=ParseMode.HTML,
    )


# ── /stats ─────────────────────────────────────────

async def build_status_text(chat_id: int) -> str:
    preference = await db.fetchone("SELECT notifications_enabled FROM users WHERE chat_id=?", (chat_id,))
    address_row = await db.fetchone("SELECT COUNT(*) AS c FROM addresses WHERE chat_id=?", (chat_id,))
    notification_row = await db.fetchone(
        "SELECT COUNT(*) AS c FROM notifications WHERE chat_id=? AND created_at > datetime('now', '-1 day')",
        (chat_id,),
    )
    latest = await db.fetchone(
        "SELECT chain, asset, amount, created_at FROM notifications WHERE chat_id=? ORDER BY created_at DESC LIMIT 1",
        (chat_id,),
    )
    uptime = "not started"
    if app_started_at:
        seconds = max(0, int((datetime.now() - app_started_at).total_seconds()))
        uptime = f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    online_count = sum(
        1 for monitor in active_monitors.values()
        if monitor.running and (monitor.cfg.get("type") != "evm" or monitor.w3 is not None)
    )
    offline_count = max(0, len(active_monitors) - online_count)
    monitored = [
        (key, monitor.running)
        for key, monitor in active_monitors.items()
        if await db.fetchone("SELECT 1 FROM addresses WHERE chat_id=? AND chain=? LIMIT 1", (chat_id, key))
        or (monitor.cfg.get("type") == "evm" and await db.fetchone("SELECT 1 FROM addresses WHERE chat_id=? AND chain='evm' LIMIT 1", (chat_id,)))
    ]
    lines = [
        "<b>📊 Monitor Status</b>",
        "",
        f"<b>Addresses:</b> <code>{address_row['c']}</code>",
        f"<b>Alerts (24h):</b> <code>{notification_row['c']}</code>",
        f"<b>Notifications:</b> <code>{'ON' if not preference or preference['notifications_enabled'] else 'PAUSED'}</code>",
        f"<b>Networks configured:</b> <code>{len(CHAINS)}</code>",
        f"<b>Network health:</b> <code>{online_count} online</code> / <code>{offline_count} reconnecting</code>",
        f"<b>Bot uptime:</b> <code>{uptime}</code>",
        "",
    ]
    if latest:
        lines.append(f"<b>Latest alert:</b> <code>{latest['amount']} {latest['asset']}</code> ({latest['chain']})")
    if monitored:
        lines.append("<b>Your active networks:</b>")
        for key, running in monitored:
            cfg = CHAINS[key]
            monitor = active_monitors[key]
            connected = cfg.get("type") != "evm" or monitor.w3 is not None
            state = "🟢 polling" if running and connected else "🔴 reconnecting"
            lines.append(f"{cfg['emoji']} <b>{cfg['name']}</b>: {state}")
    else:
        lines.append("<i>No addresses are being monitored yet.</i>")
    return "\n".join(lines)


@router.message(Command("status"))
async def cmd_status(message: types.Message):
    await message.answer(await build_status_text(message.chat.id), parse_mode=ParseMode.HTML)


@router.message(Command("report", "wallets"))
async def cmd_wallet_report(message: types.Message):
    chat_id = message.chat.id
    rows = await db.fetchall(
        "SELECT chain, address, label, created_at FROM addresses WHERE chat_id=? ORDER BY chain, created_at",
        (chat_id,),
    )
    if not rows:
        await message.answer(
            "📭 <b>No wallets configured.</b>\nUse <code>/add &lt;chain&gt; &lt;address&gt; [label]</code> to begin.",
            parse_mode=ParseMode.HTML,
        )
        return

    text = "<b>👛 Wallet Report</b>\n\n"
    current_chain = None
    for row in rows:
        chain = row["chain"]
        if chain != current_chain:
            current_chain = chain
            cfg = CHAINS.get(chain, {})
            chain_count = await db.fetchone(
                "SELECT COUNT(*) AS c FROM addresses WHERE chat_id=? AND chain=?", (chat_id, chain)
            )
            text += f"<b>{cfg.get('emoji', '⛓️')} {cfg.get('name', chain)}</b> — {chain_count['c']} wallet(s)\n"
        alert_count = await db.fetchone(
            "SELECT COUNT(*) AS c FROM notifications WHERE chat_id=? AND chain=?",
            (chat_id, chain),
        )
        short = row["address"]
        text += f"  <code>{short}</code> — <i>{row['label'] or 'No label'}</i> — {alert_count['c']} alert(s)\n"
    text += "\n<i>Use /list for addresses or /rename to update labels.</i>"
    await send_long(message, text, parse_mode=ParseMode.HTML)


@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    chat_id = message.chat.id
    addr_count = await db.fetchone("SELECT COUNT(*) as c FROM addresses WHERE chat_id=?", (chat_id,))
    notif_count = await db.fetchone("SELECT COUNT(*) as c FROM notifications WHERE chat_id=?", (chat_id,))
    notif_24h = await db.fetchone(
        "SELECT COUNT(*) as c FROM notifications WHERE chat_id=? AND created_at > datetime('now', '-1 day')",
        (chat_id,),
    )
    row = await db.fetchone("SELECT timezone FROM users WHERE chat_id=?", (chat_id,))
    tz = row["timezone"] if row and row["timezone"] else "UTC"

    # Chain breakdown
    chain_rows = await db.fetchall(
        "SELECT chain, COUNT(*) as c FROM addresses WHERE chat_id=? GROUP BY chain",
        (chat_id,),
    )
    chain_breakdown = "\n".join(
        f"  {CHAINS.get(r['chain'], {}).get('emoji', '⬜')} {CHAINS.get(r['chain'], {}).get('name', r['chain'])}: <code>{r['c']}</code>"
        for r in chain_rows
    )

    await message.answer(
        f"<b>📊 Your Stats</b>\n\n"
        f"<b>📬 Total Addresses:</b> <code>{addr_count['c']}</code>\n"
        f"<b>🔔 Total Notifications:</b> <code>{notif_count['c']}</code>\n"
        f"<b>📈 Last 24h:</b> <code>{notif_24h['c']}</code>\n"
        f"<b>🌍 Timezone:</b> <code>{tz}</code>\n\n"
        f"<b>By Chain:</b>\n{chain_breakdown}",
        parse_mode=ParseMode.HTML,
    )


# ── /export ────────────────────────────────────────

@router.message(Command("export"))
async def cmd_export(message: types.Message):
    chat_id = message.chat.id
    rows = await db.fetchall(
        "SELECT chain, address, label FROM addresses WHERE chat_id=? ORDER BY chain, created_at",
        (chat_id,),
    )
    if not rows:
        await message.answer("📭 No addresses to export.", parse_mode=ParseMode.HTML)
        return

    lines = ["# telegram-multichain-monitor Export", f"# User: {chat_id}", f"# Exported: {fmt_time(db, chat_id)}", ""]
    for r in rows:
        cfg = CHAINS.get(r["chain"], {})
        name = "EVM" if r["chain"] == "evm" else cfg.get("name", r["chain"])
        lines.append(f"[{name}] {r['address']}  # {r['label'] or 'No Label'}")

    text = "\n".join(lines)
    header = "<b>📤 Export</b>\n\n"
    footer = "\n\n<i>Copy the text above to back up your addresses.</i>"
    wrapper_overhead = len(header) + len(footer) + len("<pre></pre>")
    # Chunk the raw lines first, then wrap each chunk in its own <pre> tags --
    # splitting an already-wrapped block would leave an unmatched <pre>/</pre>
    # in whichever message got cut, breaking HTML parsing for that message.
    line_chunks = _chunk_text(text, limit=TELEGRAM_MAX_LEN - wrapper_overhead)
    for i, chunk in enumerate(line_chunks):
        piece = f"<pre>{chunk}</pre>"
        if i == 0:
            piece = header + piece
        if i == len(line_chunks) - 1:
            piece = piece + footer
        await message.answer(piece, parse_mode=ParseMode.HTML)


# ── /chains ────────────────────────────────────────

@router.message(Command("chains"))
async def cmd_chains(message: types.Message):
    evm_chains = [(k, c) for k, c in CHAINS.items() if c.get("type") == "evm"]
    other_chains = [(k, c) for k, c in CHAINS.items() if c.get("type") != "evm"]

    text = "<b>⛓️ Supported Chains</b>\n\n"
    text += (
        f"<b>⛓️ EVM</b> — add once with <code>/add evm &lt;address&gt;</code>, "
        f"monitored across all {len(evm_chains)} networks below:\n"
    )
    text += ", ".join(f"{c['emoji']} {c['name']}" for _, c in evm_chains) + "\n"
    text += "<i>All ERC-20 tokens (USDT, USDC, etc.) detected automatically on each.</i>\n\n"

    text += "<b>Other networks</b> — add individually by key:\n"
    for k, c in other_chains:
        note = ""
        if k == "ton": note = " — <i>Native + All Jettons</i>"
        elif k == "xlm": note = " — <i>Native + All Assets</i>"
        elif k == "sol": note = " — <i>Native + All SPLs</i>"
        elif k == "tron": note = " — <i>Native + All TRC-20s</i>"
        elif k == "btc": note = " — <i>Native BTC only</i>"
        elif k == "sui": note = " — <i>Native + All Move Coins</i>"
        text += f"{c['emoji']} <b>{c['name']}</b> — <code>{k}</code> — <code>{c['native']}</code>{note}\n"
    await send_long(message, text, parse_mode=ParseMode.HTML)


# ── /help ──────────────────────────────────────────

@router.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(
        "<b>📖 Command Reference</b>\n\n"
        "<b>➕ /add</b> <code>&lt;chain&gt; &lt;address&gt; [label]</code>\n"
        "  └ Monitor a new address\n"
        "<b>🗑️ /remove</b> <code>&lt;chain&gt; &lt;address&gt;</code>\n"
        "  └ Stop monitoring (or tap to remove inline)\n"
        "<b>✏️ /rename</b> <code>&lt;chain&gt; &lt;address&gt; &lt;label&gt;</code>\n"
        "  └ Change an address's label\n"
        "<b>📋 /list</b> [chain] — Show monitored addresses\n"
        "<b>👛 /report</b> — Wallets with per-address alert counts\n"
        "<b>📜 /history</b> [limit] — Show notification history (max 50)\n"
        "<b>🗑️ /clearhistory</b> — Clear all history\n"
        "<b>🔕 /pause</b> — Pause notifications\n"
        "<b>🔔 /resume</b> — Resume notifications\n"
        "<b>🌍 /timezone</b> <code>&lt;zone&gt;</code> — Set local timezone\n"
        "<b>📊 /stats</b> — Your monitoring statistics\n"
        "<b>📊 /status</b> — Monitor health and active networks\n"
        "<b>🧪 /test</b> — Send a sample alert to check formatting/delivery\n"
        "<b>📤 /export</b> — Export addresses as text\n"
        "<b>⛓️ /chains</b> — List supported chains\n"
        "<b>❓ /help</b> — This message\n\n"
        "<i>💡 EVM addresses monitor all 19 EVM chains simultaneously.</i>\n"
        "<i>💡 All token types detected: ERC-20, Jettons, SPL, TRC-20, Stellar assets.</i>\n"
        "<i>💡 TON & XLM memos shown automatically.</i>\n"
        "<i>💡 Notifications use YOUR local timezone.</i>",
        parse_mode=ParseMode.HTML,
    )


# ── Global error handler ────────────────────────────
# Without this, any unhandled exception in a command or callback handler
# (bad input we didn't anticipate, a transient Telegram API error, a
# message that turned out too long, etc.) results in the user getting
# absolutely no response and no indication anything went wrong.
# `bot: Bot` is injected automatically by aiogram's dependency system,
# the same way it is for ordinary message/callback handlers.

@router.errors()
async def on_error(event: types.ErrorEvent, bot: Bot):
    update = event.update
    chat = None
    if update.message:
        chat = update.message.chat
    elif update.callback_query and update.callback_query.message:
        chat = update.callback_query.message.chat

    logger.error("Handler error on update %s: %s", update.update_id, event.exception, exc_info=event.exception)

    if chat is not None:
        try:
            await bot.send_message(
                chat.id,
                "⚠️ <b>Something went wrong processing that.</b>\n\n"
                "It's been logged. Please try again, and if it keeps happening, "
                "double-check your command with /help.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass  # Don't let a failure in the error handler itself raise.
    return True


# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════

def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Multi-Chain Telegram wallet monitor")
    parser.add_argument("--list-chains", action="store_true", help="List configured networks and exit")
    parser.add_argument("--check-rpcs", action="store_true", help="Test every configured RPC and exit")
    return parser


def cli_list_chains():
    for key, cfg in CHAINS.items():
        endpoints = [cfg["rpc"], *cfg.get("rpc_fallbacks", [])] if cfg.get("rpc") else []
        print(f"{key:<12} {cfg['name']:<20} {cfg['type']:<5} {len(endpoints)} RPC endpoint(s)")


async def cli_check_rpcs():
    async with httpx.AsyncClient(timeout=10) as client:
        for key, cfg in CHAINS.items():
            endpoints = [cfg["rpc"], *cfg.get("rpc_fallbacks", [])] if cfg.get("rpc") else []
            if not endpoints:
                print(f"{key:<12} SKIP (API monitor)")
                continue
            working = None
            for endpoint in dict.fromkeys(endpoints):
                try:
                    method = "sui_getLatestCheckpointSequenceNumber" if cfg.get("type") == "sui" else "eth_chainId"
                    response = await client.post(endpoint, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": []})
                    response.raise_for_status()
                    payload = response.json()
                    if "result" in payload:
                        working = endpoint
                        break
                except Exception:
                    continue
            if working:
                print(f"{key:<12} OK   {working}")
            else:
                print(f"{key:<12} FAIL (all RPC endpoints unavailable)")


async def main():
    global db, evm_cache, spl_cache, active_monitors, app_started_at
    app_started_at = datetime.now()

    if TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("\n❌ Set TELEGRAM_BOT_TOKEN environment variable!\n")
        return

    db = Database(DB_PATH)
    evm_cache = EVMTokenCache()
    spl_cache = SPLTokenCache()
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    # Register the Telegram command menu before contacting any blockchain RPC.
    # A slow or unavailable RPC must never delay the bot's command interface.
    monitors = []
    active_monitors = {}
    for k, c in CHAINS.items():
        if c.get("type") == "evm":
            monitor = EVMMonitor(k, c, db, bot, evm_cache)
            active_monitors[k] = monitor
            monitors.append(monitor.start())
        elif k == "ton":
            monitor = TONMonitor(k, c, db, bot)
            active_monitors[k] = monitor
            monitors.append(monitor.start())
        elif k == "xlm":
            monitor = XLMMonitor(k, c, db, bot)
            active_monitors[k] = monitor
            monitors.append(monitor.start())
        elif k == "sol":
            monitor = SOLMonitor(k, c, db, bot, spl_cache)
            active_monitors[k] = monitor
            monitors.append(monitor.start())
        elif k == "tron":
            monitor = TRONMonitor(k, c, db, bot)
            active_monitors[k] = monitor
            monitors.append(monitor.start())
        elif k == "btc":
            monitor = BTCMonitor(k, c, db, bot)
            active_monitors[k] = monitor
            monitors.append(monitor.start())
        elif k == "sui":
            monitor = SUIMonitor(k, c, db, bot)
            active_monitors[k] = monitor
            monitors.append(monitor.start())

    await bot.set_my_commands([
        BotCommand(command="start", description="Open the main menu"),
        BotCommand(command="status", description="Show monitor and network health"),
        BotCommand(command="add", description="Add a wallet address"),
        BotCommand(command="rename", description="Rename a wallet label"),
        BotCommand(command="remove", description="Remove a wallet address"),
        BotCommand(command="list", description="List monitored addresses"),
        BotCommand(command="report", description="Show detailed wallet report"),
        BotCommand(command="history", description="Show recent alerts"),
        BotCommand(command="clearhistory", description="Clear your alert history"),
        BotCommand(command="pause", description="Pause notifications"),
        BotCommand(command="resume", description="Resume notifications"),
        BotCommand(command="stats", description="Show monitoring statistics"),
        BotCommand(command="chains", description="List supported networks"),
        BotCommand(command="timezone", description="Set notification timezone"),
        BotCommand(command="export", description="Export monitored addresses"),
        BotCommand(command="help", description="Show command help"),
        BotCommand(command="test", description="Test Telegram notifications"),
    ])

    logger.info("🤖 Bot starting... %d chains | ALL tokens | History | Timezone", len(CHAINS))

    if ADMIN_CHAT_ID:
        try:
            await bot.send_message(
                ADMIN_CHAT_ID,
                f"🤖 <b>telegram-multichain-monitor is Online</b>\n\n"
                f"✅ <b>Networks:</b> {len(CHAINS)} configured\n"
                f"✅ <b>Monitoring:</b> native coins, ERC-20s, and supported chain tokens\n"
                f"✅ <b>Reliability:</b> RPC failover and automatic reconnection\n"
                f"✅ <b>Alerts:</b> incoming and outgoing EVM transfers\n\n"
                f"Use <code>/status</code> for live health or <code>/help</code> for commands.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    try:
        await asyncio.gather(dp.start_polling(bot), *monitors)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    args = build_cli_parser().parse_args()
    if args.list_chains:
        cli_list_chains()
    elif args.check_rpcs:
        asyncio.run(cli_check_rpcs())
    else:
        asyncio.run(main())
