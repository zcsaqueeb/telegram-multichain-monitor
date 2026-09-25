# Telegram Multichain Monitor

> Real-time Telegram alerts for wallet activity across 25 blockchain networks.

Monitor native assets and tokens, track incoming and outgoing transfers, and manage everything from Telegram. Wallets are detected automatically from their address format, or you can specify a chain explicitly.

## Highlights

- **25 supported networks** with EVM and non-EVM monitoring
- **Incoming and outgoing transfer alerts**
- **Automatic chain detection** when adding wallets
- **Native asset and token monitoring** with token metadata lookup
- **Burn/dead-address protection** to prevent monitoring unrecoverable addresses
- **Memo/comment display** on TON, Stellar, Solana, and TRON alerts
- **Fast asynchronous polling** with concurrent EVM scans and RPC failover
- **SQLite persistence** with WAL mode, batching, and optimized caching
- **Duplicate-alert prevention** and rate limiting
- **Per-user timezone and notification controls**
- **Configurable automatic message deletion**
- **Admin panel** with broadcast and monitor controls
- **Custom EVM chain support** through environment configuration

## Supported networks

### EVM networks

Ethereum, BNB Smart Chain, Polygon, Arbitrum, Optimism, Base, Avalanche, Fantom, zkSync, Linea, Scroll, Mantle, Gnosis, Celo, Cronos, Moonbeam, Robinhood Chain, Merlin Chain, and HyperEVM.

### Other networks

TON, Stellar, Solana, TRON, Bitcoin, and Sui.

## Requirements

- Python 3.10 or newer
- A Telegram bot token from [@BotFather](https://t.me/BotFather)
- Network access to the configured public RPC and API endpoints

## Installation

```bash
git clone https://github.com/zcsaqueeb/telegram-multichain-monitor.git
cd telegram-multichain-monitor
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
# .venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install -r requirements.txt
```

Create a `.env` file beside `bot.py`:

```env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token

# Optional settings
ADMIN_CHAT_ID=123456789
DB_PATH=data/monitor.db
TON_API_KEY=your_tonapi_key
AUTO_DELETE_DELAY=30
# TELEGRAM_REQUEST_TIMEOUT=60
# RPC_TIMEOUT=20           # per-request timeout for blockchain RPC calls
# EVM_MAX_BLOCKS=10        # max blocks scanned per EVM tick (catch-up cap)
# IDLE_POLL=30             # poll gap (s) for chains with no monitored wallets
```

Start the monitor:

```bash
python bot.py
```

The database is created automatically at `data/monitor.db` unless `DB_PATH` is changed.

## Telegram commands

| Command | Description |
| --- | --- |
| `/start` | Open the main menu |
| `/add <address> [label]` | Add a wallet with automatic chain detection |
| `/add <chain> <address> [label]` | Add a wallet with an explicit chain |
| `/remove <chain> <address>` | Remove a wallet |
| `/rename <chain> <address> <label>` | Rename a wallet |
| `/label <chain> <address> <label>` | Set or change a custom wallet label |
| `/list [chain]` | List monitored wallets |
| `/report` | Show alert counts by wallet |
| `/history [limit]` | View alert history, up to 50 records |
| `/clearhistory` | Clear alert history |
| `/pause` / `/resume` | Pause or resume notifications |
| `/timezone [zone]` | Set the display timezone |
| `/stats` | Show your usage statistics |
| `/status` | Check monitor health |
| `/ask` | Start a guided question-and-answer session with the built-in local assistant (no API) |
| `/test` | Send a sample alert |
| `/export` | Export monitored addresses |
| `/chains` | List supported networks |
| `/help` | Show command help |

Administrators can also use `/admin` and `/broadcast <message>` when `ADMIN_CHAT_ID` is configured.
Use `/admin_user <chat_id>` to view a user’s full profile, wallets, labels, timezone, notification state, and IN/OUT alert totals.
Admins can manage access with `/ban <chat_id>` and `/unban <chat_id>`. Anti-spam protection applies to regular users but is disabled for admins.

The built-in project assistant is available with `/ask`. The bot asks for your question, then replies in a formatted message. You can also use `/ask <question>` as a one-message shortcut. It analyzes the bot's local configuration and features, so it does not use an AI API or send the question to an external service.

## Adding wallets

Automatic detection examples:

```text
/add 0xYourEvmAddress Main wallet
/add EQYourTonAddress TON wallet
/add GYourStellarAddress Stellar wallet
/add YourSolanaAddress Solana wallet
/add TYourTronAddress TRON wallet
/add bc1qYourBitcoinAddress Bitcoin wallet
```

For ambiguous or custom EVM addresses, use the explicit form:

```text
/add ethereum 0xYourEvmAddress Main wallet
```

## Custom EVM chains

Additional EVM chains can be supplied as JSON in `EXTRA_EVM_CHAINS`:

```env
EXTRA_EVM_CHAINS={"mychain":{"name":"My Chain","native":"MYC","rpc":"https://rpc.example.com","explorer":"https://explorer.example.com/tx/","chain_id":12345,"poll":5}}
```

Each custom chain requires `name`, `native`, `rpc`, and `explorer`. `chain_id`, `poll`, `emoji`, and `rpc_fallbacks` are optional.

## Performance design

- Async HTTP and blockchain requests
- Address-filtered `eth_getLogs` queries (public RPCs reject chain-wide log scans, which previously kept chains like BSC in a reconnect loop)
- Concurrent raw JSON-RPC block fetches instead of sequential full-block formatting
- Per-tick TON/Solana memos, Stellar memo lookups batched and deduplicated per transaction
- Batched database writes and deduplication, plus a shared TTL cache for monitored-address lists
- Idle backoff: chains with no monitored wallets poll every 30s instead of every 2-5s
- SQLite WAL mode with a 64 MB cache
- TTL caches for user preferences, timezones, and token metadata
- Compiled address patterns and constant-time burn-address lookups
- Structured configuration and alert objects using dataclasses and type hints

## Security notes

- Keep `.env` private and never commit bot tokens or API keys.
- Use a restricted Telegram admin chat ID and verify it before enabling admin features.
- Public RPC and API endpoints may have rate limits; replace them with private endpoints if needed.
- A monitored wallet is read-only. This bot does not request private keys or sign transactions.

## License

Add the license that matches how you want to distribute this project.
