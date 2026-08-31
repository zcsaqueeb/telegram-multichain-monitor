# telegram-multichain-monitor

## Multi-Chain Telegram Wallet Monitor

A resilient Telegram bot that watches wallet addresses and sends incoming-transfer alerts across EVM and non-EVM networks.

## Features

- Native-coin and token alerts across 19 EVM networks
- Incoming and outgoing EVM transfer tracking for native coins and ERC-20 tokens
- TON Jettons, Stellar assets, Solana SPL, TRON TRC-20, and Sui fungible Move coins
- Bitcoin native-transfer monitoring
- Automatic RPC failover, log-range splitting, and reconnection
- Per-user timezones, history, labels, export, statistics, and health status
- Pause/resume notifications without removing wallets
- Automatic Telegram command menu registration
- CLI tools for chain listing and RPC checks

## Requirements

- Python 3.10+
- Telegram bot token from [@BotFather](https://t.me/BotFather)
- Network access to Telegram and blockchain RPC/API endpoints

## Installation

```bash
git clone <your-repository-url>
cd multichain-monitor
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Configuration

Create `.env` beside `bot.py`:

```env
TELEGRAM_BOT_TOKEN=123456:replace_with_your_token
ADMIN_CHAT_ID=
TON_API_KEY=
DB_PATH=data/monitor.db
EXTRA_EVM_CHAINS=
```

The bot loads `.env` automatically. Operating-system environment variables take precedence. Never commit or share `.env`.

## Run

```bash
python bot.py
```

Windows example:

```bat
C:\Python314\python.exe C:\Users\saque\Desktop\multichain-monitor\bot.py
```

## Telegram commands

| Command | Description |
|---|---|
| `/start` | Open the main menu |
| `/status` | Health, uptime, notification state, and latest alert |
| `/add <chain> <address> [label]` | Start monitoring an address |
| `/rename <chain> <address> <label>` | Change an address label |
| `/remove <chain> <address>` | Stop monitoring an address |
| `/list [chain]` | List all addresses or filter by chain |
| `/report` or `/wallets` | Detailed wallet and alert report |
| `/history [limit]` | Show up to 50 recent alerts |
| `/clearhistory` | Delete your notification history |
| `/pause` | Pause your notifications |
| `/resume` | Resume your notifications |
| `/stats` | Show address and alert statistics |
| `/chains` | List supported networks |
| `/timezone [IANA zone]` | Set notification timezone |
| `/export` | Export monitored addresses |
| `/test` | Test Telegram delivery |
| `/help` | Show command help |

Examples:

```text
/add robinhood 0x1234... Trading Wallet
/add sui 0x1234... Sui Wallet
/list sui
/history 20
/pause
/resume
```

An EVM address monitors that wallet across all configured EVM chains. Use `/chains` for exact names.

## Supported networks

### EVM

Ethereum, BSC, Polygon, Arbitrum, Optimism, Base, Avalanche, Fantom, zkSync, Linea, Scroll, Mantle, Gnosis, Celo, Cronos, Moonbeam, Robinhood Chain, Merlin Chain, and HyperEVM.

### Other networks

TON, Stellar, Solana, TRON, Bitcoin, and Sui. Sui currently monitors SUI and fungible Move coins; NFT/object transfers are not included.

## RPC resilience

EVM networks use primary and fallback RPC endpoints. Timeout, 4xx/5xx, provider, and log-limit failures trigger retries, smaller log ranges, or endpoint rotation. A failed network does not stop the bot or other monitors.

Check connectivity without starting Telegram polling:

```bash
python bot.py --list-chains
python bot.py --check-rpcs
```

Additional EVM networks can be configured with `EXTRA_EVM_CHAINS`:

```env
EXTRA_EVM_CHAINS={"zora":{"name":"Zora","rpc":"https://rpc.zora.energy","explorer":"https://explorer.zora.energy/tx/","native":"ETH"}}
```

Public RPCs may be rate-limited. Use provider-backed endpoints for high-volume production deployments.

## Database and backups

SQLite is stored at `DB_PATH`, defaulting to `data/monitor.db`. It contains addresses, labels, settings, and notification history. Back up the database regularly, preferably while the bot is stopped.

## Production deployment

Use systemd, Docker, PM2, or Windows Task Scheduler with automatic restart, persistent database storage, and log rotation.

Example systemd service:

```ini
[Unit]
Description=Multi-Chain Telegram Wallet Monitor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/multichain-monitor
ExecStart=/opt/multichain-monitor/.venv/bin/python bot.py
Restart=always
RestartSec=10
EnvironmentFile=/opt/multichain-monitor/.env

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now multichain-monitor
sudo journalctl -u multichain-monitor -f
```

## Troubleshooting

**Token missing:** confirm the file is named `.env`, is beside `bot.py`, and contains exactly `TELEGRAM_BOT_TOKEN=...`.

**Network reconnecting:** run `python bot.py --check-rpcs`. Public providers may be temporarily unavailable; automatic retry will continue.

**No alerts:** run `/status`, check notifications are `ON`, verify `/list`, and run `/test`.

## Security

The bot only needs read access to blockchain APIs and never needs private keys. Do not store private keys in `.env`, source files, or the database.

MIT License.
