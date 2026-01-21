# 🤖 Extended Bot

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Status](https://img.shields.io/badge/status-active-success.svg)](https://github.com/Sviche/Extended-Trading-Bot)
[![Platform](https://img.shields.io/badge/platform-StarkNet-purple.svg)](https://starknet.io/)
[![Trading](https://img.shields.io/badge/trading-automated-orange.svg)](https://extended.exchange/)
[![Telegram Channel](https://img.shields.io/badge/Telegram-Channel-blue?logo=telegram)](https://t.me/sviche_crypto)
[![Telegram Chat](https://img.shields.io/badge/Telegram-Chat-blue?logo=telegram)](https://t.me/Sviche_Crypto_Chat)

> Professional trading bot for Extended Protocol (StarkNet) with automatic account onboarding and hedge strategies

**[🇷🇺 Русская версия](README_RU.md)**

---

## 🚀 Overview

**Extended Bot** is a powerful tool for automated trading on Extended Protocol (StarkNet), designed for efficient management of large numbers of accounts with minimal effort.

The bot uses batch account processing (batch trading) with automatic long/short position balancing to reduce risks and maximize profits.

---

## 📱 Join Our Community

[![Telegram Channel](https://img.shields.io/badge/JOIN-TELEGRAM_CHANNEL-blue?style=for-the-badge&logo=telegram)](https://t.me/sviche_crypto)
[![Telegram Chat](https://img.shields.io/badge/JOIN-TELEGRAM_CHAT-blue?style=for-the-badge&logo=telegram)](https://t.me/Sviche_Crypto_Chat)

---

## ✨ Key Features

- 🎯 **Hedge Strategy** — automatic balancing of longs and shorts (sum of longs = sum of shorts)
- ⚡ **Two Trading Modes** — LIMIT (saves 0.03% on fees) and MARKET (fast execution)
- 🔄 **Hybrid Architecture** — Pool + Queue + Workers for scaling to 100+ accounts
- 🛡️ **Automatic Onboarding** — account registration via SDK with a single command
- 📊 **Real-time Data** — WebSocket connections for instant prices (10ms updates)
- 🎮 **Graceful Shutdown** — safe termination on Ctrl+C with automatic position closing
- 💾 **SQLite Database** — secure storage of credentials and statistics
- 🔁 **Retry Logic** — automatic retries on failures (exponential backoff)

---

## 🛠️ Core Capabilities

### Trading Features
- ✅ Batch trading (5-7 accounts per batch)
- ✅ Limit orders with adaptive offset and retry logic (up to 5 attempts)
- ✅ Market orders via IOC for fast execution
- ✅ Automatic position management (TP/SL/time-based)
- ✅ Support for 52+ trading pairs on Extended Exchange

### Scalability
- ✅ Dynamic batch formation from account pool
- ✅ Load balancing between accounts (cooldown system)
- ✅ Parallel processing via Worker Pool (3-5 workers)
- ✅ Scaling to 100+ accounts

### Automation
- ✅ Automatic account registration via SDK (programmatic onboarding)
- ✅ Private key synchronization with DB on startup
- ✅ Automatic referral code application
- ✅ Automatic API key creation

---

## ⚙️ Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
# or use setup.bat on Windows
```

### 2. Configuration
Create configuration files:
- `user_data/private_keys.txt` — your Ethereum private keys
- `user_data/proxies.txt` — proxies for each account
- Edit `settings.py` if needed

### 3. Launch
```bash
python main.py
# or use start.bat on Windows
```

**That's it!** On first run, automatic onboarding of all accounts will occur, and the bot will start trading.

> **Note:** Full installation guide available in [INSTALL.md](INSTALL.md)

---

## 📖 Documentation

- 📦 [**Installation Guide**](INSTALL.md) — detailed installation instructions
- 💬 For questions and support, join our [Telegram Chat](https://t.me/Sviche_Crypto_Chat)

---

## 📋 Requirements

- Python 3.10 or higher
- Ethereum private keys (to create StarkNet accounts)
- HTTP/SOCKS5 proxies (one per account)
- Minimum 5-7 accounts to work in batch mode

---

## 🏗️ Architecture

```
Account Pool (100 accounts)
    ↓
Batch Generator (creates tasks every 5s)
    ↓
Task Queue (task buffer)
    ↓
Worker Pool (3-5 workers process in parallel)
    ↓
Accounts → Cooldown → Available
```

**Hybrid approach:** Pool + Queue + Workers for controlled load and scalability.

---

## 💡 Economics

### LIMIT Mode (default)
- Fee: **~0.02%** (Maker)
- Speed: ~100s per position
- Savings: **$30/day** at $100k/day volume

### MARKET Mode (fast farming)
- Fee: **~0.05%** (Taker)
- Speed: ~2s per position
- **50-100x faster**

---

## 🔒 Security

- ✅ Private keys stored locally (not in code)
- ✅ SQLite DB with credentials protected at OS level
- ✅ Proxies for request anonymization
- ✅ Graceful Shutdown for safe termination
- ✅ Automatic position closing on stop

---

## ⚠️ Disclaimer

This software is provided "as is" for educational purposes. Cryptocurrency trading involves risks. Use at your own risk.

---

## 📄 License

MIT License

---

<p align="center">Made with ❤️ for Extended Protocol community</p>
