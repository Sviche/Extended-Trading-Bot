# 📦 Installation Guide

## System Requirements
- Python 3.10 or higher
- pip package manager
- Ethereum private keys
- Proxies (one per account)

## Step 1: Clone Repository
```bash
git clone https://github.com/Sviche/Extended-Trading-Bot.git
cd Extended-Trading-Bot
```

## Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

Or use the ready-made script:
```bash
setup.bat  # Windows
```

## Step 3: Configuration

### 3.1 Add Your Private Keys
Open the file `user_data/private_keys.txt` and add your Ethereum private keys (one per line):
```
0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890
```

**Note:** The file is already created in the repository, just add your keys.

### 3.2 Add Your Proxies
Open the file `user_data/proxies.txt` and add your proxies (one per line):
```
http://user:password@1.2.3.4:8080
socks5://user:password@5.6.7.8:1080
```

**Note:** The file is already created in the repository, just add your proxies.

### 3.3 Configure Trading Parameters (Optional)
Edit `settings.py` if you need to change:
- Trading markets (`markets`)
- Batch size (`batch_size_usd`)
- Order mode (`order_mode`: LIMIT or MARKET)
- Other parameters

## Step 4: Run
```bash
python main.py
```

Or use the ready-made script:
```bash
start.bat  # Windows
```

On first run, the following will happen automatically:
1. Automatic key synchronization with database
2. Automatic onboarding of new accounts on Extended Protocol
3. Programmatic API key creation
4. Referral code application
5. Trading starts

## Troubleshooting

### "Module not found" Error
Make sure all dependencies are installed:
```bash
pip install -r requirements.txt --upgrade
```

### "Invalid private key format" Error
Private keys must be in `0x...` format (66 characters). Check the `user_data/private_keys.txt` file.

### "Proxy connection failed" Error
Check the proxy format in `user_data/proxies.txt`. Make sure the proxies are working.

### Other Questions
Join our Telegram chat for support: https://t.me/Sviche_Crypto_Chat
