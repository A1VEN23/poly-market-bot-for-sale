# Polymarket Signal Bot - Deployment Guide

## Prerequisites

- Ubuntu 20.04+ server (Oracle Cloud Free Tier, AWS Free Tier, or any VPS)
- Python 3.10+ installed
- Git (optional)

## Step 1: Server Setup

### Update System
```bash
sudo apt update && sudo apt upgrade -y
```

### Install Python and Dependencies
```bash
sudo apt install -y python3 python3-pip python3-venv git
```

### Create Project Directory
```bash
mkdir -p ~/polymarket-bot
cd ~/polymarket-bot
```

## Step 2: Application Setup

### Upload/Clone Code
If using Git:
```bash
git clone <your-repo-url> .
```

Or upload files via SCP/WinSCP to `~/polymarket-bot/`

### Create Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## Step 3: Configuration

### Create Environment File
```bash
cp .env.example .env
nano .env
```

Edit `.env` with your actual values:
```
BOT_TOKEN=your_telegram_bot_token_here
GEMINI_API_KEY=your_gemini_api_key_here
POLYGON_RPC_URL=https://polygon-rpc.com
ADMIN_IDS=your_telegram_id_here
DATABASE_PATH=polymarket_bot.db
```

### Get Required API Keys

**Telegram Bot Token:**
1. Message [@BotFather](https://t.me/botfather)
2. Send `/newbot`
3. Follow instructions, save the token

**Google Gemini API Key (Free):**
1. Go to [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Create API key
3. Copy to `.env`

**Your Telegram ID:**
1. Message [@userinfobot](https://t.me/userinfobot)
2. Copy your ID to `ADMIN_IDS`

## Step 4: Testing

### Run Bot Manually
```bash
source venv/bin/activate
python main.py
```

You should see:
```
==================================================
Polymarket Signal Bot Starting...
==================================================
Bot started and database initialized!
Starting Telegram polling...
```

Press `Ctrl+C` to stop.

### Test Commands in Telegram
1. Start bot with `/start`
2. Try menu buttons
3. Test admin command: `/stats` (if you're in ADMIN_IDS)

## Step 5: Production Deployment

### Create Systemd Service
```bash
sudo nano /etc/systemd/system/polymarket-bot.service
```

Paste this content:
```ini
[Unit]
Description=Polymarket Signal Bot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/polymarket-bot
Environment="PATH=/home/ubuntu/polymarket-bot/venv/bin"
ExecStart=/home/ubuntu/polymarket-bot/venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Note:** Replace `ubuntu` with your actual username if different.

### Enable and Start Service
```bash
sudo systemctl daemon-reload
sudo systemctl enable polymarket-bot
sudo systemctl start polymarket-bot
```

### Check Status
```bash
sudo systemctl status polymarket-bot
```

### View Logs
```bash
# Real-time logs
sudo journalctl -u polymarket-bot -f

# Last 100 lines
sudo journalctl -u polymarket-bot -n 100
```

## Step 6: Maintenance

### Update Bot
```bash
cd ~/polymarket-bot
source venv/bin/activate
git pull  # if using git
pip install -r requirements.txt
sudo systemctl restart polymarket-bot
```

### Backup Database
```bash
cp ~/polymarket-bot/polymarket_bot.db ~/polymarket_bot_backup_$(date +%Y%m%d).db
```

### Monitor Resources
```bash
# Check memory usage
free -h

# Check CPU
htop

# Check disk space
df -h
```

## Free VPS Options

### Oracle Cloud Free Tier
- Always Free: 2 AMD instances + 4 ARM instances
- Sign up: [cloud.oracle.com](https://cloud.oracle.com)

### AWS Free Tier
- 12 months: t2.micro (1 vCPU, 1GB RAM)
- Sign up: [aws.amazon.com/free](https://aws.amazon.com/free)

### Google Cloud Free Tier
- $300 credit + always free e2-micro
- Sign up: [cloud.google.com/free](https://cloud.google.com/free)

## Security Recommendations

1. **Firewall**: Only open port 22 (SSH) if needed
2. **SSH Keys**: Use key-based authentication
3. **Regular Updates**: `sudo apt update && sudo apt upgrade -y`
4. **Fail2ban**: Install to prevent brute force
   ```bash
   sudo apt install fail2ban
   ```

## Troubleshooting

### Bot Not Starting
```bash
# Check Python version
python3 --version  # Should be 3.10+

# Check dependencies
pip list | grep aiogram

# Check .env file
cat .env | grep BOT_TOKEN
```

### Database Errors
```bash
# Check permissions
ls -la polymarket_bot.db

# Recreate database (WARNING: loses data)
rm polymarket_bot.db
sudo systemctl restart polymarket-bot
```

### API Rate Limits
If you hit rate limits:
- Increase check intervals in `config.py`
- Use multiple RPC endpoints (rotate them)
- Reduce number of markets scanned

## Admin Commands

| Command | Description |
|---------|-------------|
| `/addvip [user_id] [days]` | Grant VIP access |
| `/stats` | View bot statistics |

## Support

For issues:
1. Check logs: `sudo journalctl -u polymarket-bot -f`
2. Verify API keys are valid
3. Ensure enough disk space: `df -h`

---

**Bot deployed successfully! 🚀**
