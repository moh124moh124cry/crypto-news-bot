import os
import re
import sqlite3
import asyncio
import threading
from datetime import datetime, timezone

import feedparser
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.constants import ParseMode
from telegram.ext import Application

# ---------------- الإعدادات ----------------
TOKEN = os.environ["BOT_TOKEN"]
CHANNEL_ID = os.environ.get("CHANNEL_ID", "")      # اختياري: "@my_crypto_news"
WEBAPP_URL = os.environ.get("WEBAPP_URL", "")      # رابط الاستضافة العام + "/app"
INTERVAL_MINUTES = int(os.environ.get("INTERVAL_MINUTES", "10"))
DB_PATH = os.environ.get("DB_PATH", "news.db")

# مصادر RSS موثوقة للعملات المشفرة
FEEDS = [
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cryptopotato.com/feed/",
    "https://bitcoinmagazine.com/feed",
    "https://decrypt.co/feed",
    "https://ambcrypto.com/feed/",
    "https://u.today/rss",
    "https://news.bitcoin.com/feed/",
]

# كلمات التصنيف
MINING_WORDS = [
    "mining", "miner", "miners", "hashrate", "asic", "mining bot", "tap to earn",
    "tap-to-earn", "telegram mining", "mining app", "cloud mining", "airdrop",
    "testnet mining", "proof of work", "pow",
]
TRADING_WORDS = [
    "trading", "trader", "futures", "spot", "leverage", "liquidation", "exchange",
    "binance", "bybit", "okx", "price analysis", "technical analysis", "etf",
    "bull", "bear", "rally", "dump", "pump", "market",
]
GENERAL_WORDS = [
    "bitcoin", "btc", "ethereum", "eth", "crypto", "altcoin", "solana", "ton",
    "blockchain", "stablecoin", "defi", "token", "sec", "regulation",
]
# استثناءات (ضوضاء)
BLOCK_WORDS = ["sponsored", "press release", "casino", "gambling", "betting"]

def classify(text: str):
    """يُعيد التصنيف أو None إذا كان الخبر غير مطابق."""
    t = text.lower()
    if any(w in t for w in BLOCK_WORDS):
        return None
    if any(w in t for w in MINING_WORDS):
        return "⛏️ تعدين / بوتات تعدين"
    if any(w in t for w in TRADING_WORDS):
        return "📈 تداول وأسواق"
    if any(w in t for w in GENERAL_WORDS):
        return "🪙 أخبار عامة"
    return None

# ---------------- قاعدة البيانات ----------------
def db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("""CREATE TABLE IF NOT EXISTS news(
        link TEXT PRIMARY KEY, title TEXT, source TEXT,
        category TEXT, created TEXT)""")
    con.execute("CREATE TABLE IF NOT EXISTS subs(chat_id INTEGER PRIMARY KEY)")
    con.commit()
    return con

CON = db()

def save_news(link, title, source, category):
    try:
        CON.execute(
            "INSERT INTO news VALUES (?,?,?,?,?)",
            (link, title, source, category, datetime.now(timezone.utc).isoformat()),
        )
        CON.commit()
        return True          # خبر جديد
    except sqlite3.IntegrityError:
        return False         # مكرر

def latest(limit=40, category=None):
    q = "SELECT title, link, source, category, created FROM news"
    p = []
    if category:
        q += " WHERE category = ?"
        p.append(category)
    q += " ORDER BY created DESC LIMIT ?"
    p.append(limit)
    return [
        {"title": r[0], "link": r[1], "source": r[2], "category": r[3], "date": r[4]}
        for r in CON.execute(q, p).fetchall()
    ]

# ---------------- جمع الأخبار ----------------
def collect():
    fresh = []
    for url in FEEDS:
        try:
            feed = feedparser.parse(url)
            source = feed.feed.get("title", url)
            for e in feed.entries[:25]:
                title = re.sub(r"<[^>]+>", "", e.get("title", "")).strip()
                link = e.get("link", "")
                summary = re.sub(r"<[^>]+>", "", e.get("summary", ""))[:300]
                if not title or not link:
                    continue
                cat = classify(f"{title} {summary}")
                if not cat:
                    continue
                if save_news(link, title, source, cat):
                    fresh.append({"title": title, "link": link,
                                  "source": source, "category": cat})
        except Exception as err:
            print("feed error:", url, err)
    return fresh

async def job_push(context: ContextTypes.DEFAULT_TYPE):
    """يعمل تلقائيًا كل INTERVAL_MINUTES دقيقة."""
    fresh = await asyncio.to_thread(collect)
    if not fresh:
        return
    targets = [r[0] for r in CON.execute("SELECT chat_id FROM subs").fetchall()]
    if CHANNEL_ID:
        targets.append(CHANNEL_ID)

    for item in fresh[:8]:                      # حد أقصى لتجنّب الإزعاج
        msg = (f"{item['category']}\n\n<b>{item['title']}</b>\n"
               f"<i>{item['source']}</i>\n{item['link']}")
        for chat in targets:
            try:
                await context.bot.send_message(
                    chat, msg, parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False)
            except Exception as err:
                print("send error:", chat, err)
            await asyncio.sleep(0.4)

# ---------------- أوامر البوت ----------------
def kb():
    rows = []
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(
            "📱 افتح التطبيق", web_app=WebAppInfo(url=WEBAPP_URL))])
    rows.append([InlineKeyboardButton("⛏️ تعدين", callback_data="m"),
                 InlineKeyboardButton("📈 تداول", callback_data="t")])
    return InlineKeyboardMarkup(rows)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    CON.execute("INSERT OR IGNORE INTO subs VALUES (?)",
                (update.effective_chat.id,))
    CON.commit()
    await update.message.reply_text(
        "✅ تم تشغيل الاشتراك التلقائي.\n\n"
        f"سأرسل لك أخبار العملات المشفرة (تعدين + تداول) كل {INTERVAL_MINUTES} دقيقة.\n\n"
        "الأوامر:\n/mining أخبار التعدين\n/trading أخبار التداول\n"
        "/latest آخر الأخبار\n/stop إيقاف الإرسال",
        reply_markup=kb())

async def send_list(update: Update, category=None, title="آخر الأخبار"):
    items = latest(10, category)
    if not items:
        await update.message.reply_text("لا توجد أخبار بعد، انتظر الدورة القادمة.")
        return
    body = "\n\n".join(
        f"<b>{i['title']}</b>\n{i['link']}" for i in items)
    await update.message.reply_text(f"<b>{title}</b>\n\n{body}",
                                    parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True)

async def mining(u, c):  await send_list(u, "⛏️ تعدين / بوتات تعدين", "⛏️ أخبار التعدين")
async def trading(u, c): await send_list(u, "📈 تداول وأسواق", "📈 أخبار التداول")
async def latest_cmd(u, c): await send_list(u, None, "🗞️ آخر الأخبار")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    CON.execute("DELETE FROM subs WHERE chat_id = ?", (update.effective_chat.id,))
    CON.commit()
    await update.message.reply_text("⏹️ تم إيقاف الإرسال. أرسل /start للعودة.")

# ---------------- خادم الـ Mini App ----------------
api = FastAPI()

@api.get("/api/news")
def api_news(category: str = ""):
    return JSONResponse(latest(50, category or None))

@api.get("/app", response_class=HTMLResponse)
def app_page():
    with open("webapp.html", encoding="utf-8") as f:
        return f.read()

@api.get("/")
def health():
    return {"status": "ok"}

def run_api():
    uvicorn.run(api, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

# ---------------- الإقلاع ----------------
def main():
    threading.Thread(target=run_api, daemon=True).start()

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("mining", mining))
    app.add_handler(CommandHandler("trading", trading))
    app.add_handler(CommandHandler("latest", latest_cmd))
    app.add_handler(CommandHandler("stop", stop))

    app.job_queue.run_repeating(job_push, interval=INTERVAL_MINUTES * 60, first=15)
    app.run_polling()

if __name__ == "__main__":
    main()
