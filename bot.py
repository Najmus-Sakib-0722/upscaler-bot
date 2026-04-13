import os
import json
import time
import logging
import requests
from flask import Flask, request, jsonify
from kaggle_trigger import trigger_kaggle_job

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


def send_message(chat_id, text, parse_mode="Markdown"):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload)
    data = r.json()
    return data.get("result", {}).get("message_id")


def edit_message(chat_id, message_id, text, parse_mode="Markdown"):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode,
    }
    requests.post(f"{TELEGRAM_API}/editMessageText", json=payload)


def extract_gdrive_link(text: str) -> str | None:
    """Extract a Google Drive shareable link from user message."""
    import re
    patterns = [
        r"https://drive\.google\.com/file/d/[a-zA-Z0-9_-]+(?:/[^\s]*)?",
        r"https://drive\.google\.com/open\?id=[a-zA-Z0-9_-]+",
        r"https://drive\.google\.com/uc\?id=[a-zA-Z0-9_-]+",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(0)
    return None


def extract_file_id(drive_url: str) -> str | None:
    import re
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", drive_url)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", drive_url)
    if m:
        return m.group(1)
    return None


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "bot": "running"})


@app.route(f"/webhook", methods=["POST"])
def webhook():
    update = request.get_json(silent=True)
    if not update:
        return jsonify({"ok": True})

    message = update.get("message") or update.get("edited_message")
    if not message:
        return jsonify({"ok": True})

    chat_id = message["chat"]["id"]
    text = message.get("text", "")

    if text.startswith("/start"):
        send_message(
            chat_id,
            "👋 *স্বাগতম!*\n\n"
            "Google Drive-এর ZIP ফাইলের লিংক পাঠান।\n\n"
            "📌 *ফরম্যাট:*\n"
            "`https://drive.google.com/file/d/FILE_ID/view?usp=sharing`\n\n"
            "⚙️ আমি তারপর:\n"
            "• ছবিগুলো 4x আপস্কেল করব (Real-ESRGAN)\n"
            "• Adobe Stock SEO টাইটেল ও কিওয়ার্ড বানাব (Gemini)\n"
            "• GoFile-এ আপলোড করে ডাউনলোড লিংক পাঠাব",
        )
        return jsonify({"ok": True})

    drive_link = extract_gdrive_link(text)
    if not drive_link:
        send_message(
            chat_id,
            "❌ *Google Drive লিংক পাওয়া যায়নি।*\n\n"
            "সঠিক ফরম্যাটে লিংক পাঠান:\n"
            "`https://drive.google.com/file/d/FILE_ID/view?usp=sharing`",
        )
        return jsonify({"ok": True})

    file_id = extract_file_id(drive_link)
    if not file_id:
        send_message(chat_id, "❌ Drive লিংক থেকে File ID বের করা যায়নি।")
        return jsonify({"ok": True})

    status_msg_id = send_message(
        chat_id,
        "⏳ *প্রসেসিং শুরু হচ্ছে...*\n\n"
        "🔗 Kaggle-এর সাথে কানেক্ট করা হচ্ছে...",
    )

    try:
        trigger_kaggle_job(
            chat_id=str(chat_id),
            file_id=file_id,
            status_message_id=str(status_msg_id),
        )
        edit_message(
            chat_id,
            status_msg_id,
            "✅ *Kaggle Job চালু হয়েছে!*\n\n"
            "⚙️ GPU (T4×2) স্টার্ট হচ্ছে...\n"
            "📥 Google Drive থেকে ফাইল ডাউনলোড হবে...\n\n"
            "🔔 কাজ শেষ হলে এখানেই আপডেট পাবেন।",
        )
    except Exception as e:
        logger.error(f"Kaggle trigger error: {e}")
        edit_message(
            chat_id,
            status_msg_id,
            f"❌ *Kaggle Job চালু করতে সমস্যা হয়েছে।*\n\n`{str(e)[:200]}`",
        )

    return jsonify({"ok": True})


@app.route("/set_webhook", methods=["GET"])
def set_webhook():
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "")
    webhook_url = f"{render_url}/webhook"
    r = requests.get(f"{TELEGRAM_API}/setWebhook?url={webhook_url}")
    return jsonify(r.json())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
