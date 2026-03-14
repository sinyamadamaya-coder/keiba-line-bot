import os
import re
import requests
from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from bs4 import BeautifulSoup
from datetime import datetime

app = Flask(__name__)

LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

PLACE_MAP = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉"
}

def get_today_race_ids(date_str=None):
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    url = f"https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={date_str}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        links = soup.find_all("a", href=re.compile(r"race_id=\d+"))
        race_ids = list(dict.fromkeys([
            re.search(r"race_id=(\d+)", a["href"]).group(1)
            for a in links if re.search(r"race_id=(\d+)", a["href"])
        ]))
        return race_ids
    except Exception as e:
        print(f"Error: {e}")
        return []

def scrape_good_horses(race_id):
    url = f"https://race.netkeiba.com/race/oikiri.html?race_id={race_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        good_horses = []
        full_text = soup.get_text(separator='\n')
        lines = [l.strip() for l in full_text.split('\n') if l.strip()]
        for i, line in enumerate(lines):
            if line in ['A', 'B']:
                context = lines[max(0,i-6):i]
                horse_name = None
                comment = ""
                for j, ctx in enumerate(reversed(context)):
                    if re.match(r'^[\u30A0-\u30FF\u4E00-\u9FFF\u3040-\u309FA-Za-z]{2,12}$', ctx):
                        horse_name = ctx
                        remaining = context[len(context)-j:]
                        for r in remaining:
                            if r != '前走' and len(r) > 1 and not r.isdigit():
                                comment = r
                                break
                        break
                if horse_name:
                    good_horses.append({"name": horse_name, "comment": comment, "grade": line})
        return good_horses
    except Exception as e:
        print(f"Error {race_id}: {e}")
        return []

def build_line_message(date_str=None):
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    date_display = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    race_ids = get_today_race_ids(date_str)
    if not race_ids:
        return f"📭 {date_display} のレース情報が見つかりませんでした。"
    msg_lines = [f"🏇 {date_display} 調教評価レポート（B以上）\n"]
    found_any = False
    place_groups = {}
    for race_id in race_ids:
        place_code = race_id[4:6]
        place_name = PLACE_MAP.get(place_code, f"場{place_code}")
        if place_name not in place_groups:
            place_groups[place_name] = []
        place_groups[place_name].append(race_id)
    for place_name, ids in place_groups.items():
        place_lines = [f"【{place_name}】"]
        place_found = False
        for race_id in sorted(ids):
            race_num = int(race_id[10:12])
            horses = scrape_good_horses(race_id)
            if horses:
                found_any = True
                place_found = True
                place_lines.append(f"\n{race_num}R")
                for h in horses:
                    emoji = "⭐" if h["grade"] == "A" else "✅"
                    comment = f"({h['comment']})" if h['comment'] else ""
                    place_lines.append(f"  {emoji}{h['grade']} {h['name']} {comment}")
        if place_found:
            msg_lines.append("\n".join(place_lines))
    if not found_any:
        msg_lines.append("本日はB評価以上の馬が見当たりませんでした。")
    msg_lines.append(f"\n🔗 https://race.netkeiba.com/top/race_list.html?kaisai_date={date_str}")
    return "\n".join(msg_lines)

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_text = event.message.text.strip()
    if any(kw in user_text for kw in ["今日", "きょう", "本日", "調教"]):
        reply_text = build_line_message()
    elif re.match(r'^\d{8}$', user_text):
        reply_text = build_line_message(user_text)
    elif user_text in ["ヘルプ", "help", "使い方"]:
        reply_text = "🏇 使い方\n\n「今日」と送ると本日の全レースから調教評価B以上の馬を一覧表示します。\n\n日付指定: 20260315 のように8桁の日付も使えます。"
    else:
        reply_text = "「今日」と送ると調教評価レポートをお届けします 🏇\n「ヘルプ」で使い方を確認できます。"
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)]
            )
        )

@app.route("/", methods=["GET"])
def health():
    return "🏇 Keiba LINE Bot is running!"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
