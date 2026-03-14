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

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": "https://race.netkeiba.com/",
})

def fetch_soup(url):
    res = session.get(url, timeout=15)
    html = res.content.decode("euc-jp", errors="replace")
    return BeautifulSoup(html, "html.parser")

def get_today_race_ids(date_str=None):
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    url = f"https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={date_str}"
    try:
        soup = fetch_soup(url)
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
    try:
        soup = fetch_soup(url)
        good_horses = []
        rows = soup.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            cell_texts = [c.get_text(separator=" ", strip=True) for c in cells]
            grade = None
            for t in cell_texts:
                if t.strip() in ["A", "B"]:
                    grade = t.strip()
                    break
            if not grade:
                continue
            horse_name = None
            comment = ""
            if len(cells) >= 4:
                name_cell = cell_texts[3]
                name_part = name_cell.split("前走")[0].strip()
                name_part = name_part.split()[0] if name_part.split() else ""
                if re.search(r'[\u30A0-\u30FF]{2,}', name_part):
                    horse_name = name_part
                    if len(cells) >= 5:
                        comment = cell_texts[4].strip()
            if horse_name:
                good_horses.append({"name": horse_name, "comment": comment, "grade": grade})
        return good_horses
    except Exception as e:
        print(f"Error {race_id}: {e}")
        return []

def build_line_messages(date_str=None):
    """LINEに送る複数メッセージのリストを返す（各5000文字以内、最大5件）"""
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    date_display = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    race_ids = get_today_race_ids(date_str)
    if not race_ids:
        return [f"📭 {date_display} のレース情報が見つかりませんでした。"]

    # 場ごとにまとめる
    place_groups = {}
    for race_id in race_ids:
        place_code = race_id[4:6]
        place_name = PLACE_MAP.get(place_code, f"場{place_code}")
        if place_name not in place_groups:
            place_groups[place_name] = []
        place_groups[place_name].append(race_id)

    # 各場のB以上馬を収集
    place_results = []
    for place_name, ids in place_groups.items():
        lines = []
        found = False
        for race_id in sorted(ids):
            race_num = int(race_id[10:12])
            horses = scrape_good_horses(race_id)
            if horses:
                found = True
                lines.append(f"{race_num}R")
                for h in horses:
                    emoji = "⭐" if h["grade"] == "A" else "✅"
                    comment = f"({h['comment']})" if h['comment'] else ""
                    lines.append(f"  {emoji}{h['grade']} {h['name']} {comment}")
        if found:
            place_results.append(f"【{place_name}】\n" + "\n".join(lines))

    if not place_results:
        return [f"🏇 {date_display}\n本日はB評価以上の馬が見当たりませんでした。"]

    # メッセージを5000文字以内に分割（最大5メッセージ）
    header = f"🏇 {date_display} 調教評価（B以上）"
    messages = []
    current = header
    for block in place_results:
        if len(current) + len(block) + 2 > 4800:
            messages.append(current)
            current = block
        else:
            current += "\n\n" + block
    messages.append(current)

    # LINEは1返信最大5メッセージ
    return messages[:5]

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
        messages = build_line_messages()
    elif re.match(r'^\d{8}$', user_text):
        messages = build_line_messages(user_text)
    elif user_text in ["ヘルプ", "help", "使い方"]:
        messages = ["🏇 使い方\n\n「今日」と送ると本日の全レースから調教評価B以上の馬を一覧表示します。\n\n日付指定: 20260315 のように8桁の日付も使えます。"]
    else:
        messages = ["「今日」と送ると調教評価レポートをお届けします 🏇"]

    text_messages = [TextMessage(text=m) for m in messages]
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=text_messages
            )
        )

@app.route("/", methods=["GET"])
def health():
    return "🏇 Keiba LINE Bot is running!"

@app.route("/debug/<race_id>", methods=["GET"])
def debug(race_id):
    try:
        horses = scrape_good_horses(race_id)
        result = f"B以上の馬 ({len(horses)}頭)\n\n"
        for h in horses:
            result += f"{h['grade']} {h['name']} ({h['comment']})\n"
        return result, 200, {"Content-Type": "text/plain; charset=utf-8"}
    except Exception as e:
        return f"Error: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
