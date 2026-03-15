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

def fetch_soup(url, encoding="euc-jp"):
    res = session.get(url, timeout=15)
    html = res.content.decode(encoding, errors="replace")
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
                if t.strip() == "A":
                    grade = "A"
                    break
            if not grade:
                continue
            banum = cell_texts[1].strip() if len(cell_texts) > 1 else ""
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
            if horse_name and banum.isdigit():
                good_horses.append({
                    "banum": banum, "name": horse_name,
                    "comment": comment, "grade": grade
                })
        return good_horses
    except Exception as e:
        print(f"Error {race_id}: {e}")
        return []

def get_race_condition(race_id):
    place_code = race_id[4:6]
    place_name = PLACE_MAP.get(place_code, "")
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        soup = fetch_soup(url)
        text = soup.get_text()
        dist_match = re.search(r'(芝|ダ)(\d{3,4})', text)
        if not dist_match:
            return None
        return {
            "place": place_name,
            "surface": dist_match.group(1),
            "distance": dist_match.group(2),
        }
    except Exception as e:
        print(f"レース条件取得エラー {race_id}: {e}")
        return None

def get_horse_links(race_id):
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        soup = fetch_soup(url)
        horses = []
        seen_ids = set()
        links = soup.find_all("a", href=re.compile(r"db\.netkeiba\.com/horse/\d+"))
        for a in links:
            name = a.get_text(strip=True)
            href = a.get("href", "")
            if not href.startswith("http"):
                href = "https:" + href if href.startswith("//") else "https://db.netkeiba.com" + href
            m = re.search(r'/horse/(\d+)', href)
            if m and name and re.search(r'[\u30A0-\u30FF]{2,}', name):
                horse_id = m.group(1)
                if horse_id not in seen_ids:
                    seen_ids.add(horse_id)
                    horses.append({
                        "name": name,
                        "url": f"https://db.netkeiba.com/horse/{horse_id}/"
                    })
        return horses
    except Exception as e:
        print(f"馬リスト取得エラー: {e}")
        return []

def check_past_results(horse_url, target_place, target_surface, target_distance):
    try:
        soup = fetch_soup(horse_url, encoding="euc-jp")
        table = soup.find("table", class_="db_h_race_results")
        if not table:
            return False, ""
        rows = table.find_all("tr")
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 15:
                continue
            kaisai = cells[1]
            rank_str = cells[11]
            dist_raw = cells[14]
            try:
                rank = int(rank_str)
                if rank > 3:
                    continue
            except:
                continue
            if target_place not in kaisai:
                continue
            m = re.match(r'(芝|ダ)(\d+)', dist_raw)
            if not m:
                continue
            if m.group(1) != target_surface:
                continue
            if m.group(2) != target_distance:
                continue
            return True, f"{target_place}{dist_raw} {rank}着"
        return False, ""
    except Exception as e:
        print(f"過去成績取得エラー: {e}")
        return False, ""

def get_condition_matched_horses(race_id):
    condition = get_race_condition(race_id)
    if not condition:
        return [], ""
    target_place = condition["place"]
    target_surface = condition["surface"]
    target_distance = condition["distance"]
    condition_str = f"{target_place}・{target_surface}{target_distance}m"
    horses = get_horse_links(race_id)
    if not horses:
        return [], condition_str
    matched = []
    for horse in horses:
        hit, detail = check_past_results(
            horse["url"], target_place, target_surface, target_distance
        )
        if hit:
            matched.append({"name": horse["name"], "detail": detail})
    return matched, condition_str

def build_line_messages(date_str=None):
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    date_display = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    race_ids = get_today_race_ids(date_str)
    if not race_ids:
        return [f"📭 {date_display} のレース情報が見つかりませんでした。"]
    place_groups = {}
    for race_id in race_ids:
        place_code = race_id[4:6]
        place_name = PLACE_MAP.get(place_code, f"場{place_code}")
        if place_name not in place_groups:
            place_groups[place_name] = []
        place_groups[place_name].append(race_id)

    a_results = []
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
                    comment = f"({h['comment']})" if h['comment'] else ""
                    lines.append(f"  ⭐A {h['banum']}番 {h['name']} {comment}")
        if found:
            a_results.append(f"【{place_name}】\n" + "\n".join(lines))

    cond_results = []
    for place_name, ids in place_groups.items():
        lines = []
        found = False
        for race_id in sorted(ids):
            race_num = int(race_id[10:12])
            matched, cond_str = get_condition_matched_horses(race_id)
            if matched:
                found = True
                lines.append(f"{race_num}R ({cond_str})")
                for h in matched:
                    lines.append(f"  🔁 {h['name']} [{h['detail']}]")
        if found:
            cond_results.append(f"【{place_name}】\n" + "\n".join(lines))

    messages = []
    if a_results:
        current = f"🏇 {date_display} 調教評価A"
        for block in a_results:
            if len(current) + len(block) + 2 > 4800:
                messages.append(current)
                current = block
            else:
                current += "\n\n" + block
        messages.append(current)
    else:
        messages.append(f"🏇 {date_display}\n本日はA評価の馬が見当たりませんでした。")

    if cond_results:
        current = f"🔁 {date_display} 同条件で過去3着内あり"
        for block in cond_results:
            if len(current) + len(block) + 2 > 4800:
                messages.append(current)
                current = block
            else:
                current += "\n\n" + block
        messages.append(current)
    else:
        messages.append(f"🔁 {date_display}\n同条件で過去3着内の馬は見当たりませんでした。")

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
        messages = ["🏇 使い方\n\n「今日」と送ると:\n⭐ 調教評価Aの馬\n🔁 同条件で過去3着内の馬\nを表示します。"]
    else:
        messages = ["「今日」と送ると調教評価と過去成績レポートをお届けします 🏇"]
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
    """詳細デバッグ: レース条件・馬リスト・過去成績チェックを表示"""
    try:
        # A評価
        horses_a = scrape_good_horses(race_id)
        result = f"A評価の馬 ({len(horses_a)}頭)\n"
        for h in horses_a:
            result += f"  ⭐A {h['banum']}番 {h['name']} ({h['comment']})\n"

        # レース条件
        condition = get_race_condition(race_id)
        result += f"\nレース条件: {condition}\n"

        if not condition:
            return result, 200, {"Content-Type": "text/plain; charset=utf-8"}

        # 馬リスト（最初の5頭）
        horses = get_horse_links(race_id)
        result += f"出走馬 ({len(horses)}頭)\n"
        for h in horses[:5]:
            result += f"  {h['name']} → {h['url']}\n"

        # 各馬の成績詳細チェック（最初の5頭）
        result += f"\n--- 同条件({condition['place']}・{condition['surface']}{condition['distance']}m)チェック ---\n"
        for horse in horses[:5]:
            try:
                soup = fetch_soup(horse['url'], encoding="euc-jp")
                table = soup.find("table", class_="db_h_race_results")
                if not table:
                    result += f"  {horse['name']}: テーブルなし\n"
                    continue
                rows = table.find_all("tr")
                hits = []
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cells) < 15:
                        continue
                    kaisai = cells[1]
                    rank_str = cells[11]
                    dist_raw = cells[14]
                    if condition['place'] in kaisai and dist_raw.startswith(condition['surface']) and condition['distance'] in dist_raw:
                        hits.append(f"{kaisai} {dist_raw} {rank_str}着")
                if hits:
                    result += f"  {horse['name']}: ✅ {', '.join(hits[:3])}\n"
                else:
                    result += f"  {horse['name']}: 該当なし ({len(rows)-1}行)\n"
            except Exception as ex:
                result += f"  {horse['name']}: エラー {ex}\n"

        # 最終結果
        matched, cond_str = get_condition_matched_horses(race_id)
        result += f"\n▶ 同条件で過去3着内 ({len(matched)}頭)\n"
        for h in matched:
            result += f"  🔁 {h['name']} [{h['detail']}]\n"

        return result, 200, {"Content-Type": "text/plain; charset=utf-8"}
    except Exception as e:
        return f"Error: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
