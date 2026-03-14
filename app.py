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

# JRAの場コード → 場名マッピング
JRA_PLACE_CODES = {
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
    """A評価の馬のみ取得（馬番付き）"""
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
                    "banum": banum,
                    "name": horse_name,
                    "comment": comment,
                    "grade": grade
                })
        return good_horses
    except Exception as e:
        print(f"Error {race_id}: {e}")
        return []

# ==================== JRA 過去成績チェック ====================

def get_jra_shutuba_url(date_str, place_code, kai, nichi, race_num):
    """JRA出馬表URLを組み立てる"""
    # 例: pw01dde0106202602060120260315
    # 形式: pw01dde + 場コード2 + 回2 + 場名コード2 + 日2 + レース番号2 + 年月日8
    # 実際はnetkeibaのrace_idから組み立て
    pass

def get_jra_race_condition(race_id):
    """
    race_idからJRA出馬表URLを取得し、レース条件（競馬場・芝ダ・距離）を返す
    race_id例: 202606020501
      年4桁 + 場コード2桁 + 回2桁 + 日2桁 + レース番号2桁
    """
    year = race_id[:4]
    place_code = race_id[4:6]
    kai = race_id[6:8]
    nichi = race_id[8:10]
    race_num = race_id[10:12]
    place_name = PLACE_MAP.get(place_code, "")

    # netkeiba の出馬表から条件取得
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        soup = fetch_soup(url)
        text = soup.get_text()
        # 「ダ1200」「芝1600」形式を探す
        dist_match = re.search(r'(芝|ダ)[\s]*(\d{3,4})', text)
        if not dist_match:
            # 「1200m」「1200メートル」形式も試みる
            dist_match2 = re.search(r'(\d{3,4})\s*[mｍメートル]', text)
            distance = dist_match2.group(1) if dist_match2 else ""
            surface = ""
        else:
            surface = dist_match.group(1)  # 芝 or ダ
            distance = dist_match.group(2)

        return {
            "place": place_name,
            "surface": surface,  # 芝 or ダ
            "distance": distance,  # 数字のみ
        }
    except Exception as e:
        print(f"レース条件取得エラー {race_id}: {e}")
        return None

def get_jra_horse_links(race_id):
    """
    JRA出馬表から全馬の（馬番、馬名、過去成績URL）を取得
    JRAのURLはnetkeibaのrace_idから組み立て
    """
    year = race_id[:4]
    place_code = race_id[4:6]
    kai = int(race_id[6:8])
    nichi = int(race_id[8:10])
    race_num = int(race_id[10:12])

    # JRA出馬表URL: CNAME=pw01dde + 場2 + 回2 + 場2 + 日2 + レース2 + 年月日 + /97
    kai_str = str(kai).zfill(2)
    nichi_str = str(nichi).zfill(2)
    race_str = str(race_num).zfill(2)
    date_str_jra = datetime.now().strftime("%Y%m%d")  # 今日の日付

    cname = f"pw01dde{place_code}{kai_str}{place_code}{nichi_str}{race_str}{year}{date_str_jra}"
    jra_url = f"https://www.jra.go.jp/JRADB/accessD.html?CNAME={cname}/97"

    try:
        res = session.get(jra_url, timeout=15)
        html = res.content.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")

        horses = []
        links = soup.find_all("a", href=re.compile(r"accessU\.html"))
        for a in links:
            name = a.get_text(strip=True)
            href = a.get("href", "")
            if not href.startswith("http"):
                href = "https://www.jra.go.jp/JRADB/" + href.lstrip("/")
            if name and len(name) >= 2 and re.search(r'[\u30A0-\u30FF]', name):
                horses.append({"name": name, "url": href})
        return horses
    except Exception as e:
        print(f"JRA出馬表取得エラー: {e}")
        return []

def check_horse_past_results(horse_url, target_place, target_surface, target_distance):
    """
    馬の過去成績ページから、指定条件（競馬場・芝ダ・距離）で1〜3着があるか確認
    """
    try:
        res = session.get(horse_url, timeout=15)
        html = res.content.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")

        # 出走レーステーブルを取得
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
            # 「場」「距離」「着順」の列を特定
            if "場" not in headers or "距離" not in headers or "着順" not in headers:
                continue
            place_idx = headers.index("場")
            dist_idx = headers.index("距離")
            rank_idx = headers.index("着順")

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
                if len(cells) <= max(place_idx, dist_idx, rank_idx):
                    continue
                place = cells[place_idx]
                dist_raw = cells[dist_idx]  # 例: "ダ1200" "芝1600"
                rank_str = cells[rank_idx]

                # 着順チェック（1〜3着）
                try:
                    rank = int(rank_str)
                    if rank > 3:
                        continue
                except:
                    continue

                # 競馬場チェック
                if place != target_place:
                    continue

                # 芝ダチェック
                surface_match = re.match(r'(芝|ダ)(\d+)', dist_raw)
                if not surface_match:
                    continue
                surface = surface_match.group(1)
                distance = surface_match.group(2)

                if surface != target_surface:
                    continue
                if distance != target_distance:
                    continue

                # 全条件一致！
                return True, f"{place}{dist_raw} {rank}着"

        return False, ""
    except Exception as e:
        print(f"過去成績取得エラー {horse_url}: {e}")
        return False, ""

def get_condition_matched_horses(race_id):
    """
    指定レースの出走馬で、同条件（競馬場・芝ダ・距離）で過去1〜3着がある馬リスト
    """
    # 1. レース条件を取得
    condition = get_jra_race_condition(race_id)
    if not condition:
        return [], ""

    target_place = condition["place"]
    target_surface = condition["surface"]
    target_distance = condition["distance"]
    condition_str = f"{target_place}・{target_surface}{target_distance}m"

    # 2. JRA出馬表から馬リストを取得
    horses = get_jra_horse_links(race_id)
    if not horses:
        return [], condition_str

    # 3. 各馬の過去成績をチェック
    matched = []
    for horse in horses:
        hit, detail = check_horse_past_results(horse["url"], target_place, target_surface, target_distance)
        if hit:
            matched.append({"name": horse["name"], "detail": detail})

    return matched, condition_str

# ==================== LINE メッセージ生成 ====================

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

    # ① A評価の馬
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

    # ② 同条件で過去1〜3着がある馬
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

    # A評価メッセージ
    if a_results:
        header_a = f"🏇 {date_display} 調教評価A（最高評価）"
        current = header_a
        for block in a_results:
            if len(current) + len(block) + 2 > 4800:
                messages.append(current)
                current = block
            else:
                current += "\n\n" + block
        messages.append(current)
    else:
        messages.append(f"🏇 {date_display}\n本日はA評価の馬が見当たりませんでした。")

    # 同条件過去成績メッセージ
    if cond_results:
        header_c = f"🔁 {date_display} 同条件で過去3着内あり"
        current = header_c
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
        messages = ["🏇 使い方\n\n「今日」と送ると:\n⭐ 調教評価Aの馬\n🔁 同条件で過去3着内の馬\nを一覧表示します。"]
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
    try:
        # A評価
        horses = scrape_good_horses(race_id)
        result = f"A評価の馬 ({len(horses)}頭)\n"
        for h in horses:
            result += f"⭐A {h['banum']}番 {h['name']} ({h['comment']})\n"
        # 同条件過去成績
        matched, cond_str = get_condition_matched_horses(race_id)
        result += f"\n同条件({cond_str})で過去3着内 ({len(matched)}頭)\n"
        for h in matched:
            result += f"🔁 {h['name']} [{h['detail']}]\n"
        return result, 200, {"Content-Type": "text/plain; charset=utf-8"}
    except Exception as e:
        return f"Error: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
