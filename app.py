import os
import re
import threading
import requests
from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, PushMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import json

DB_ENABLED = bool(os.environ.get("DATABASE_URL"))
if DB_ENABLED:
    from db import get_sire_stats, get_jockey_stats, stats_to_str, init_db
    from batch import run_weekly_batch

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

USER_IDS_FILE = "/tmp/user_ids.json"
HISTORY_STATUS_FILE = "/tmp/history_batch_status.json"

def load_user_ids():
    try:
        with open(USER_IDS_FILE, "r") as f:
            return set(json.load(f))
    except:
        return set()

def save_user_id(user_id):
    ids = load_user_ids()
    ids.add(user_id)
    with open(USER_IDS_FILE, "w") as f:
        json.dump(list(ids), f)

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
                good_horses.append({"banum": banum, "name": horse_name, "comment": comment, "grade": grade})
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
        return {"place": place_name, "surface": dist_match.group(1), "distance": dist_match.group(2)}
    except Exception as e:
        print(f"レース条件取得エラー {race_id}: {e}")
        return None

def get_horse_links(race_id):
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        soup = fetch_soup(url)
        horses = []
        seen_ids = set()
        rows = soup.find_all("tr", class_="HorseList")
        for row in rows:
            banum_td = row.find("td", class_=re.compile(r'^Umaban'))
            banum = banum_td.get_text(strip=True) if banum_td else ""
            link = row.find("a", href=re.compile(r"db\.netkeiba\.com/horse/\d+"))
            if not link:
                continue
            name = link.get_text(strip=True)
            href = link.get("href", "")
            if not href.startswith("http"):
                href = "https:" + href if href.startswith("//") else "https://db.netkeiba.com" + href
            m = re.search(r'/horse/(\d+)', href)
            if m and name and re.search(r'[\u30A0-\u30FF]{2,}', name):
                horse_id = m.group(1)
                if horse_id not in seen_ids:
                    seen_ids.add(horse_id)
                    horses.append({"banum": banum, "name": name, "url": f"https://db.netkeiba.com/horse/result/{horse_id}/"})
        return horses
    except Exception as e:
        print(f"馬リスト取得エラー: {e}")
        return []

def get_condition_stats(horse_url, target_place, target_surface, target_distance):
    try:
        soup = fetch_soup(horse_url, encoding="euc-jp")
        table = soup.find("table", class_="db_h_race_results")
        if not table:
            return False, ""
        rows = table.find_all("tr")
        w1, w2, w3, out = 0, 0, 0, 0
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 15:
                continue
            kaisai = cells[1]
            rank_str = cells[11]
            dist_raw = cells[14]
            if target_place not in kaisai:
                continue
            m = re.match(r'(芝|ダ)(\d+)', dist_raw)
            if not m:
                continue
            if m.group(1) != target_surface or m.group(2) != target_distance:
                continue
            try:
                rank = int(rank_str)
                if rank == 1: w1 += 1
                elif rank == 2: w2 += 1
                elif rank == 3: w3 += 1
                else: out += 1
            except:
                out += 1
        total = w1 + w2 + w3 + out
        if total == 0:
            return False, ""
        return (w1 + w2 + w3 > 0), f"{w1}-{w2}-{w3}-{out}"
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
        hit, stat = get_condition_stats(horse["url"], target_place, target_surface, target_distance)
        if hit:
            matched.append({"banum": horse["banum"], "name": horse["name"], "stat": stat})
    return matched, condition_str

def get_sire_jockey_info(race_id):
    if not DB_ENABLED:
        return []
    condition = get_race_condition(race_id)
    if not condition:
        return []
    surface = condition["surface"]
    distance = condition["distance"]
    place = condition["place"]
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        soup = fetch_soup(url)
        results = []
        rows = soup.find_all("tr", class_="HorseList")
        for row in rows:
            banum_td = row.find("td", class_=re.compile(r'^Umaban'))
            banum = banum_td.get_text(strip=True) if banum_td else ""
            name_link = row.find("a", href=re.compile(r"db\.netkeiba\.com/horse/\d+"))
            if not name_link:
                continue
            horse_name = name_link.get_text(strip=True)
            sire, bms = "", ""
            sire_links = row.find_all("a", href=re.compile(r"/horse/sire/"))
            if sire_links:
                sire = sire_links[0].get_text(strip=True)
            if len(sire_links) > 1:
                bms = sire_links[1].get_text(strip=True)
            jockey_td = row.find("td", class_="Jockey")
            jockey = jockey_td.get_text(strip=True) if jockey_td else ""
            sire_stat = stats_to_str(get_sire_stats(sire, 1, surface, int(distance), place)) if sire else None
            bms_stat = stats_to_str(get_sire_stats(bms, 2, surface, int(distance), place)) if bms else None
            jockey_stat = stats_to_str(get_jockey_stats(jockey, surface, int(distance), place)) if jockey else None
            if sire_stat or bms_stat or jockey_stat:
                results.append({
                    "banum": banum, "name": horse_name,
                    "sire": sire, "sire_stat": sire_stat,
                    "bms": bms, "bms_stat": bms_stat,
                    "jockey": jockey, "jockey_stat": jockey_stat,
                })
        return results
    except Exception as e:
        print(f"血統・騎手情報取得エラー: {e}")
        return []

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
        place_groups.setdefault(place_name, []).append(race_id)

    def build_block(groups, race_fn, header_fn, line_fn, found_check):
        results = []
        for place_name, ids in groups.items():
            lines = []
            found = False
            for race_id in sorted(ids):
                race_num = int(race_id[10:12])
                data = race_fn(race_id)
                if found_check(data):
                    found = True
                    lines.append(f"{race_num}R")
                    for item in data:
                        lines.append(line_fn(item))
            if found:
                results.append(f"【{place_name}】\n" + "\n".join(lines))
        return results

    def pack_messages(header, blocks):
        if not blocks:
            return []
        msgs = []
        current = header
        for block in blocks:
            if len(current) + len(block) + 2 > 4800:
                msgs.append(current)
                current = block
            else:
                current += "\n\n" + block
        msgs.append(current)
        return msgs

    a_blocks = build_block(
        place_groups,
        scrape_good_horses,
        lambda p: f"🏇 {date_display} 調教評価A",
        lambda h: f"  ⭐A {h['banum']}番 {h['name']} {'(' + h['comment'] + ')' if h['comment'] else ''}",
        lambda d: bool(d)
    )
    cond_blocks = []
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
                    banum_str = f"{h['banum']}番 " if h['banum'] else ""
                    lines.append(f"  🔁 {banum_str}{h['name']} [{h['stat']}]")
        if found:
            cond_blocks.append(f"【{place_name}】\n" + "\n".join(lines))

    sj_blocks = []
    if DB_ENABLED:
        for place_name, ids in place_groups.items():
            lines = []
            found = False
            for race_id in sorted(ids):
                race_num = int(race_id[10:12])
                horses = get_sire_jockey_info(race_id)
                if horses:
                    found = True
                    lines.append(f"{race_num}R")
                    for h in horses:
                        banum_str = f"{h['banum']}番 " if h['banum'] else ""
                        line = f"  🧬 {banum_str}{h['name']}"
                        if h['sire'] and h['sire_stat']:
                            line += f"\n     父:{h['sire']} [{h['sire_stat']}]"
                        if h['bms'] and h['bms_stat']:
                            line += f"\n     母父:{h['bms']} [{h['bms_stat']}]"
                        if h['jockey'] and h['jockey_stat']:
                            line += f"\n     騎手:{h['jockey']} [{h['jockey_stat']}]"
                        lines.append(line)
            if found:
                sj_blocks.append(f"【{place_name}】\n" + "\n".join(lines))

    messages = []
    messages += pack_messages(f"🏇 {date_display} 調教評価A", a_blocks) or [f"🏇 {date_display}\n本日はA評価の馬が見当たりませんでした。"]
    messages += pack_messages(f"🔁 {date_display} 同条件で過去3着内あり", cond_blocks) or [f"🔁 {date_display}\n同条件で過去3着内の馬は見当たりませんでした。"]
    messages += pack_messages(f"🧬 {date_display} 血統・騎手注目馬", sj_blocks)
    return messages[:5]

def send_push_messages(user_id, date_str=None):
    try:
        messages = build_line_messages(date_str)
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=user_id, messages=[TextMessage(text=m) for m in messages])
            )
        print(f"Push送信完了: {user_id}")
    except Exception as e:
        print(f"Push送信エラー: {e}")

def get_next_race_date():
    jst = pytz.timezone("Asia/Tokyo")
    tomorrow = (datetime.now(jst) + timedelta(days=1)).strftime("%Y%m%d")
    return tomorrow if get_today_race_ids(tomorrow) else None

def scheduled_daily_send():
    print(f"[Scheduler] 定時送信チェック: {datetime.now()}")
    next_date = get_next_race_date()
    if not next_date:
        print("[Scheduler] 翌日は開催なし - スキップ")
        return
    user_ids = load_user_ids()
    if not user_ids:
        print("[Scheduler] 登録ユーザーなし")
        return
    for uid in user_ids:
        t = threading.Thread(target=send_push_messages, args=(uid, next_date))
        t.daemon = True
        t.start()
    print(f"[Scheduler] {len(user_ids)}人に送信開始 ({next_date})")

def scheduled_weekly_batch():
    if not DB_ENABLED:
        return
    t = threading.Thread(target=run_weekly_batch)
    t.daemon = True
    t.start()

# =============================================
# 長期履歴バッチ：過去10年分を自動収集
# =============================================

def generate_sunday_list(start_year=2016, end_year=2026):
    """start_yearからend_yearまでの全日曜日リストを生成（新しい順）"""
    sundays = []
    d = datetime(end_year, 12, 31)
    start = datetime(start_year, 1, 1)
    while d >= start:
        if d.weekday() == 6:  # 日曜日
            sundays.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return sundays  # 新しい順

def save_history_status(status):
    with open(HISTORY_STATUS_FILE, "w") as f:
        json.dump(status, f)

def load_history_status():
    try:
        with open(HISTORY_STATUS_FILE, "r") as f:
            return json.load(f)
    except:
        return {"running": False, "completed": [], "total": 0, "current": "", "started_at": ""}

def run_full_history_batch(start_year=2016):
    """過去10年分を全て処理（バックグラウンド実行）"""
    if not DB_ENABLED:
        print("[HistoryBatch] DB無効 - スキップ")
        return

    all_sundays = generate_sunday_list(start_year=start_year)
    status = load_history_status()
    completed_set = set(status.get("completed", []))

    # 未処理の週だけ対象
    remaining = [d for d in all_sundays if d not in completed_set]
    total = len(remaining)
    print(f"[HistoryBatch] 開始: {total}週を処理（済み: {len(completed_set)}週）")

    status = {
        "running": True,
        "completed": list(completed_set),
        "total": len(all_sundays),
        "remaining": total,
        "current": "",
        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    save_history_status(status)

    for i, date_str in enumerate(remaining):
        try:
            status["current"] = date_str
            status["remaining"] = total - i
            status["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_history_status(status)

            print(f"[HistoryBatch] {i+1}/{total} 処理中: {date_str}")
            run_weekly_batch(date_str)

            completed_set.add(date_str)
            status["completed"] = list(completed_set)
            save_history_status(status)

        except Exception as e:
            print(f"[HistoryBatch] {date_str} エラー: {e}")
            continue

    status["running"] = False
    status["current"] = ""
    status["remaining"] = 0
    status["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_history_status(status)
    print(f"[HistoryBatch] 完了！ {len(completed_set)}週を処理")

# スケジューラー
scheduler = BackgroundScheduler(timezone=pytz.utc)
scheduler.add_job(scheduled_daily_send, CronTrigger(hour=11, minute=0, timezone=pytz.utc), id="daily_send", replace_existing=True)
scheduler.add_job(scheduled_weekly_batch, CronTrigger(day_of_week="sun", hour=13, minute=0, timezone=pytz.utc), id="weekly_batch", replace_existing=True)
scheduler.start()
print(f"[Scheduler] 起動完了 / DB: {'有効' if DB_ENABLED else '無効'}")

if DB_ENABLED:
    try:
        init_db()
    except Exception as e:
        print(f"[DB] 初期化エラー: {e}")

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
    user_id = event.source.user_id
    user_text = event.message.text.strip()
    save_user_id(user_id)
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text="📊 データを収集中です...\n少々お待ちください（1〜2分）")]
            )
        )
    if any(kw in user_text for kw in ["今日", "きょう", "本日", "調教"]):
        threading.Thread(target=send_push_messages, args=(user_id,), daemon=True).start()
    elif re.match(r'^\d{8}$', user_text):
        threading.Thread(target=send_push_messages, args=(user_id, user_text), daemon=True).start()
    elif user_text in ["ヘルプ", "help", "使い方"]:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=user_id, messages=[TextMessage(text=(
                    "🏇 使い方\n\n「今日」と送ると:\n"
                    "⭐ 調教評価Aの馬\n🔁 同条件で過去3着内の馬\n🧬 血統・騎手注目馬\n"
                    "を表示します。\n\n📅 開催前日20時に自動送信されます。"
                ))])
            )

@app.route("/", methods=["GET"])
def health():
    jst = pytz.timezone("Asia/Tokyo")
    now = datetime.now(jst).strftime("%Y/%m/%d %H:%M JST")
    user_count = len(load_user_ids())
    db_status = "有効" if DB_ENABLED else "無効"
    hist = load_history_status()
    hist_info = ""
    if hist.get("running"):
        done = len(hist.get("completed", []))
        total = hist.get("total", 0)
        current = hist.get("current", "")
        hist_info = f"\n📚 履歴収集中: {done}/{total}週 (現在:{current})"
    elif hist.get("completed"):
        hist_info = f"\n📚 履歴収集済み: {len(hist['completed'])}週"
    return f"🏇 Keiba LINE Bot [{now}]\n登録ユーザー: {user_count}人\nDB: {db_status}{hist_info}"

@app.route("/batch/run", methods=["GET"])
def batch_run():
    if not DB_ENABLED:
        return "DB未設定", 400
    date_str = request.args.get("date", datetime.now().strftime("%Y%m%d"))
    threading.Thread(target=run_weekly_batch, args=(date_str,), daemon=True).start()
    return f"バッチ開始: {date_str}", 200

@app.route("/batch/history", methods=["GET"])
def batch_history():
    """過去10年分の全データを収集するエンドポイント"""
    if not DB_ENABLED:
        return "DB未設定", 400
    hist = load_history_status()
    if hist.get("running"):
        done = len(hist.get("completed", []))
        total = hist.get("total", 0)
        current = hist.get("current", "")
        return f"📚 履歴収集は既に実行中です\n進捗: {done}/{total}週\n現在処理中: {current}", 200
    start_year = int(request.args.get("from", 2016))
    threading.Thread(target=run_full_history_batch, args=(start_year,), daemon=True).start()
    all_weeks = len(generate_sunday_list(start_year=start_year))
    return f"📚 履歴収集バッチ開始！\n対象: {start_year}年〜現在 ({all_weeks}週)\nバックグラウンドで処理中です。\n/batch/status で進捗確認できます。", 200

@app.route("/batch/status", methods=["GET"])
def batch_status():
    """バッチ処理の進捗状況を確認"""
    hist = load_history_status()
    if not hist.get("started_at"):
        return "📭 履歴収集バッチはまだ実行されていません。\n/batch/history で開始できます。", 200
    done = len(hist.get("completed", []))
    total = hist.get("total", 0)
    remaining = hist.get("remaining", 0)
    current = hist.get("current", "-")
    running = hist.get("running", False)
    started = hist.get("started_at", "")
    updated = hist.get("last_updated", "")
    pct = f"{done/total*100:.1f}" if total > 0 else "0"
    status_icon = "🔄" if running else "✅"
    return (
        f"{status_icon} 履歴収集バッチ\n"
        f"進捗: {done}/{total}週 ({pct}%)\n"
        f"残り: {remaining}週\n"
        f"現在: {current}\n"
        f"開始: {started}\n"
        f"更新: {updated}"
    ), 200

@app.route("/send_now", methods=["GET"])
def send_now():
    threading.Thread(target=scheduled_daily_send, daemon=True).start()
    return "定時送信テスト開始", 200

@app.route("/debug/<race_id>", methods=["GET"])
def debug(race_id):
    try:
        horses_a = scrape_good_horses(race_id)
        result = f"A評価の馬 ({len(horses_a)}頭)\n"
        for h in horses_a:
            result += f"  ⭐A {h['banum']}番 {h['name']} ({h['comment']})\n"
        matched, cond_str = get_condition_matched_horses(race_id)
        result += f"\n同条件({cond_str})で過去3着内 ({len(matched)}頭)\n"
        for h in matched:
            banum_str = f"{h['banum']}番 " if h['banum'] else ""
            result += f"  🔁 {banum_str}{h['name']} [{h['stat']}]\n"
        if DB_ENABLED:
            sj = get_sire_jockey_info(race_id)
            result += f"\n血統・騎手注目馬 ({len(sj)}頭)\n"
            for h in sj:
                banum_str = f"{h['banum']}番 " if h['banum'] else ""
                result += f"  🧬 {banum_str}{h['name']}\n"
                if h['sire_stat']:
                    result += f"     父:{h['sire']} [{h['sire_stat']}]\n"
                if h['bms_stat']:
                    result += f"     母父:{h['bms']} [{h['bms_stat']}]\n"
                if h['jockey_stat']:
                    result += f"     騎手:{h['jockey']} [{h['jockey_stat']}]\n"
        return result, 200, {"Content-Type": "text/plain; charset=utf-8"}
    except Exception as e:
        return f"Error: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
