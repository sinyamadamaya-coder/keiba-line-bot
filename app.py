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
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            cell_texts = [c.get_text(separator=" ", strip=True) for c in cells]
            if not any(t.strip() == "A" for t in cell_texts):
                continue
            banum = cell_texts[1].strip() if len(cell_texts) > 1 else ""
            horse_name, comment = None, ""
            name_part = cell_texts[3].split("前走")[0].strip().split()
            name_part = name_part[0] if name_part else ""
            if re.search(r'[\u30A0-\u30FF]{2,}', name_part):
                horse_name = name_part
                if len(cells) >= 5:
                    comment = cell_texts[4].strip()
            if horse_name and banum.isdigit():
                good_horses.append({"banum": banum, "name": horse_name, "comment": comment})
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
        m = re.search(r'(芝|ダ)(\d{3,4})', soup.get_text())
        if not m:
            return None
        return {"place": place_name, "surface": m.group(1), "distance": m.group(2)}
    except Exception as e:
        print(f"レース条件取得エラー {race_id}: {e}")
        return None

def get_horse_list(race_id):
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        soup = fetch_soup(url)
        horses, seen_ids = [], set()
        for row in soup.find_all("tr", class_="HorseList"):
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
                    horses.append({
                        "banum": banum, "name": name,
                        "horse_id": horse_id,
                        "result_url": f"https://db.netkeiba.com/horse/result/{horse_id}/"
                    })
        return horses
    except Exception as e:
        print(f"馬リスト取得エラー: {e}")
        return []

def get_condition_stats(result_url, target_place, target_surface, target_distance):
    try:
        soup = fetch_soup(result_url, encoding="euc-jp")
        table = soup.find("table", class_="db_h_race_results")
        if not table:
            return False, ""
        w1, w2, w3, out = 0, 0, 0, 0
        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 15:
                continue
            kaisai, rank_str, dist_raw = cells[1], cells[11], cells[14]
            if target_place not in kaisai:
                continue
            m = re.match(r'(芝|ダ)(\d+)', dist_raw)
            if not m or m.group(1) != target_surface or m.group(2) != target_distance:
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
    tp, ts, td = condition["place"], condition["surface"], condition["distance"]
    condition_str = f"{tp}・{ts}{td}m"
    horses = get_horse_list(race_id)
    matched = []
    for horse in horses:
        hit, stat = get_condition_stats(horse["result_url"], tp, ts, td)
        if hit:
            matched.append({"banum": horse["banum"], "name": horse["name"], "stat": stat})
    return matched, condition_str

def get_sire_bms(horse_id):
    """db.netkeiba.com/horse/{id}/ の blood_table から父馬・母父馬を取得"""
    try:
        url = f"https://db.netkeiba.com/horse/{horse_id}/"
        soup = fetch_soup(url, encoding="euc-jp")
        bt = soup.find("table", class_="blood_table")
        if not bt:
            return "", ""
        rows = bt.find_all("tr")
        sire = rows[0].find_all("td")[0].get_text(strip=True) if rows else ""
        bms = ""
        if len(rows) >= 3:
            row2_cells = rows[2].find_all("td")
            if len(row2_cells) >= 2:
                bms = row2_cells[1].get_text(strip=True)
        return sire, bms
    except Exception as e:
        print(f"血統取得エラー horse_id={horse_id}: {e}")
        return "", ""

def get_sire_jockey_info(race_id):
    """各馬の血統（父・母父）と騎手をDBと照合して注目馬を返す"""
    if not DB_ENABLED:
        return []
    condition = get_race_condition(race_id)
    if not condition:
        return []
    surface = condition["surface"]
    distance = condition["distance"]
    place = condition["place"]
    horses = get_horse_list(race_id)
    if not horses:
        return []
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        soup = fetch_soup(url)
        jockey_map = {}
        for row in soup.find_all("tr", class_="HorseList"):
            name_link = row.find("a", href=re.compile(r"db\.netkeiba\.com/horse/\d+"))
            if not name_link:
                continue
            jockey_td = row.find("td", class_="Jockey")
            jockey = jockey_td.get_text(strip=True) if jockey_td else ""
            jockey_map[name_link.get_text(strip=True)] = jockey
    except Exception as e:
        print(f"騎手情報取得エラー: {e}")
        jockey_map = {}
    results = []
    for horse in horses:
        jockey = jockey_map.get(horse["name"], "")
        sire, bms = get_sire_bms(horse["horse_id"])
        sire_stat = stats_to_str(get_sire_stats(sire, 1, surface, int(distance), place)) if sire else None
        bms_stat = stats_to_str(get_sire_stats(bms, 2, surface, int(distance), place)) if bms else None
        jockey_stat = stats_to_str(get_jockey_stats(jockey, surface, int(distance), place)) if jockey else None
        if sire_stat or bms_stat or jockey_stat:
            results.append({
                "banum": horse["banum"], "name": horse["name"],
                "sire": sire, "sire_stat": sire_stat,
                "bms": bms, "bms_stat": bms_stat,
                "jockey": jockey, "jockey_stat": jockey_stat,
            })
    return results
def build_line_messages(date_str=None):
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    date_display = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    race_ids = get_today_race_ids(date_str)
    if not race_ids:
        return [f"📭 {date_display} のレース情報が見つかりませんでした。"]
    # 7R〜12Rのみに絞り込む
    race_ids = [rid for rid in race_ids if 7 <= int(rid[10:12]) <= 12]
    if not race_ids:
        return [f"📭 {date_display} 7R〜12Rのレース情報が見つかりませんでした。"]
    place_groups = {}
    for rid in race_ids:
        pname = PLACE_MAP.get(rid[4:6], f"場{rid[4:6]}")
        place_groups.setdefault(pname, []).append(rid)

    def pack(header, blocks):
        if not blocks: return []
        msgs, cur = [], header
        for b in blocks:
            if len(cur) + len(b) + 2 > 4800:
                msgs.append(cur); cur = b
            else:
                cur += "\n\n" + b
        msgs.append(cur)
        return msgs

    a_blocks = []
    for pname, ids in place_groups.items():
        lines, found = [], False
        for rid in sorted(ids):
            horses = scrape_good_horses(rid)
            if horses:
                found = True
                lines.append(f"{int(rid[10:12])}R")
                for h in horses:
                    c = f"({h['comment']})" if h["comment"] else ""
                    lines.append(f"  ⭐A {h['banum']}番 {h['name']} {c}")
        if found:
            a_blocks.append(f"【{pname}】\n" + "\n".join(lines))

    cond_blocks = []
    for pname, ids in place_groups.items():
        lines, found = [], False
        for rid in sorted(ids):
            matched, cond_str = get_condition_matched_horses(rid)
            if matched:
                found = True
                lines.append(f"{int(rid[10:12])}R ({cond_str})")
                for h in matched:
                    bstr = f"{h['banum']}番 " if h["banum"] else ""
                    lines.append(f"  🔁 {bstr}{h['name']} [{h['stat']}]")
        if found:
            cond_blocks.append(f"【{pname}】\n" + "\n".join(lines))

    sj_blocks = []
    if DB_ENABLED:
        for pname, ids in place_groups.items():
            lines, found = [], False
            for rid in sorted(ids):
                horses = get_sire_jockey_info(rid)
                if horses:
                    found = True
                    lines.append(f"{int(rid[10:12])}R")
                    for h in horses:
                        bstr = f"{h['banum']}番 " if h["banum"] else ""
                        line = f"  🧬 {bstr}{h['name']}"
                        if h["sire"] and h["sire_stat"]:
                            line += f"\n     父:{h['sire']} [{h['sire_stat']}]"
                        if h["bms"] and h["bms_stat"]:
                            line += f"\n     母父:{h['bms']} [{h['bms_stat']}]"
                        if h["jockey"] and h["jockey_stat"]:
                            line += f"\n     騎手:{h['jockey']} [{h['jockey_stat']}]"
                        lines.append(line)
            if found:
                sj_blocks.append(f"【{pname}】\n" + "\n".join(lines))

    msgs = []
    msgs += pack(f"🏇 {date_display} 調教評価A（7〜12R）", a_blocks) or [f"🏇 {date_display}\nA評価の馬が見当たりませんでした。"]
    msgs += pack(f"🔁 {date_display} 同条件で過去3着内あり（7〜12R）", cond_blocks) or [f"🔁 {date_display}\n同条件で過去3着内の馬は見当たりませんでした。"]
    msgs += pack(f"🧬 {date_display} 血統・騎手注目馬（7〜12R）", sj_blocks)
    return msgs[:5]

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

def build_weekend_summary():
    jst = pytz.timezone("Asia/Tokyo")
    now = datetime.now(jst)
    weekday = now.weekday()
    days_to_sat = (5 - weekday) % 7 or 7
    days_to_sun = (6 - weekday) % 7 or 7
    target_dates = []
    for d in sorted(set([days_to_sat, days_to_sun])):
        target_dates.append((now + timedelta(days=d)).strftime("%Y%m%d"))
    all_race_ids = []
    for ds in target_dates:
        all_race_ids.extend(get_today_race_ids(ds))
    if not all_race_ids:
        return "📅 今週末のレース情報がまだ公開されていません。\n当日に「今日」と送ってください。"
    # 7R〜12Rのみに絞り込む
    all_race_ids = [rid for rid in all_race_ids if 7 <= int(rid[10:12]) <= 12]
    place_ids = {}
    for rid in all_race_ids:
        pname = PLACE_MAP.get(rid[4:6], f"場{rid[4:6]}")
        place_ids.setdefault(pname, []).append(rid)
    date_labels = [f"{d[4:6]}/{d[6:]}" for d in target_dates]
    lines = [f"📅 今週末の注目馬（{' ・ '.join(date_labels)} / 7〜12R）",
             f"開催: {' / '.join(place_ids.keys())}", ""]
    total = 0
    for place, ids in place_ids.items():
        place_lines = []
        for rid in sorted(ids):
            sj = get_sire_jockey_info(rid) if DB_ENABLED else []
            if sj:
                place_lines.append(f"{int(rid[10:12])}R")
                for h in sj[:3]:
                    banum = f"{h['banum']}番 " if h["banum"] else ""
                    place_lines.append(f"  {banum}{h['name']}")
                    if h["sire"] and h["sire_stat"]:
                        place_lines.append(f"  父:{h['sire']} [{h['sire_stat']}]")
                    if h["jockey"] and h["jockey_stat"]:
                        place_lines.append(f"  {h['jockey']} [{h['jockey_stat']}]")
                    total += 1
        if place_lines:
            lines.append(f"【{place}】")
            lines.extend(place_lines)
            lines.append("")
    if total == 0:
        return "📅 今週末の注目馬データを収集中です。\n当日に「今日」と送ってください。"
    lines.append(f"（2016年〜のDBデータより {total}頭抽出）")
    return "\n".join(lines)

def send_weekend_summary(user_id):
    try:
        msg = build_weekend_summary()
        parts = [msg[i:i+4800] for i in range(0, len(msg), 4800)]
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=user_id, messages=[TextMessage(text=p) for p in parts[:5]])
            )
    except Exception as e:
        print(f"週末まとめ送信エラー: {e}")
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
        threading.Thread(target=send_push_messages, args=(uid, next_date), daemon=True).start()
    print(f"[Scheduler] {len(user_ids)}人に送信開始 ({next_date})")

def scheduled_weekly_batch():
    if not DB_ENABLED: return
    threading.Thread(target=run_weekly_batch, daemon=True).start()

def generate_sunday_list(start_year=2016, end_year=2026):
    sundays, d, start = [], datetime(end_year, 12, 31), datetime(start_year, 1, 1)
    while d >= start:
        if d.weekday() == 6: sundays.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return sundays

def save_history_status(status):
    with open(HISTORY_STATUS_FILE, "w") as f: json.dump(status, f)

def load_history_status():
    try:
        with open(HISTORY_STATUS_FILE, "r") as f: return json.load(f)
    except:
        return {"running": False, "completed": [], "total": 0, "current": "", "started_at": ""}

def run_full_history_batch(start_year=2016):
    if not DB_ENABLED: return
    all_sundays = generate_sunday_list(start_year=start_year)
    status = load_history_status()
    completed_set = set(status.get("completed", []))
    remaining = [d for d in all_sundays if d not in completed_set]
    total = len(remaining)
    print(f"[HistoryBatch] 開始: {total}週を処理")
    status = {"running": True, "completed": list(completed_set), "total": len(all_sundays),
              "remaining": total, "current": "",
              "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    save_history_status(status)
    for i, date_str in enumerate(remaining):
        try:
            status["current"] = date_str
            status["remaining"] = total - i
            status["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_history_status(status)
            run_weekly_batch(date_str)
            completed_set.add(date_str)
            status["completed"] = list(completed_set)
            save_history_status(status)
        except Exception as e:
            print(f"[HistoryBatch] {date_str} エラー: {e}")
    status["running"] = False; status["current"] = ""; status["remaining"] = 0
    status["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_history_status(status)
    print(f"[HistoryBatch] 完了！ {len(completed_set)}週を処理")

scheduler = BackgroundScheduler(timezone=pytz.utc)
scheduler.add_job(scheduled_daily_send, CronTrigger(hour=11, minute=0, timezone=pytz.utc), id="daily_send", replace_existing=True)
scheduler.add_job(scheduled_weekly_batch, CronTrigger(day_of_week="sun", hour=13, minute=0, timezone=pytz.utc), id="weekly_batch", replace_existing=True)
scheduler.start()
print(f"[Scheduler] 起動完了 / DB: {'有効' if DB_ENABLED else '無効'}")
if DB_ENABLED:
    try: init_db()
    except Exception as e: print(f"[DB] 初期化エラー: {e}")

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try: handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    user_text = event.message.text.strip()
    save_user_id(user_id)
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message_with_http_info(
            ReplyMessageRequest(reply_token=event.reply_token,
                messages=[TextMessage(text="📊 データを収集中です...\n少々お待ちください（1〜2分）")])
        )
    if any(kw in user_text for kw in ["今日", "きょう", "本日", "調教"]):
        threading.Thread(target=send_push_messages, args=(user_id,), daemon=True).start()
    elif user_text in ["今週末", "今週", "週末"]:
        threading.Thread(target=send_weekend_summary, args=(user_id,), daemon=True).start()
    elif re.match(r'^\d{8}$', user_text):
        threading.Thread(target=send_push_messages, args=(user_id, user_text), daemon=True).start()
    elif user_text in ["ヘルプ", "help", "使い方", "?"]:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=user_id, messages=[TextMessage(text=(
                    "🏇 使えるコマンド\n\n"
                    "「今日」→ 7〜12Rの調教評価・過去成績・血統騎手\n"
                    "「今週末」→ 週末7〜12Rの注目馬まとめ\n"
                    "「20260322」→ 日付指定\n\n"
                    "📅 開催前日20時に自動送信されます"
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
        hist_info = f"\n📚 履歴収集中: {done}/{hist.get('total',0)}週"
    elif hist.get("completed"):
        hist_info = f"\n📚 履歴収集済み: {len(hist['completed'])}週"
    return f"🏇 Keiba LINE Bot [{now}]\n登録ユーザー: {user_count}人\nDB: {db_status}{hist_info}"

@app.route("/batch/run", methods=["GET"])
def batch_run():
    if not DB_ENABLED: return "DB未設定", 400
    date_str = request.args.get("date", datetime.now().strftime("%Y%m%d"))
    threading.Thread(target=run_weekly_batch, args=(date_str,), daemon=True).start()
    return f"バッチ開始: {date_str}", 200

@app.route("/batch/history", methods=["GET"])
def batch_history():
    if not DB_ENABLED: return "DB未設定", 400
    hist = load_history_status()
    if hist.get("running"):
        done = len(hist.get("completed", []))
        return f"📚 履歴収集は既に実行中です\n進捗: {done}/{hist.get('total',0)}週", 200
    start_year = int(request.args.get("from", 2016))
    threading.Thread(target=run_full_history_batch, args=(start_year,), daemon=True).start()
    return f"📚 履歴収集バッチ開始！\n対象: {start_year}年〜現在 ({len(generate_sunday_list(start_year=start_year))}週)", 200

@app.route("/batch/status", methods=["GET"])
def batch_status():
    hist = load_history_status()
    if not hist.get("started_at"):
        return "📭 履歴収集バッチはまだ実行されていません。", 200
    done = len(hist.get("completed", []))
    total = hist.get("total", 0)
    pct = f"{done/total*100:.1f}" if total > 0 else "0"
    icon = "🔄" if hist.get("running") else "✅"
    return (f"{icon} 履歴収集バッチ\n進捗: {done}/{total}週 ({pct}%)\n"
            f"残り: {hist.get('remaining',0)}週\n現在: {hist.get('current','-')}\n"
            f"開始: {hist.get('started_at','')}\n更新: {hist.get('last_updated','')}"), 200

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
            bstr = f"{h['banum']}番 " if h["banum"] else ""
            result += f"  🔁 {bstr}{h['name']} [{h['stat']}]\n"
        if DB_ENABLED:
            sj = get_sire_jockey_info(race_id)
            result += f"\n血統・騎手注目馬 ({len(sj)}頭)\n"
            for h in sj:
                bstr = f"{h['banum']}番 " if h["banum"] else ""
                result += f"  🧬 {bstr}{h['name']}\n"
                if h["sire_stat"]: result += f"     父:{h['sire']} [{h['sire_stat']}]\n"
                if h["bms_stat"]: result += f"     母父:{h['bms']} [{h['bms_stat']}]\n"
                if h["jockey_stat"]: result += f"     騎手:{h['jockey']} [{h['jockey_stat']}]\n"
        return result, 200, {"Content-Type": "text/plain; charset=utf-8"}
    except Exception as e:
        import traceback
        return f"Error: {e}\n{traceback.format_exc()}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
