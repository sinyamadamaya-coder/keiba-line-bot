"""
週次バッチ処理モジュール
毎週日曜夜に実行 - 当週の全レース結果を収集してDBに蓄積
対象: netkeiba + JRA公式
"""
import re
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from db import (
    upsert_sire_stats, upsert_jockey_stats,
    mark_race_processed, stats_to_str, get_conn
)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
})

PLACE_MAP = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉"
}

def fetch(url, encoding="euc-jp"):
    time.sleep(1)
    res = session.get(url, timeout=15)
    html = res.content.decode(encoding, errors="replace")
    return BeautifulSoup(html, "html.parser")

def get_race_ids_for_week(date_str):
    """指定週（日曜日の日付）の全race_idを取得"""
    sunday = datetime.strptime(date_str, "%Y%m%d")
    saturday = sunday - timedelta(days=1)
    race_ids = []
    for d in [saturday, sunday]:
        ds = d.strftime("%Y%m%d")
        url = f"https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={ds}"
        soup = fetch(url)
        links = soup.find_all("a", href=re.compile(r"race_id=\d+"))
        ids = list(dict.fromkeys([
            re.search(r"race_id=(\d+)", a["href"]).group(1)
            for a in links if re.search(r"race_id=(\d+)", a["href"])
        ]))
        race_ids.extend(ids)
    return race_ids

def get_race_result(race_id):
    """netkeibaのレース結果ページから全馬の着順・父馬・母父馬・騎手を取得"""
    url = f"https://db.netkeiba.com/race/{race_id}/"
    try:
        soup = fetch(url, encoding="euc-jp")
        place_code = race_id[4:6]
        place_name = PLACE_MAP.get(place_code, "")
        text = soup.get_text()
        dist_match = re.search(r'(芝|ダート)(\d{3,4})', text)
        if not dist_match:
            return []
        surface = "芝" if dist_match.group(1) == "芝" else "ダ"
        distance = int(dist_match.group(2))
        table = soup.find("table", class_="race_table_01")
        if not table:
            return []
        rows = table.find_all("tr")[1:]
        results = []
        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 20:
                continue
            try:
                rank = int(cells[0])
            except:
                continue
            tds = row.find_all("td")
            sire = ""
            bms = ""
            for td in tds:
                links = td.find_all("a", href=re.compile(r"/horse/sire/"))
                if links:
                    if not sire:
                        sire = links[0].get_text(strip=True)
                    elif not bms and len(links) > 1:
                        bms = links[1].get_text(strip=True)
            jockey = cells[7] if len(cells) > 7 else ""
            results.append({
                "rank": rank, "sire": sire, "bms": bms,
                "jockey": jockey, "surface": surface,
                "distance": distance, "place": place_name,
            })
        return results
    except Exception as e:
        print(f"レース結果取得エラー {race_id}: {e}")
        return []

def aggregate_and_upsert(race_id, results):
    """1レースの結果から血統・騎手の集計データをDBにUPSERT"""
    if not results:
        return
    surface = results[0]["surface"]
    distance = results[0]["distance"]
    place = results[0]["place"]
    sire_data = {}
    jockey_data = {}
    for r in results:
        rank = r["rank"]
        for gen, name in [(1, r["sire"]), (2, r["bms"])]:
            if not name:
                continue
            for p in [place, None]:
                key = (name, gen, p)
                if key not in sire_data:
                    sire_data[key] = [0, 0, 0, 0]
                if rank == 1: sire_data[key][0] += 1
                elif rank == 2: sire_data[key][1] += 1
                elif rank == 3: sire_data[key][2] += 1
                else: sire_data[key][3] += 1
        jockey = r["jockey"]
        if jockey:
            for p in [place, None]:
                key = (jockey, p)
                if key not in jockey_data:
                    jockey_data[key] = [0, 0, 0, 0]
                if rank == 1: jockey_data[key][0] += 1
                elif rank == 2: jockey_data[key][1] += 1
                elif rank == 3: jockey_data[key][2] += 1
                else: jockey_data[key][3] += 1

    sire_records = []
    for (name, gen, p), counts in sire_data.items():
        existing = _get_existing_sire(name, gen, surface, distance, p)
        sire_records.append({
            "sire_name": name, "generation": gen, "surface": surface,
            "distance": distance, "place": p,
            "wins":   (existing["wins"]   if existing else 0) + counts[0],
            "second": (existing["second"] if existing else 0) + counts[1],
            "third":  (existing["third"]  if existing else 0) + counts[2],
            "out_count": (existing["out"] if existing else 0) + counts[3],
        })
    jockey_records = []
    for (name, p), counts in jockey_data.items():
        existing = _get_existing_jockey(name, surface, distance, p)
        jockey_records.append({
            "jockey_name": name, "surface": surface, "distance": distance, "place": p,
            "wins":   (existing["wins"]   if existing else 0) + counts[0],
            "second": (existing["second"] if existing else 0) + counts[1],
            "third":  (existing["third"]  if existing else 0) + counts[2],
            "out_count": (existing["out"] if existing else 0) + counts[3],
        })
    if sire_records:
        upsert_sire_stats(sire_records)
    if jockey_records:
        upsert_jockey_stats(jockey_records)

def _get_existing_sire(sire_name, generation, surface, distance, place):
    sql = """SELECT wins, second, third, out_count FROM sire_stats
             WHERE sire_name=%s AND generation=%s AND surface=%s
               AND distance=%s AND place IS NOT DISTINCT FROM %s"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (sire_name, generation, surface, distance, place))
            row = cur.fetchone()
    return {"wins": row[0], "second": row[1], "third": row[2], "out": row[3]} if row else None

def _get_existing_jockey(jockey_name, surface, distance, place):
    sql = """SELECT wins, second, third, out_count FROM jockey_stats
             WHERE jockey_name=%s AND surface=%s
               AND distance=%s AND place IS NOT DISTINCT FROM %s"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (jockey_name, surface, distance, place))
            row = cur.fetchone()
    return {"wins": row[0], "second": row[1], "third": row[2], "out": row[3]} if row else None

def run_weekly_batch(sunday_date_str=None):
    """メイン処理 - 当週の全レースを処理してDBに蓄積"""
    if not sunday_date_str:
        sunday_date_str = __import__('datetime').datetime.now().strftime("%Y%m%d")
    print(f"[Batch] 週次バッチ開始: {sunday_date_str}")
    race_ids = get_race_ids_for_week(sunday_date_str)
    print(f"[Batch] 対象レース数: {len(race_ids)}")
    success = 0
    for i, race_id in enumerate(race_ids):
        try:
            results = get_race_result(race_id)
            if results:
                aggregate_and_upsert(race_id, results)
                place = results[0]["place"]
                surface = results[0]["surface"]
                distance = results[0]["distance"]
                mark_race_processed(race_id, sunday_date_str, place, surface, distance)
                success += 1
            print(f"[Batch] {i+1}/{len(race_ids)} {race_id} 完了")
        except Exception as e:
            print(f"[Batch] {race_id} エラー: {e}")
    print(f"[Batch] 完了: {success}/{len(race_ids)} レース処理")
    return success
