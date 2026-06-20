"""
generate_scraper.py
Data generate untuk simulasi post X (Twitter) dan Reddit.
Dipakai karena:
- X: Twikit GraphQL ID expired (known bug, bukan salah implementasi)
- Reddit: diblokir Kominfo di Indonesia
Data dibuat realistis berdasarkan pola keluhan warga Surabaya nyata.
"""

import os
import sys
import random
import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scrapers.base_scraper import BaseScraper

TEMPLATES_X = [
    "Mati air dari kemarin sore sampai sekarang di {lokasi} Surabaya, udah lapor PDAM tapi belum ada respon #PDAMSurabaya",
    "Air PDAM di {lokasi} keruh banget sejak 3 hari lalu, ga bisa dipake masak. Tolong @PDAMSurabaya",
    "Sampah di TPS {lokasi} udah numpuk seminggu ga diangkut, bau banget! @pemkot_sby tolong dong",
    "Jalan di {lokasi} berlubang parah udah berbulan-bulan, minta segera diperbaiki #SurabayaTimur",
    "Banjir lagi di {lokasi} Surabaya, ketinggian lutut orang dewasa. Drainase mampet kayaknya",
    "Pipa PDAM bocor di depan {lokasi}, air ngalir ke jalan dari tadi pagi. Udah lapor belum ada yang datang",
    "Lampu jalan di {lokasi} mati sudah 2 minggu, jalan jadi gelap dan rawan. @pemkot_sby mohon ditindak",
    "Air PDAM di {lokasi} sudah 2 hari mati total, warga terpaksa beli air galon",
    "Sampah menumpuk di pinggir jalan {lokasi}, sudah dilaporkan ke RT tapi belum diambil",
    "Got mampet di {lokasi} bikin air meluap ke jalan setiap hujan. Tolong segera dikeruk @pemkot_sby",
]

TEMPLATES_REDDIT = [
    "Air PDAM di daerah {lokasi} mati 2 hari, udah lapor lewat aplikasi tapi ga ada update. Ada yang tau harus gimana?",
    "Numpang nanya, sampah di {lokasi} emang jarang diangkut ya? Udah seminggu ga ada petugas lewat",
    "Jalan rusak parah di kawasan {lokasi} Surabaya, lubangnya udah gede banget dan belum diperbaiki",
    "Banjir rutin tiap hujan di {lokasi}, warga udah protes berkali-kali tapi ga ada solusi dari pemkot",
    "Ada yang pernah komplain soal air keruh ke PDAM Surabaya? Di {lokasi} airnya keruh terus",
    "Update: pipa bocor di {lokasi} akhirnya diperbaiki setelah viral di medsos. Tapi butuh waktu 5 hari!",
    "Mohon info, listrik di {lokasi} sering byar-pet akhir-akhir ini. Udah lapor ke PLN tapi belum ada solusi",
    "Truk sampah di {lokasi} jadwalnya berantakan, kadang 3 hari sekali baru muncul",
]

LOKASI_SURABAYA = [
    "Gubeng", "Wonokromo", "Rungkut", "Mulyorejo", "Sukolilo",
    "Tenggilis Mejoyo", "Gunung Anyar", "Tambaksari", "Simokerto",
    "Bubutan", "Genteng", "Tegalsari", "Sawahan", "Dukuh Pakis",
    "Wiyung", "Lakarsantri", "Benowo", "Pakal", "Asemrowo",
    "Sukomanunggal", "Tandes", "Sambikerep", "Kenjeran", "Bulak",
    "Semampir", "Krembangan", "Pabean Cantian",
]

AUTHORS_X = ["warga_sby", "surabayainfo", "keluhanwarga", "pemerhatisby", "wargasurabaya1"]
AUTHORS_REDDIT = ["u/warga_surabaya", "u/sby_resident", "u/info_sby", "u/surabayans"]


def generate_x_records(n: int = 30) -> list[dict]:
    records = []
    now = datetime.datetime.now(datetime.timezone.utc)
    for i in range(n):
        lokasi = random.choice(LOKASI_SURABAYA)
        text = random.choice(TEMPLATES_X).format(lokasi=lokasi)
        published = (now - datetime.timedelta(hours=random.randint(1, 48))).isoformat()
        fake_id = f"x_gen_{i:04d}_{lokasi.lower().replace(' ', '_')}"
        records.append({
            "id": BaseScraper.make_id("x_generate", fake_id),
            "source_type": "x",
            "source_name": "x_generated",
            "raw_text": text,
            "author": random.choice(AUTHORS_X),
            "url": None,
            "likes": random.randint(0, 150),
            "shares": random.randint(0, 30),
            "published_at": published,
            "scraped_at": BaseScraper.now_iso(),
        })
    return records


def generate_reddit_records(n: int = 20) -> list[dict]:
    records = []
    now = datetime.datetime.now(datetime.timezone.utc)
    for i in range(n):
        lokasi = random.choice(LOKASI_SURABAYA)
        text = random.choice(TEMPLATES_REDDIT).format(lokasi=lokasi)
        published = (now - datetime.timedelta(hours=random.randint(1, 72))).isoformat()
        fake_id = f"reddit_gen_{i:04d}_{lokasi.lower().replace(' ', '_')}"
        subreddit = random.choice(["indonesia", "Surabaya"])
        records.append({
            "id": BaseScraper.make_id("reddit_generate", fake_id),
            "source_type": "reddit",
            "source_name": f"reddit_r_{subreddit}_generated",
            "raw_text": text,
            "author": random.choice(AUTHORS_REDDIT),
            "url": None,
            "likes": random.randint(1, 50),
            "shares": 0,
            "published_at": published,
            "scraped_at": BaseScraper.now_iso(),
        })
    return records


if __name__ == "__main__":
    x_records = generate_x_records(30)
    reddit_records = generate_reddit_records(20)
    print(f"Generated X records: {len(x_records)}")
    print(f"Generated Reddit records: {len(reddit_records)}")
    from scrapers.base_scraper import BaseScraper as BS
    print("\nSample X:")
    print(BS.to_jsonl(x_records[:2]))
    print("\nSample Reddit:")
    print(BS.to_jsonl(reddit_records[:2]))