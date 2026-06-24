import os
import sys
import time
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from playwright.sync_api import sync_playwright

def scrape_threads_nyata(hashtags, max_scrolls=2):
    """
    Scraper Threads menggunakan Playwright dengan cookies yang sudah tersimpan.
    """
    cookie_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies_threads.json")
    
    if not os.path.exists(cookie_path):
        print(f"[ERROR] File {cookie_path} tidak ditemukan!")
        print("Jalankan 'python simpan_login_threads.py' terlebih dahulu untuk login.")
        return []

    hasil_teks = []
    
    with sync_playwright() as p:
        # headless=True agar berjalan senyap di background (tanpa membuka window GUI)
        browser = p.chromium.launch(headless=True)
        # Muat state/cookies agar kita langsung terdeteksi login
        context = browser.new_context(storage_state=cookie_path)
        page = context.new_page()

        for tag in hashtags:
            print(f"\n[Threads] Mulai mencari hashtag: #{tag}")
            url = f"https://www.threads.net/search?q=%23{tag}&serp_type=tags"
            
            try:
                page.goto(url)
                # Tunggu elemen pembungkus post (biasanya memiliki class yang panjang)
                # Catatan: Class 'x1a2a7pz' ini bisa berubah sewaktu-waktu oleh pihak Meta.
                # Alternatif yang lebih aman adalah mencari role="article"
                page.wait_for_selector('div[data-pressable-container="true"]', timeout=15000)
                
                # Simulasi membaca (scroll)
                for i in range(max_scrolls):
                    time.sleep(random.uniform(2.5, 5.0))  # Jeda acak 2.5 - 5 detik
                    page.mouse.wheel(0, 800) # Scroll ke bawah
                
                time.sleep(2) # Tunggu hasil load selesai
                
                # Ambil semua teks dari hasil pencarian
                posts = page.locator('div[data-pressable-container="true"]').all_inner_texts()
                
                for post in posts:
                    # Filter teks kosong
                    if post.strip() and len(post.strip()) > 15:
                        hasil_teks.append(post.strip())
                        
                print(f"[Threads] Menemukan {len(posts)} post di #{tag}")
                
            except Exception as e:
                print(f"[Threads] Gagal menarik #{tag} atau tidak ada postingan: {e}")
            
            # Jeda antar pencarian agar tidak dianggap bot spam
            time.sleep(random.uniform(3, 7))
            
        browser.close()
        
    return hasil_teks

if __name__ == "__main__":
    tags = ["keluhansurabaya", "pdamsurabaya", "surabaya"]
    print("Memulai scraping data NYATA Threads...")
    data_nyata = scrape_threads_nyata(tags)
    
    print("\n" + "="*50)
    print(f"SELESAI! Ditemukan {len(data_nyata)} data nyata.")
    print("="*50)
    
    for i, teks in enumerate(data_nyata[:5], 1): # Tampilkan 5 sampel
        print(f"\n[{i}] {teks[:200]}...")
