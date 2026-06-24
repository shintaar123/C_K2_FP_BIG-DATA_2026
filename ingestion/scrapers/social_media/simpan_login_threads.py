from playwright.sync_api import sync_playwright
import os

print("Memulai Playwright...")
with sync_playwright() as p:
    # Membuka browser sungguhan yang bisa Anda lihat
    browser = p.firefox.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    print("Sedang membuka halaman login Threads...")
    page.goto("https://www.threads.net/login")
    
    print("\n=======================================================")
    print("BROWSER FIREFOX TELAH TERBUKA!")
    print("1. Silakan buka window Firefox yang baru saja muncul.")
    print("2. Login menggunakan username dan password akun TUMBAL Anda.")
    print("3. JANGAN gunakan akun pribadi yang penting!")
    print("4. Jika sudah berhasil login dan melihat beranda (Feed) Threads,")
    print("   kembali ke terminal ini lalu TEKAN ENTER.")
    print("=======================================================\n")
    
    input("TEKAN ENTER DI SINI JIKA SUDAH BERHASIL LOGIN... ")
    
    # Menyimpan sesi/cookies ke folder saat ini
    cookie_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies_threads.json")
    context.storage_state(path=cookie_path)
    
    print(f"\n[SUKSES] Sesi login berhasil disimpan di: {cookie_path}")
    browser.close()
