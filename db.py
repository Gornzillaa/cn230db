# ชยวัฒน์ กาญจนะแก้ว 6610685122
import sqlite3
import requests
import json
import datetime # อาจจะใช้ถ้าต้องการแปลง timestamp เป็นวันที่

# --- กำหนดค่าคงที่ ---
DATABASE_NAME = "cheapshark_deals.db"
# URL ของ API ที่คุณเลือก
# storeID=1 คือ Steam, upperPrice=15 คือ ดีลที่มีราคาสูงสุดไม่เกิน 15 USD
API_URL = "https://www.cheapshark.com/api/1.0/deals?storeID=1&upperPrice=15"

# --- ฟังก์ชันสำหรับดึงข้อมูลจาก API ---
def fetch_deals_from_api(api_url):
    """ดึงข้อมูลดีลเกมจาก CheapShark API"""
    print(f"Fetching data from API: {api_url}")
    try:
        response = requests.get(api_url)
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        print(f"Successfully fetched {len(data)} deals.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None
    except json.JSONDecodeError:
        print("Error decoding JSON response from API.")
        return None

# --- ฟังก์ชันสำหรับตั้งค่าฐานข้อมูล SQLite ---
def setup_database(db_name):
    """สร้างการเชื่อมต่อและตารางในฐานข้อมูล SQLite สำหรับดีลเกม"""
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # สร้างตาราง deals ถ้ายังไม่มี
        # กำหนดคอลัมน์ตาม key ใน JSON
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT, -- ID อัตโนมัติ
                internalName TEXT,
                title TEXT,
                metacriticLink TEXT,
                dealID TEXT UNIQUE, -- dealID น่าจะเป็นค่าที่ไม่ซ้ำกัน
                storeID INTEGER, -- แปลงเป็น INTEGER
                gameID INTEGER, -- แปลงเป็น INTEGER
                salePrice REAL, -- ราคาขาย (ทศนิยม)
                normalPrice REAL, -- ราคาปกติ (ทศนิยม)
                isOnSale INTEGER, -- 1 หรือ 0 (Boolean)
                savings REAL, -- เปอร์เซ็นต์ส่วนลด (ทศนิยม)
                metacriticScore INTEGER, -- คะแนน Metacritic (ตัวเลข)
                steamRatingText TEXT, -- ข้อความรีวิว Steam
                steamRatingPercent INTEGER, -- เปอร์เซ็นต์รีวิว Steam (ตัวเลข)
                steamRatingCount INTEGER, -- จำนวนรีวิว Steam (ตัวเลข)
                steamAppID INTEGER, -- Steam App ID (ตัวเลข)
                releaseDate INTEGER, -- Timestamp (ตัวเลข)
                lastChange INTEGER, -- Timestamp (ตัวเลข)
                dealRating REAL, -- คะแนนดีล (ทศนิยม)
                thumb TEXT -- URL รูปภาพ
            )
        ''')
        conn.commit()
        print(f"Database '{db_name}' and table 'deals' are ready.")
        return conn
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        return None

# --- ฟังก์ชันสำหรับนำข้อมูลดีลเข้าฐานข้อมูล ---
def insert_deals_data(conn, deals_list):
    """เพิ่มข้อมูลดีลเกมลงในตาราง deals"""
    if not conn or not deals_list:
        print("Cannot insert data: No connection or no deals data.")
        return 0

    cursor = conn.cursor()
    inserted_count = 0

    # คำสั่ง SQL สำหรับเพิ่มข้อมูล
    # ใช้ INSERT OR IGNORE เพื่อข้ามถ้า dealID ซ้ำ (ป้องกันการเพิ่มดีลเดิมซ้ำๆ)
    sql = ''' INSERT OR IGNORE INTO deals(
                internalName, title, metacriticLink, dealID, storeID, gameID,
                salePrice, normalPrice, isOnSale, savings, metacriticScore,
                steamRatingText, steamRatingPercent, steamRatingCount, steamAppID,
                releaseDate, lastChange, dealRating, thumb
              )
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''

    deals_to_insert = []
    for deal in deals_list:
        # ดึงข้อมูลจาก JSON object แต่ละดีล
        # ใช้ .get() เพื่อป้องกัน KeyError ถ้า key ไม่มีในบาง object
        # แปลงประเภทข้อมูลให้ตรงกับที่กำหนดใน CREATE TABLE
        try:
            internalName = deal.get('internalName')
            title = deal.get('title')
            metacriticLink = deal.get('metacriticLink')
            dealID = deal.get('dealID')
            storeID = int(deal.get('storeID')) if deal.get('storeID') else None
            gameID = int(deal.get('gameID')) if deal.get('gameID') else None
            salePrice = float(deal.get('salePrice')) if deal.get('salePrice') else None
            normalPrice = float(deal.get('normalPrice')) if deal.get('normalPrice') else None
            isOnSale = int(deal.get('isOnSale')) if deal.get('isOnSale') else None
            savings = float(deal.get('savings')) if deal.get('savings') else None
            metacriticScore = int(deal.get('metacriticScore')) if deal.get('metacriticScore') else None
            steamRatingText = deal.get('steamRatingText')
            steamRatingPercent = int(deal.get('steamRatingPercent')) if deal.get('steamRatingPercent') else None
            steamRatingCount = int(deal.get('steamRatingCount')) if deal.get('steamRatingCount') else None
            steamAppID = int(deal.get('steamAppID')) if deal.get('steamAppID') else None
            releaseDate = int(deal.get('releaseDate')) if deal.get('releaseDate') else None # เก็บเป็น timestamp
            lastChange = int(deal.get('lastChange')) if deal.get('lastChange') else None # เก็บเป็น timestamp
            dealRating = float(deal.get('dealRating')) if deal.get('dealRating') else None
            thumb = deal.get('thumb')

            # ตรวจสอบข้อมูลที่จำเป็น (เช่น ต้องมี dealID และ title)
            if dealID and title:
                 deals_to_insert.append((
                     internalName, title, metacriticLink, dealID, storeID, gameID,
                     salePrice, normalPrice, isOnSale, savings, metacriticScore,
                     steamRatingText, steamRatingPercent, steamRatingCount, steamAppID,
                     releaseDate, lastChange, dealRating, thumb
                 ))
            else:
                 print(f"Skipping deal due to missing dealID or title: {deal}")

        except (ValueError, TypeError) as e:
             print(f"Error processing deal data: {deal}. Error: {e}")
             continue # ข้าม deal นี้ ถ้ามีปัญหาในการแปลงประเภทข้อมูล


    try:
        cursor.executemany(sql, deals_to_insert)
        conn.commit()
        inserted_count = cursor.rowcount # จำนวนแถวที่ถูกเพิ่มจริงๆ (ไม่รวมที่ถูก IGNORE)
        print(f"Successfully inserted {inserted_count} new deal records.")
        return inserted_count
    except sqlite3.Error as e:
        print(f"Failed to insert deals data: {e}")
        conn.rollback()
        return 0

# --- ฟังก์ชันสำหรับ Data Analytics ด้วย SQL ---
def analyze_deals_data(conn):
    """วิเคราะห์ข้อมูลดีลเกมในฐานข้อมูลด้วย SQL"""
    if not conn:
        print("Cannot analyze data: No connection.")
        return

    cursor = conn.cursor()
    print("\n--- Data Analysis Results ---")

    try:
        # 1: นับจำนวนดีลทั้งหมด
        cursor.execute("SELECT COUNT(*) FROM deals")
        total_deals = cursor.fetchone()[0]
        print(f"1. Total number of deals stored: {total_deals}")

        # 2: แสดงดีล 5 อันดับแรกที่มีส่วนลด (savings) สูงสุด
        print("\n2. Top 5 deals with highest savings:")
        cursor.execute("SELECT title, savings, salePrice, normalPrice FROM deals ORDER BY savings DESC LIMIT 5")
        top_savings_deals = cursor.fetchall()
        for i, (title, savings, salePrice, normalPrice) in enumerate(top_savings_deals):
            print(f"   - Rank {i+1}: {title} - Savings: {savings:.2f}%, Price: ${salePrice:.2f} (Normal: ${normalPrice:.2f})")

        # 3: แสดงดีล 5 อันดับแรกที่มีคะแนน Metacritic สูงสุด
        print("\n3. Top 5 deals with highest Metacritic Score:")
        # กรองเฉพาะดีลที่มีคะแนน Metacritic
        cursor.execute("SELECT title, metacriticScore, salePrice FROM deals WHERE metacriticScore IS NOT NULL ORDER BY metacriticScore DESC LIMIT 5")
        top_metacritic_deals = cursor.fetchall()
        for i, (title, score, price) in enumerate(top_metacritic_deals):
            print(f"   - Rank {i+1}: {title} - Metacritic: {score}, Price: ${price:.2f}")

        # 4: แสดงดีล 5 อันดับแรกที่มี Steam Rating Percent สูงสุด (และมีจำนวนรีวิวมากพอสมควร)
        print("\n4. Top 5 deals with highest Steam Rating Percent (min 1000 reviews):")
        # กรองเฉพาะดีลที่มี Steam Rating และมีจำนวนรีวิวอย่างน้อย 1000
        cursor.execute("SELECT title, steamRatingText, steamRatingPercent, steamRatingCount, salePrice FROM deals WHERE steamRatingPercent IS NOT NULL AND steamRatingCount >= 1000 ORDER BY steamRatingPercent DESC, steamRatingCount DESC LIMIT 5")
        top_steam_rating_deals = cursor.fetchall()
        for i, (title, rating_text, percent, count, price) in enumerate(top_steam_rating_deals):
             print(f"   - Rank {i+1}: {title} - Steam Rating: {rating_text} ({percent}%, {count} reviews), Price: ${price:.2f}")

        # 5: แสดงค่าเฉลี่ย Steam Rating Percent ของดีลทั้งหมด
        cursor.execute("SELECT AVG(steamRatingPercent) FROM deals WHERE steamRatingPercent IS NOT NULL")
        avg_steam_rating = cursor.fetchone()[0]
        if avg_steam_rating is not None:
             print(f"\n5. Average Steam Rating Percent of all deals: {avg_steam_rating:.2f}%")
        else:
             print("\n5. No Steam Rating Percent data available for average calculation.")

        # 6: แสดงจำนวนดีลแยกตาม Steam Rating Text
        print("\n6. Number of deals by Steam Rating Text:")
        cursor.execute("SELECT steamRatingText, COUNT(*) FROM deals WHERE steamRatingText IS NOT NULL GROUP BY steamRatingText ORDER BY COUNT(*) DESC")
        rating_counts = cursor.fetchall()
        for rating_text, count in rating_counts:
            print(f"   - {rating_text}: {count}")

    except sqlite3.Error as e:
        print(f"Error during data analysis: {e}")

# --- ส่วน Main Execution ---
if __name__ == "__main__":
    # 1. Setup Database
    connection = setup_database(DATABASE_NAME)

    if connection:
        # 2. Fetch Data
        deals_data = fetch_deals_from_api(API_URL)

        if deals_data:
            # 3. Insert Data
            print("\nInserting data into database...")
            inserted_count = insert_deals_data(connection, deals_data)
            print(f"Data insertion complete. {inserted_count} new records inserted.")

            # 4. Analyze Data
            if inserted_count > 0 or connection.execute("SELECT COUNT(*) FROM deals").fetchone()[0] > 0:
                 # วิเคราะห์ถ้ามีการเพิ่มข้อมูลใหม่ หรือมีข้อมูลเดิมอยู่ในฐานข้อมูลแล้ว
                 analyze_deals_data(connection)
            else:
                 print("\nNo data available in the database for analysis.")

        else:
            print("Failed to fetch data from API. Cannot proceed with insertion and analysis.")

        # 5. Close connection
        connection.close()
        print("\nDatabase connection closed.")
    else:
        print("Failed to initialize database. Exiting.")