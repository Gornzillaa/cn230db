"""
CN230 Project: Data Pipeline and Analysis using SQLite and Public API

This script demonstrates the process of:
1. Fetching data from a public API (CheapShark API in this case).
2. Storing the fetched data into a SQLite database.
3. Performing data analytics on the stored data using SQL queries.

Database Explanation:
ฐานข้อมูลที่ใช้ในโปรเจกต์นี้คือ SQLite ซึ่งเป็นระบบจัดการฐานข้อมูลแบบไฟล์ (file-based database)
ข้อมูลที่ดึงมาจาก CheapShark API จะถูกจัดเก็บไว้ในตารางชื่อ 'deals'
แต่ละแถวในตาราง 'deals' แทนข้อมูลดีลเกม 1 รายการ โดยมีคอลัมน์ต่างๆ เช่น ชื่อเกม ราคา คะแนนรีวิว และส่วนลด
เราจะใช้คำสั่ง SQL เพื่อสอบถามและวิเคราะห์ข้อมูลในตารางนี้ เช่น หาดีลที่มีส่วนลดสูงสุด หรือดีลที่มีคะแนนรีวิวดี

Author: [ชยวัฒน์ กาญจนะแก้ว 6610685122]
"""

import sqlite3
import requests
import json
import datetime # แปลง timestamp เป็นวันที่

# --- กำหนดค่าคงที่ ---
DATABASE_NAME = "cheapshark_deals.db"
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
                dealID TEXT UNIQUE, -- dealID 
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

# --- Helper function สำหรับแสดงผลเป็นตาราง ---
def print_table(headers, data, col_widths=None):
    """
    แสดงข้อมูลในรูปแบบตาราง

    headers: List ของชื่อคอลัมน์ (strings)
    data: List ของ Tuple หรือ List ของ List (แต่ละ Tuple/List คือ 1 แถวข้อมูล)
    col_widths: Optional List ของความกว้างที่ต้องการสำหรับแต่ละคอลัมน์
    """
    if not headers:
        print("No headers provided.")
        return
    if not data:
        print("No data to display.")
        return

    # คำนวณความกว้างของแต่ละคอลัมน์ ถ้าไม่ได้กำหนดมา
    if col_widths is None:
        col_widths = [len(h) for h in headers]
        for row in data:
            for i, item in enumerate(row):
                # แปลงข้อมูลเป็น string ก่อนวัดความยาว
                item_str = str(item) if item is not None else "NULL"
                if i < len(col_widths): # ตรวจสอบขอบเขต
                    col_widths[i] = max(col_widths[i], len(item_str))
                else: # กรณีมีคอลัมน์ในข้อมูลมากกว่าใน headers (ไม่ควรเกิดขึ้นถ้า query ถูกต้อง)
                    col_widths.append(len(item_str))

    # ปรับความกว้างขั้นต่ำ (เผื่อกรณีข้อมูลสั้นมาก)
    min_width = 5
    col_widths = [max(w, min_width) for w in col_widths]

    # สร้างรูปแบบสำหรับแต่ละแถว
    format_string = " | ".join([f"{{:<{w}}}" for w in col_widths]) # {:<w} คือ จัดชิดซ้าย กว้าง w

    # แสดง Header
    print("-" * (sum(col_widths) + (len(col_widths) - 1) * 3 + 2)) # เส้นคั่นด้านบน
    print(format_string.format(*headers))
    print("-" * (sum(col_widths) + (len(col_widths) - 1) * 3 + 2)) # เส้นคั่นระหว่าง Header กับ Data

    # แสดง Data
    for row in data:
        # แปลงข้อมูลในแถวเป็น string และจัดการค่า None
        formatted_row = [str(item) if item is not None else "NULL" for item in row]
        # ตัดข้อความที่ยาวเกินความกว้างคอลัมน์ (ป้องกันตารางเบี้ยว)
        truncated_row = [item[:col_widths[i]] if i < len(col_widths) else item for i, item in enumerate(formatted_row)]
        print(format_string.format(*truncated_row))

    print("-" * (sum(col_widths) + (len(col_widths) - 1) * 3 + 2)) # เส้นคั่นด้านล่าง
    print("\n") # บรรทัดว่างหลังตาราง


# --- ฟังก์ชันสำหรับทำ Data Analytics ด้วย SQL (ปรับปรุงการแสดงผล) ---
def analyze_deals_data(conn):
    """วิเคราะห์ข้อมูลดีลเกมในฐานข้อมูลด้วย SQL และแสดงผลเป็นตาราง"""
    if not conn:
        print("Cannot analyze data: No connection.")
        return

    cursor = conn.cursor()
    print("\n--- Data Analysis Results ---")

    try:
        # 1: นับจำนวนดีลทั้งหมด
        print("1. Total number of deals stored:")
        cursor.execute("SELECT COUNT(*) FROM deals")
        total_deals = cursor.fetchone() # fetchone() คืนค่าเป็น Tuple
        print_table(["Total Deals"], [total_deals]) # ใส่ผลลัพธ์ใน List ของ List/Tuple

        # 2: แสดงดีล 5 อันดับแรกที่มีส่วนลด (savings) สูงสุด
        print("2. Top 5 deals with highest savings:")
        cursor.execute("SELECT title, savings, salePrice, normalPrice FROM deals ORDER BY savings DESC LIMIT 5")
        top_savings_deals = cursor.fetchall()
        # กำหนดความกว้างคอลัมน์เองเพื่อให้ title ไม่ยาวเกินไป
        print_table(["Title", "Savings (%)", "Sale Price ($)", "Normal Price ($)"], top_savings_deals, col_widths=[40, 12, 15, 15])


        # 3: แสดงดีล 5 อันดับแรกที่มีคะแนน Metacritic สูงสุด
        print("3. Top 5 deals with highest Metacritic Score:")
        cursor.execute("SELECT title, metacriticScore, salePrice FROM deals WHERE metacriticScore IS NOT NULL ORDER BY metacriticScore DESC LIMIT 5")
        top_metacritic_deals = cursor.fetchall()
        print_table(["Title", "Metacritic Score", "Sale Price ($)"], top_metacritic_deals, col_widths=[40, 18, 15])


        # 4: แสดงดีล 5 อันดับแรกที่มี Steam Rating Percent สูงสุด (และมีจำนวนรีวิวมากพอสมควร)
        print("4. Top 5 deals with highest Steam Rating Percent (min 1000 reviews):")
        cursor.execute("SELECT title, steamRatingText, steamRatingPercent, steamRatingCount, salePrice FROM deals WHERE steamRatingPercent IS NOT NULL AND steamRatingCount >= 1000 ORDER BY steamRatingPercent DESC, steamRatingCount DESC LIMIT 5")
        top_steam_rating_deals = cursor.fetchall()
        print_table(["Title", "Steam Rating Text", "Steam Rating (%)", "Steam Reviews", "Sale Price ($)"], top_steam_rating_deals, col_widths=[40, 20, 18, 15, 15])


        # 5: แสดงค่าเฉลี่ย Steam Rating Percent ของดีลทั้งหมด
        print("5. Average Steam Rating Percent of all deals:")
        cursor.execute("SELECT AVG(steamRatingPercent) FROM deals WHERE steamRatingPercent IS NOT NULL")
        avg_steam_rating = cursor.fetchone()
        if avg_steam_rating and avg_steam_rating[0] is not None:
             print_table(["Average Steam Rating (%)"], [[f"{avg_steam_rating[0]:.2f}"]]) # จัดรูปแบบทศนิยมและใส่ใน List ของ List
        else:
             print("   No Steam Rating Percent data available for average calculation.")


        # 6: แสดงจำนวนดีลแยกตาม Steam Rating Text
        print("6. Number of deals by Steam Rating Text:")
        cursor.execute("SELECT steamRatingText, COUNT(*) FROM deals WHERE steamRatingText IS NOT NULL GROUP BY steamRatingText ORDER BY COUNT(*) DESC")
        rating_counts = cursor.fetchall()
        print_table(["Steam Rating Text", "Number of Deals"], rating_counts)
        

        # 7: แสดงดีลที่มี Metacritic Score สูงกว่าค่าเฉลี่ย Metacritic Score ทั้งหมด
        print("7. Deals with Metacritic Score above average:")
        cursor.execute("""
            SELECT title, metacriticScore, salePrice
            FROM deals
            WHERE metacriticScore IS NOT NULL
              AND metacriticScore > (SELECT AVG(metacriticScore) FROM deals WHERE metacriticScore IS NOT NULL)
            ORDER BY metacriticScore DESC
            LIMIT 10;
        """)
        above_average_metacritic = cursor.fetchall()
        print_table(["Title", "Metacritic Score", "Sale Price ($)"], above_average_metacritic, col_widths=[40, 18, 15])


        # 8: แสดงดีลที่มี Steam Rating Percent สูงกว่าค่าเฉลี่ย Steam Rating Percent ของดีลที่มี Steam Rating Text เดียวกัน
        print("8. Deals with Steam Rating Percent above average for their rating text category:")
        cursor.execute("""
            WITH AvgRatings AS (
                SELECT
                    steamRatingText,
                    AVG(steamRatingPercent) as avg_percent
                FROM deals
                WHERE steamRatingText IS NOT NULL
                GROUP BY steamRatingText
            )
            SELECT
                d.title,
                d.steamRatingText,
                d.steamRatingPercent,
                ar.avg_percent
            FROM deals d
            JOIN AvgRatings ar ON d.steamRatingText = ar.steamRatingText
            WHERE d.steamRatingPercent IS NOT NULL
              AND d.steamRatingPercent > ar.avg_percent
            ORDER BY d.steamRatingPercent DESC
            LIMIT 10;
        """)
        above_average_steam_rating = cursor.fetchall()
        # ปรับการแสดงผล avg_percent ให้มีทศนิยม
        formatted_data = [(row[0], row[1], row[2], f"{row[3]:.2f}") for row in above_average_steam_rating]
        print_table(["Title", "Steam Rating Text", "Steam Rating (%)", "Avg for Category (%)"], formatted_data, col_widths=[40, 20, 18, 20])


        # 9: แสดงดีลที่มี Metacritic Score สูง (>= 85) และมี Steam Rating Percent สูง (>= 90) และมีส่วนลด (savings) มากกว่า 70%
        print("9. High-rated deals with significant savings:")
        cursor.execute("""
            SELECT title, metacriticScore, steamRatingPercent, savings, salePrice
            FROM deals
            WHERE metacriticScore IS NOT NULL AND metacriticScore >= 85
              AND steamRatingPercent IS NOT NULL AND steamRatingPercent >= 90
              AND savings IS NOT NULL AND savings >= 70
            ORDER BY savings DESC, metacriticScore DESC
            LIMIT 10;
        """)
        high_rated_deals = cursor.fetchall()
        # ปรับการแสดงผล savings และ salePrice ให้มีทศนิยม
        formatted_data = [(row[0], row[1], row[2], f"{row[3]:.2f}", f"{row[4]:.2f}") for row in high_rated_deals]
        print_table(["Title", "Meta Score", "Steam (%)", "Savings (%)", "Sale Price ($)"], formatted_data, col_widths=[40, 12, 12, 12, 15])


        # 10: แสดงดีลที่มีจำนวนรีวิว Steam (steamRatingCount) อยู่ในช่วง Top 10%
        print("10. Deals with Steam Rating Count in the top 10%:")
        cursor.execute("""
            SELECT MIN(steamRatingCount)
            FROM (
                SELECT steamRatingCount
                FROM deals
                WHERE steamRatingCount IS NOT NULL
                ORDER BY steamRatingCount DESC
                LIMIT (SELECT CAST(COUNT(*) * 0.1 AS INTEGER) FROM deals WHERE steamRatingCount IS NOT NULL)
            ) AS Top10PercentCounts;
        """)
        min_count_for_top10 = cursor.fetchone()[0]

        if min_count_for_top10 is not None:
            print(f"   Minimum review count for top 10%: {min_count_for_top10}")
            cursor.execute("""
                SELECT title, steamRatingCount, steamRatingText, steamRatingPercent, salePrice
                FROM deals
                WHERE steamRatingCount IS NOT NULL
                  AND steamRatingCount >= ?
                ORDER BY steamRatingCount DESC
                LIMIT 10;
            """, (min_count_for_top10,))
            top_review_count_deals = cursor.fetchall()
            # ปรับการแสดงผล percent และ price
            formatted_data = [(row[0], row[1], row[2], f"{row[3]:.2f}", f"{row[4]:.2f}") for row in top_review_count_deals]
            print_table(["Title", "Reviews", "Steam Rating Text", "Steam Rating (%)", "Sale Price ($)"], formatted_data, col_widths=[40, 10, 20, 18, 15])
        else:
            print("   Not enough data to calculate top 10% review count.")


        # 11: แสดงดีลที่ออกก่อนปี 2015 และยังมีดีลอยู่
        timestamp_2015 = 1420070400
        print("11. Deals released before 2015:")
        cursor.execute("""
            SELECT title, releaseDate, salePrice
            FROM deals
            WHERE releaseDate IS NOT NULL
              AND releaseDate < ?
            ORDER BY releaseDate ASC
            LIMIT 10;
        """, (timestamp_2015,))
        old_deals = cursor.fetchall()
        if old_deals:
            # แปลง timestamp เป็นวันที่ใน Python ก่อนแสดงผล
            formatted_data = []
            for title, release_ts, price in old_deals:
                 release_date_str = datetime.datetime.fromtimestamp(release_ts).strftime('%d-%m-%Y')
                 formatted_data.append((title, release_date_str, f"{price:.2f}"))
            print_table(["Title", "Release Date", "Sale Price ($)"], formatted_data, col_widths=[40, 15, 15])
        else:
            print("   No deals found released before 2015.")


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
