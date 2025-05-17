# CN230 Project: Data Pipeline and Analysis using SQLite and Public API

This script demonstrates the process of:
1. Fetching data from a public API (CheapShark API in this case).  
2. Storing the fetched data into a SQLite database.  
3. Performing data analytics on the stored data using SQL queries.  

Database Explanation:  
ฐานข้อมูลที่ใช้ในโปรเจกต์นี้คือ SQLite ซึ่งเป็นระบบจัดการฐานข้อมูลแบบไฟล์ (file-based database)  
ข้อมูลที่ดึงมาจาก CheapShark API จะถูกจัดเก็บไว้ในตารางชื่อ 'deals'  
แต่ละแถวในตาราง 'deals' แทนข้อมูลดีลเกม 1 รายการ โดยมีคอลัมน์ต่างๆ เช่น ชื่อเกม ราคา คะแนนรีวิว และส่วนลด  
เราจะใช้คำสั่ง SQL เพื่อสอบถามและวิเคราะห์ข้อมูลในตารางนี้ เช่น หาดีลที่มีส่วนลดสูงสุด หรือดีลที่มีคะแนนรีวิวดี  

## Author: [ชยวัฒน์ กาญจนะแก้ว 6610685122]  

## Data Analysis  

 1:  นับจำนวนดีลทั้งหมด      
 2: แสดงดีล 5 อันดับแรกที่มีส่วนลด (savings) สูงสุด  
 3: แสดงดีล 5 อันดับแรกที่มีคะแนน Metacritic สูงสุด  
 4: แสดงดีล 5 อันดับแรกที่มี Steam Rating Percent สูงสุด (และมีจำนวนรีวิวมากพอสมควร)  
 5: แสดงค่าเฉลี่ย Steam Rating Percent ของดีลทั้งหมด  
 6: แสดงจำนวนดีลแยกตาม Steam Rating Text  
 7: แสดงดีลที่มี Metacritic Score สูงกว่าค่าเฉลี่ย Metacritic Score ทั้งหมด  
 8: แสดงดีลที่มี Steam Rating Percent สูงกว่าค่าเฉลี่ย Steam Rating Percent ของดีลที่มี Steam Rating Text เดียวกัน  
 9: แสดงดีลที่มี Metacritic Score สูง (>= 85) และมี Steam Rating Percent สูง (>= 90) และมีส่วนลด (savings) มากกว่า 70%  
 10: แสดงดีลที่มีจำนวนรีวิว Steam (steamRatingCount) อยู่ในช่วง Top 10%  
 11: แสดงดีลที่ออกก่อนปี 2015 และยังมีดีลอยู่  
