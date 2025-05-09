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
