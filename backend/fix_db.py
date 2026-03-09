import sqlite3
import os

db_path = 'data/stock_limit_up.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if current_status column exists
    cursor.execute("PRAGMA table_info(limit_up_records)")
    columns = [col[1] for col in cursor.fetchall()]
    print('Existing columns:', columns)
    
    if 'current_status' not in columns:
        print('Adding current_status column...')
        cursor.execute("ALTER TABLE limit_up_records ADD COLUMN current_status TEXT DEFAULT 'sealed'")
        conn.commit()
        print('Column added successfully!')
    else:
        print('current_status column already exists')
    
    # Also check for final_seal_time column
    if 'final_seal_time' not in columns:
        print('Adding final_seal_time column...')
        cursor.execute("ALTER TABLE limit_up_records ADD COLUMN final_seal_time TEXT")
        conn.commit()
        print('Column added successfully!')
    else:
        print('final_seal_time column already exists')
    
    conn.close()
else:
    print(f'Database not found: {db_path}')
