import sqlite3
import os

db_path = 'data/stock_limit_up.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print('Tables:', [t[0] for t in tables])
    
    # Check limit_up_records
    cursor.execute("SELECT trade_date, COUNT(*) FROM limit_up_records GROUP BY trade_date ORDER BY trade_date DESC LIMIT 5")
    rows = cursor.fetchall()
    if rows:
        print('Data:')
        for row in rows:
            print(f'  {row[0]}: {row[1]} records')
    else:
        print('No data in limit_up_records')
    conn.close()
else:
    print(f'Database not found: {db_path}')
