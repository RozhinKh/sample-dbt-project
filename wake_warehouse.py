#!/usr/bin/env python
"""Wake up Snowflake warehouse before running dbt tests"""
import snowflake.connector
import sys

try:
    conn = snowflake.connector.connect(
        account='GWAYEQA-CEB31664',
        user='AWENDELA',
        password='Xarsedasdvp1!1',
        warehouse='SNOWFLAKE_LEARNING_WH',
        database='BAIN_ANALYTICS',
        schema='DEV'
    )

    cursor = conn.cursor()

    # Simple query to wake up warehouse
    print("Waking up warehouse...")
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    print(f"Warehouse ready! Result: {result}")

    cursor.close()
    conn.close()

    print("SUCCESS - Warehouse is warmed up and ready!")
    sys.exit(0)

except Exception as e:
    print(f"ERROR: {str(e)}")
    sys.exit(1)
