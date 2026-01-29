#!/usr/bin/env python3
"""Extract report from FACT_CASHFLOW_SUMMARY for benchmarking"""

import json
import os
import sys
from datetime import datetime
import time

def main():
    output_dir = "benchmark/candidate"
    os.makedirs(output_dir, exist_ok=True)

    try:
        from snowflake.connector import connect

        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_USER')
        password = os.getenv('SNOWFLAKE_PASSWORD')

        if not all([account, user, password]):
            raise ValueError("Missing Snowflake credentials")

        # Connect to Snowflake
        query_start = time.time()
        conn = connect(
            account=account,
            user=user,
            password=password,
            warehouse='COMPUTE_WH',
            database='BAIN_ANALYTICS',
            schema='DEV'
        )

        # Execute query
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM FACT_CASHFLOW_SUMMARY ORDER BY portfolio_id, cashflow_month, cashflow_type")

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        query_time = time.time() - query_start

        cursor.close()
        conn.close()

        # Convert to dictionaries
        data = [dict(zip(columns, row)) for row in rows]

        # Create report
        report = {
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'row_count': len(data),
                'table': 'FACT_CASHFLOW_SUMMARY',
                'schema': 'DEV',
                'database': 'BAIN_ANALYTICS',
                'query_execution_time_seconds': round(query_time, 4),
                'rows_affected': len(data)
            },
            'data': data
        }

        # Save report
        output_file = os.path.join(output_dir, 'report.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)

        print(f"[OK] Report generated: {len(data)} rows | Time: {query_time:.2f}s")
        return 0

    except ImportError:
        print("[ERROR] snowflake-connector-python not installed")
        return 1

    except Exception as e:
        print(f"[ERROR] {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
