#!/bin/bash
set -e

echo "=========================================="
echo "🚀 dbt Pipeline - Clean Execution"
echo "=========================================="
echo ""

# Step 1: Dependencies
echo "📦 Installing dbt packages..."
dbt deps --quiet
echo "✅ Done"
echo ""

# Step 2: Seeds
echo "📊 Loading seed data..."
dbt seed --full-refresh --quiet
echo "✅ Done"
echo ""

# Step 3: Models
echo "🏗️  Building models..."
dbt run --quiet
echo "✅ Done"
echo ""

# Step 4: Tests
echo "🧪 Running tests..."
dbt test --quiet
echo "✅ Done"
echo ""

# Step 5: Generate report
echo "📄 Generating report..."
mkdir -p benchmark/candidate

# Use dbt to compile and execute extract.sql via Python
python3 << 'EOF'
import json
import os
from snowflake.connector import connect

# Get credentials from environment
account = os.getenv('SNOWFLAKE_ACCOUNT')
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')

if not all([account, user, password]):
    raise ValueError("Missing Snowflake credentials in environment variables")

# Connect to Snowflake
conn = connect(
    account=account,
    user=user,
    password=password,
    warehouse='COMPUTE_WH',
    database='BAIN_ANALYTICS',
    schema='DEV'
)

# Read and execute query
with open('benchmark/extract.sql', 'r') as f:
    query = f.read()

cursor = conn.cursor()
cursor.execute(query)

# Fetch all results
columns = [desc[0] for desc in cursor.description]
rows = cursor.fetchall()
cursor.close()
conn.close()

# Convert to list of dicts
records = [dict(zip(columns, row)) for row in rows]

# Write JSON report
report = {
    'metadata': {
        'row_count': len(records),
        'columns': columns,
        'timestamp': __import__('datetime').datetime.now().isoformat()
    },
    'data': records
}

with open('benchmark/candidate/report.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"✅ Report generated: {len(records)} rows")
EOF

echo "✅ Done"
echo ""

echo "=========================================="
echo "✅ Pipeline Complete!"
echo "=========================================="
echo ""
echo "📊 Report location:"
echo "   benchmark/candidate/report.json"
echo ""
echo "📈 Next steps:"
echo "   1. Review benchmark/candidate/report.json"
echo "   2. Copy to baseline/ for comparison"
echo ""
