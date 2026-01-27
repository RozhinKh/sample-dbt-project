#!/usr/bin/env python3
"""Expand seed data 10x for more realistic benchmarking"""

import csv
from datetime import datetime, timedelta

# 1. Expand cashflows data
print("Expanding cashflows data...")
with open('seeds/sample_cashflows.csv', 'r') as f:
    reader = csv.DictReader(f)
    cashflow_rows = list(reader)

expanded_cashflows = []
base_id = 1

for multiplier in range(10):
    for row in cashflow_rows:
        new_row = row.copy()
        new_row['cashflow_id'] = str(base_id)
        # Map to portfolio IDs 1-20
        new_row['portfolio_id'] = str((int(row['portfolio_id'].replace('PF', '')) - 1 + multiplier * 2) % 20 + 1)

        # Offset dates
        date_obj = datetime.fromisoformat(row['cashflow_date'].replace('Z', '+00:00'))
        new_date = date_obj + timedelta(days=365 * multiplier)
        new_row['cashflow_date'] = new_date.strftime('%Y-%m-%d')

        created_date = datetime.fromisoformat(row['created_at'].replace('Z', '+00:00'))
        new_created = created_date + timedelta(days=365 * multiplier)
        new_row['created_at'] = new_created.strftime('%Y-%m-%dT%H:%M:%SZ')

        updated_date = datetime.fromisoformat(row['updated_at'].replace('Z', '+00:00'))
        new_updated = updated_date + timedelta(days=365 * multiplier)
        new_row['updated_at'] = new_updated.strftime('%Y-%m-%dT%H:%M:%SZ')

        expanded_cashflows.append(new_row)
        base_id += 1

# Write expanded cashflows
with open('seeds/sample_cashflows.csv', 'w', newline='') as f:
    fieldnames = ['cashflow_id', 'portfolio_id', 'cashflow_type', 'cashflow_date', 'amount', 'currency', 'created_at', 'updated_at']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(expanded_cashflows)

print(f"  Cashflows: {len(cashflow_rows)} → {len(expanded_cashflows)} rows")

# 2. Expand portfolios data (create 20 portfolios from 8)
print("Expanding portfolios data...")
with open('seeds/sample_portfolios.csv', 'r') as f:
    reader = csv.DictReader(f)
    portfolio_rows = list(reader)

expanded_portfolios = []
portfolio_names = [
    "Global Equity Core", "US Large Cap Growth", "Investment Grade Credit", "High Yield Opportunities",
    "Multi-Asset Balanced", "Emerging Markets Equity", "Private Credit Fund I", "Real Assets Fund",
    "Tech Innovation Fund", "Healthcare Sector Focus", "Infrastructure Opportunities", "ESG Leaders",
    "Commodity Exposure", "Volatility Hedge Fund", "International Bonds", "Convertible Securities",
    "Small Cap Value", "Dividend Growth", "Merger Arbitrage", "Alternative Income"
]

portfolio_types = ["EQUITY", "EQUITY", "FIXED_INCOME", "FIXED_INCOME", "MULTI_ASSET", "EQUITY", "PRIVATE_CREDIT", "REAL_ASSETS"]
fund_ids = ["FD001", "FD002", "FD003", "FD004", "FD001", "FD002", "FD003", "FD004"]

for i in range(20):
    portfolio_id = i + 1
    new_portfolio = {
        'portfolio_id': f'PF{portfolio_id:03d}',
        'portfolio_name': portfolio_names[i],
        'portfolio_type': portfolio_types[i % len(portfolio_types)],
        'fund_id': fund_ids[i % len(fund_ids)],
        'inception_date': (datetime(2015, 1, 1) + timedelta(days=i*100)).strftime('%Y-%m-%d'),
        'status': 'ACTIVE',
        'aum_usd': str(100000000 + (i * 25000000))  # 100M to 575M range
    }
    expanded_portfolios.append(new_portfolio)

# Write expanded portfolios
with open('seeds/sample_portfolios.csv', 'w', newline='') as f:
    fieldnames = ['portfolio_id', 'portfolio_name', 'portfolio_type', 'fund_id', 'inception_date', 'status', 'aum_usd']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(expanded_portfolios)

print(f"  Portfolios: {len(portfolio_rows)} → {len(expanded_portfolios)} rows")
print(f"\nExpansion complete!")
print(f"  Total cashflows: {len(expanded_cashflows)}")
print(f"  Total portfolios: {len(expanded_portfolios)}")
print(f"  Covers: 20 portfolios across multiple years and fund types")
