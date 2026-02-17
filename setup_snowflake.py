import snowflake.connector
import sys

# Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        account='GWAYEQA-CEB31664',
        user='AWENDELA',
        password='Xarsedasdvp1!1'
    )

    cursor = conn.cursor()

    print("=" * 60)
    print("CHECKING SNOWFLAKE ACCOUNT...")
    print("=" * 60)

    # Check available warehouses
    print("\n1. Checking available warehouses...")
    cursor.execute("SHOW WAREHOUSES")
    warehouses = cursor.fetchall()

    if warehouses:
        print(f"   Found {len(warehouses)} warehouse(s):")
        for wh in warehouses:
            print(f"   - {wh[0]} (State: {wh[1]}, Size: {wh[2]})")
        warehouse_name = warehouses[0][0]  # Use the first warehouse
        print(f"\n   Will use warehouse: {warehouse_name}")
    else:
        print("   ERROR: No warehouses found!")
        sys.exit(1)

    # Set warehouse
    cursor.execute(f"USE WAREHOUSE {warehouse_name}")

    # Check if database exists
    print("\n2. Checking if BAIN_ANALYTICS database exists...")
    cursor.execute("SHOW DATABASES LIKE 'BAIN_ANALYTICS'")
    db_exists = len(cursor.fetchall()) > 0

    if not db_exists:
        print("   Database doesn't exist. Creating BAIN_ANALYTICS database...")
        cursor.execute("CREATE DATABASE BAIN_ANALYTICS")
        print("   [OK] Database created successfully!")
    else:
        print("   [OK] Database already exists")

    # Use the database
    cursor.execute("USE DATABASE BAIN_ANALYTICS")

    # Check if schema exists
    print("\n3. Checking if DEV schema exists...")
    cursor.execute("SHOW SCHEMAS LIKE 'DEV'")
    schema_exists = len(cursor.fetchall()) > 0

    if not schema_exists:
        print("   Schema doesn't exist. Creating DEV schema...")
        cursor.execute("CREATE SCHEMA DEV")
        print("   [OK] Schema created successfully!")
    else:
        print("   [OK] Schema already exists")

    print("\n" + "=" * 60)
    print("SETUP COMPLETE!")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  Database: BAIN_ANALYTICS")
    print(f"  Schema: DEV")
    print(f"  Warehouse: {warehouse_name}")
    print(f"\nWarehouse name saved to: warehouse_name.txt")

    # Save warehouse name to file
    with open('warehouse_name.txt', 'w') as f:
        f.write(warehouse_name)

    cursor.close()
    conn.close()

except Exception as e:
    print(f"\nERROR: {str(e)}")
    sys.exit(1)
