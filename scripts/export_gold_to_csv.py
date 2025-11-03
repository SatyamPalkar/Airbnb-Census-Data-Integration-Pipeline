"""
Export Gold Layer tables to CSV files
Exports analytical models from PostgreSQL gold schema to CSV for reporting and visualization.
"""

import pandas as pd
import os
from sqlalchemy import create_engine
from pathlib import Path

# Database connection (update with your credentials)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'airbnb_census_db',
    'user': 'airflow_user',
    'password': '******'  # Update with your password
}

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / 'data'
OUTPUT_DIR.mkdir(exist_ok=True)

# Tables to export from gold schema
GOLD_TABLES = [
    'airbnb_census_gold_joined',
    'dm_lga_summary'
]

def export_table_to_csv(engine, schema, table_name, output_path):
    """Export a PostgreSQL table to CSV."""
    query = f"SELECT * FROM {schema}.{table_name}"
    print(f"📊 Exporting {schema}.{table_name}...")
    
    try:
        df = pd.read_sql_query(query, engine)
        csv_path = output_path / f"{table_name}.csv"
        df.to_csv(csv_path, index=False)
        print(f"✅ Exported {len(df):,} rows to {csv_path}")
        return csv_path
    except Exception as e:
        print(f"❌ Error exporting {table_name}: {str(e)}")
        return None

def main():
    """Main export function."""
    print("="*80)
    print("EXPORTING GOLD LAYER TO CSV")
    print("="*80)
    
    # Create database connection
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    
    try:
        engine = create_engine(connection_string)
        print(f"✅ Connected to database: {DB_CONFIG['database']}")
    except Exception as e:
        print(f"❌ Failed to connect to database: {str(e)}")
        return
    
    # Export each table
    exported_files = []
    for table in GOLD_TABLES:
        csv_path = export_table_to_csv(engine, 'gold', table, OUTPUT_DIR)
        if csv_path:
            exported_files.append(csv_path)
    
    print("\n" + "="*80)
    print("EXPORT COMPLETE")
    print("="*80)
    print(f"📁 Output directory: {OUTPUT_DIR}")
    print(f"📄 Files exported: {len(exported_files)}")
    for file in exported_files:
        print(f"   - {file.name}")

if __name__ == "__main__":
    main()

