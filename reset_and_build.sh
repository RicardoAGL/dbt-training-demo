#!/usr/bin/env bash
# Reset the DuckDB database and rebuild from scratch.
# Run this before the demo to ensure a clean state.

set -euo pipefail

DB_FILE="jaffle_shop.duckdb"

echo "Cleaning previous build artifacts..."
rm -f "$DB_FILE" "$DB_FILE.wal"
dbt clean 2>/dev/null || true

echo "Seeding raw data..."
dbt seed --select identity

echo "Building identity models..."
dbt build --select identity

echo ""
echo "Done! All identity models built and tested."
echo "Open DuckDB: python3 -c \"import duckdb; duckdb.connect('$DB_FILE').execute('SELECT 1')\""
