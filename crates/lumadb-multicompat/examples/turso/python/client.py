"""
LumaDB Turso/LibSQL Compatibility Layer - Python Client Example

This example demonstrates using LumaDB with the official libsql-experimental or libsql-client
Python packages. LumaDB exposes a compatible HTTP API.
"""

import os
import time
import libsql_experimental as libsql
from dataclasses import dataclass
from typing import List, Optional

# ===== Configuration =====

# Point to LumaDB's Turso endpoint
URL = os.getenv("LUMADB_TURSO_URL", "http://localhost:8000/turso")
AUTH_TOKEN = os.getenv("LUMADB_TOKEN", "luma-token-123")

class TursoClient:
    def __init__(self, url: str, auth_token: str):
        self.conn = libsql.connect(url, auth_token=auth_token)
        print(f"✓ Connected to {url}")

    def setup_schema(self):
        """Initialize database schema"""
        print("\n=== Setting up schema ===")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                stock INTEGER DEFAULT 0,
                description TEXT
            )
        """)
        
        # Clear existing data for demo
        self.conn.execute("DELETE FROM products")
        print("✓ Schema initialized")

    def insert_products(self, products: List[dict]):
        """Batch insert using transaction"""
        print(f"\n=== Inserting {len(products)} products ===")
        
        # Start explicit transaction
        self.conn.execute("BEGIN")
        
        try:
            stmt = "INSERT INTO products (name, price, stock, description) VALUES (?, ?, ?, ?)"
            
            for p in products:
                self.conn.execute(stmt, (
                    p['name'], 
                    p['price'], 
                    p.get('stock', 0), 
                    p.get('description')
                ))
            
            self.conn.execute("COMMIT")
            print("✓ Transaction committed")
            
        except Exception as e:
            self.conn.execute("ROLLBACK")
            print(f"✗ Transaction rolled back: {e}")
            raise

    def get_products_by_price_range(self, min_price: float, max_price: float) -> List[dict]:
        """Query with parameterized filtering"""
        print(f"\n=== Querying products between ${min_price} and ${max_price} ===")
        
        cursor = self.conn.execute(
            "SELECT * FROM products WHERE price >= ? AND price <= ? ORDER BY price DESC", 
            (min_price, max_price)
        )
        
        rows = cursor.fetchall()
        
        # Convert rows to dicts
        results = []
        for row in rows:
            # Row access by index or name depending on client version
            results.append({
                "id": row[0],
                "name": row[1],
                "price": row[2],
                "stock": row[3]
            })
            
        print(f"✓ Found {len(results)} products")
        return results

    def update_stock(self, product_id: int, new_stock: int):
        """Update single record"""
        print(f"\n=== Updating stock for product #{product_id} ===")
        
        self.conn.execute(
            "UPDATE products SET stock = ? WHERE id = ?",
            (new_stock, product_id)
        )
        print("✓ Stock updated")

    def complex_analytics(self):
        """Run complex aggregation query"""
        print("\n=== Running analytics ===")
        
        sql = """
            SELECT 
                COUNT(*) as total_products,
                AVG(price) as average_price,
                SUM(stock) as total_inventory,
                MAX(price) as most_expensive
            FROM products
        """
        
        row = self.conn.execute(sql).fetchone()
        
        print("Analytics Report:")
        print(f"  Total Products: {row[0]}")
        print(f"  Average Price:  ${row[1]:.2f}")
        print(f"  Total Stock:    {row[2]}")
        print(f"  Max Price:      ${row[3]:.2f}")

    def execute_batch(self, sql_statements: List[str]):
        """Execute multiple statements in a batch"""
        print(f"\n=== Executing batch of {len(sql_statements)} statements ===")
        
        # Note: Client support for batch varies, this simulates it
        for sql in sql_statements:
            self.conn.execute(sql)
            
        print("✓ Batch execution complete")

def main():
    try:
        client = TursoClient(URL, AUTH_TOKEN)
        
        # 1. Setup
        client.setup_schema()
        
        # 2. Insert data
        products = [
            {"name": "Laptop Pro", "price": 1299.99, "stock": 50, "description": "High performance"},
            {"name": "Wireless Mouse", "price": 29.99, "stock": 200, "description": "Ergonomic"},
            {"name": "4K Monitor", "price": 399.50, "stock": 30, "description": "Crystal clear"},
            {"name": "USB-C Cable", "price": 12.99, "stock": 500, "description": "Durable braid"},
            {"name": "Mechanical Keyboard", "price": 149.00, "stock": 45, "description": "Clicky switches"}
        ]
        client.insert_products(products)
        
        # 3. Query
        cheap_gadgets = client.get_products_by_price_range(0, 50)
        for p in cheap_gadgets:
            print(f"  - {p['name']}: ${p['price']}")
            
        # 4. Update
        if cheap_gadgets:
            client.update_stock(cheap_gadgets[0]['id'], 999)
            
        # 5. Analytics
        client.complex_analytics()
        
        # 6. Batch DDL
        client.execute_batch([
            "CREATE INDEX IF NOT EXISTS idx_price ON products(price)",
            "CREATE VIEW IF NOT EXISTS v_expensive AS SELECT * FROM products WHERE price > 100"
        ])

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
