"""Generate a SQLite test database with synthetic data across multiple table groups.

SQLite doesn't support schemas, so we use table name prefixes:
  ecommerce_customers, ecommerce_orders, etc.
  hr_departments, hr_employees, etc.
  analytics_page_views, analytics_ab_test_results

Usage:
    python -m tests.seed_data          # creates tests/test_data.db
    python -m tests.seed_data /tmp/x.db  # custom path
"""

from __future__ import annotations

import os
import random
import sqlite3
import sys
from datetime import date, timedelta

SEED = 42
random.seed(SEED)

# ─── Helpers ──────────────────────────────────────────────────────────────

REGIONS = ["North", "South", "East", "West", "Central", "International"]
SEGMENTS = ["Consumer", "Corporate", "Home Office"]
CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports", "Food", "Toys", "Office"]
ORDER_STATUSES = ["Pending", "Shipped", "Delivered", "Returned"]
DEPARTMENTS = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Legal", "Support"]
LOCATIONS = ["New York", "San Francisco", "London", "Berlin", "Tokyo", "Sydney", "Toronto", "Singapore"]
PAGE_URLS = [f"/page/{i}" for i in range(1, 16)]
AB_VARIANTS = ["control", "variant_a", "variant_b", "variant_c", "variant_d", "variant_e"]
AB_SEGMENTS = ["new_users", "returning", "mobile", "desktop", "tablet"]

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank",
    "Iris", "Jack", "Karen", "Leo", "Mona", "Nick", "Olivia", "Pat",
    "Quinn", "Rosa", "Steve", "Tina", "Uma", "Vic", "Wendy", "Xander",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
]
PRODUCT_NAMES = [
    "Widget", "Gadget", "Doohickey", "Thingamajig", "Contraption",
    "Gizmo", "Apparatus", "Device", "Mechanism", "Instrument",
]
REVIEWER_NAMES = [f"Reviewer_{i}" for i in range(1, 11)]


def random_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_product_name() -> str:
    adj = random.choice(["Super", "Ultra", "Mega", "Pro", "Mini", "Max", "Elite", "Basic"])
    return f"{adj} {random.choice(PRODUCT_NAMES)}"


# ─── Schema creation ─────────────────────────────────────────────────────

def create_tables(cur: sqlite3.Cursor) -> None:
    # ── ecommerce ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce_customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            region TEXT NOT NULL,
            segment TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL NOT NULL,
            status TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES ecommerce_customers(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce_order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES ecommerce_orders(id),
            FOREIGN KEY (product_id) REFERENCES ecommerce_products(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce_monthly_revenue (
            month TEXT NOT NULL,
            revenue REAL NOT NULL,
            costs REAL NOT NULL,
            profit REAL NOT NULL
        )
    """)

    # ── hr ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hr_departments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            location TEXT NOT NULL,
            budget REAL NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hr_employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            department_id INTEGER NOT NULL,
            salary REAL NOT NULL,
            hire_date TEXT NOT NULL,
            FOREIGN KEY (department_id) REFERENCES hr_departments(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hr_performance_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            review_date TEXT NOT NULL,
            score REAL NOT NULL,
            reviewer TEXT,
            FOREIGN KEY (employee_id) REFERENCES hr_employees(id)
        )
    """)

    # ── analytics ──
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics_page_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            view_date TEXT NOT NULL,
            page_url TEXT NOT NULL,
            view_count INTEGER NOT NULL,
            avg_duration_seconds REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics_ab_test_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            variant TEXT NOT NULL,
            segment TEXT NOT NULL,
            conversion_rate REAL NOT NULL,
            avg_revenue REAL NOT NULL
        )
    """)


# ─── Data insertion ──────────────────────────────────────────────────────

def seed_data(cur: sqlite3.Cursor) -> None:
    # ── ecommerce_customers (200 rows) ──
    customers = []
    for i in range(200):
        name = random_name()
        email = f"{name.lower().replace(' ', '.')}_{i}@example.com"
        region = random.choice(REGIONS)
        segment = random.choice(SEGMENTS)
        created_at = random_date(date(2020, 1, 1), date(2025, 6, 30))
        customers.append((name, email, region, segment, created_at))
    cur.executemany(
        "INSERT INTO ecommerce_customers (name, email, region, segment, created_at) VALUES (?,?,?,?,?)",
        customers,
    )

    # ── ecommerce_products (50 rows) ──
    products = []
    for _ in range(50):
        name = random_product_name()
        category = random.choice(CATEGORIES)
        price = round(random.uniform(5, 500), 2)
        stock = random.randint(0, 1000)
        products.append((name, category, price, stock))
    cur.executemany(
        "INSERT INTO ecommerce_products (name, category, price, stock) VALUES (?,?,?,?)",
        products,
    )

    # ── ecommerce_orders (1000 rows) ──
    orders = []
    for _ in range(1000):
        customer_id = random.randint(1, 200)
        order_date = random_date(date(2023, 1, 1), date(2025, 12, 31))
        total_amount = round(random.uniform(10, 1000), 2)
        status = random.choice(ORDER_STATUSES)
        orders.append((customer_id, order_date, total_amount, status))
    cur.executemany(
        "INSERT INTO ecommerce_orders (customer_id, order_date, total_amount, status) VALUES (?,?,?,?)",
        orders,
    )

    # ── ecommerce_order_items (2500 rows) ──
    items = []
    for _ in range(2500):
        order_id = random.randint(1, 1000)
        product_id = random.randint(1, 50)
        quantity = random.randint(1, 20)
        unit_price = round(random.uniform(5, 300), 2)
        items.append((order_id, product_id, quantity, unit_price))
    cur.executemany(
        "INSERT INTO ecommerce_order_items (order_id, product_id, quantity, unit_price) VALUES (?,?,?,?)",
        items,
    )

    # ── ecommerce_monthly_revenue (36 rows — 3 years) ──
    monthly = []
    for year in range(2023, 2026):
        for month in range(1, 13):
            m = date(year, month, 1).isoformat()
            revenue = round(random.uniform(30000, 80000), 2)
            costs = round(revenue * random.uniform(0.4, 0.7), 2)
            profit = round(revenue - costs, 2)
            monthly.append((m, revenue, costs, profit))
    cur.executemany(
        "INSERT INTO ecommerce_monthly_revenue (month, revenue, costs, profit) VALUES (?,?,?,?)",
        monthly,
    )

    # ── hr_departments (8 rows) ──
    depts = []
    for i, (name, loc) in enumerate(zip(DEPARTMENTS, LOCATIONS)):
        budget = round(random.uniform(500000, 5000000), 2)
        depts.append((name, loc, budget))
    cur.executemany(
        "INSERT INTO hr_departments (name, location, budget) VALUES (?,?,?)",
        depts,
    )

    # ── hr_employees (500 rows) ──
    employees = []
    for _ in range(500):
        name = random_name()
        dept_id = random.randint(1, 8)
        salary = round(random.uniform(40000, 200000), 2)
        hire_date = random_date(date(2015, 1, 1), date(2025, 6, 30))
        employees.append((name, dept_id, salary, hire_date))
    cur.executemany(
        "INSERT INTO hr_employees (name, department_id, salary, hire_date) VALUES (?,?,?,?)",
        employees,
    )

    # ── hr_performance_reviews (1000 rows, ~20% NULL reviewer) ──
    reviews = []
    for _ in range(1000):
        emp_id = random.randint(1, 500)
        review_date = random_date(date(2020, 1, 1), date(2025, 12, 31))
        score = round(random.uniform(1.0, 5.0), 1)
        reviewer = random.choice(REVIEWER_NAMES) if random.random() > 0.2 else None
        reviews.append((emp_id, review_date, score, reviewer))
    cur.executemany(
        "INSERT INTO hr_performance_reviews (employee_id, review_date, score, reviewer) VALUES (?,?,?,?)",
        reviews,
    )

    # ── analytics_page_views (730 rows — 2 years daily) ──
    views = []
    start = date(2024, 1, 1)
    for day_offset in range(730):
        d = (start + timedelta(days=day_offset)).isoformat()
        page = random.choice(PAGE_URLS)
        count = random.randint(50, 10000)
        duration = round(random.uniform(10, 300), 2) if random.random() > 0.05 else None
        views.append((d, page, count, duration))
    cur.executemany(
        "INSERT INTO analytics_page_views (view_date, page_url, view_count, avg_duration_seconds) VALUES (?,?,?,?)",
        views,
    )

    # ── analytics_ab_test_results (60 rows — 6 variants x 5 segments x 2 records) ──
    ab = []
    for variant in AB_VARIANTS:
        for segment in AB_SEGMENTS:
            conv = round(random.uniform(0.01, 0.15), 4)
            rev = round(random.uniform(5, 100), 2)
            ab.append((variant, segment, conv, rev))
            # second record with slight variation
            conv2 = round(conv + random.uniform(-0.02, 0.02), 4)
            rev2 = round(rev + random.uniform(-10, 10), 2)
            ab.append((variant, segment, max(0.001, conv2), max(1.0, rev2)))
    cur.executemany(
        "INSERT INTO analytics_ab_test_results (variant, segment, conversion_rate, avg_revenue) VALUES (?,?,?,?)",
        ab,
    )


# ─── Main ─────────────────────────────────────────────────────────────────

def create_test_db(db_path: str) -> str:
    """Create and seed the test database. Returns the path."""
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    create_tables(cur)
    seed_data(cur)
    conn.commit()

    # Verify row counts
    tables = {
        "ecommerce_customers": 200,
        "ecommerce_products": 50,
        "ecommerce_orders": 1000,
        "ecommerce_order_items": 2500,
        "ecommerce_monthly_revenue": 36,
        "hr_departments": 8,
        "hr_employees": 500,
        "hr_performance_reviews": 1000,
        "analytics_page_views": 730,
        "analytics_ab_test_results": 60,
    }
    for table, expected in tables.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        actual = cur.fetchone()[0]
        assert actual == expected, f"{table}: expected {expected}, got {actual}"

    conn.close()
    return db_path


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "test_data.db")
    create_test_db(path)
    print(f"Created test database at {path}")
