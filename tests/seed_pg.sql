-- Idempotent PostgreSQL seed script for sql_to_graph testing.
-- Creates 3 schemas with ~5000 rows of synthetic data.

-- ═══════════════════════════════════════════════════════════════════════
-- SCHEMA: ecommerce
-- ═══════════════════════════════════════════════════════════════════════

DROP SCHEMA IF EXISTS ecommerce CASCADE;
CREATE SCHEMA ecommerce;

CREATE TABLE ecommerce.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL,
    region VARCHAR(50) NOT NULL,
    segment VARCHAR(30) NOT NULL,
    created_at DATE NOT NULL
);

CREATE TABLE ecommerce.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    stock INTEGER NOT NULL
);

CREATE TABLE ecommerce.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES ecommerce.customers(id),
    order_date DATE NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL
);

CREATE TABLE ecommerce.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES ecommerce.orders(id),
    product_id INTEGER NOT NULL REFERENCES ecommerce.products(id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL
);

CREATE TABLE ecommerce.monthly_revenue (
    month DATE NOT NULL,
    revenue NUMERIC(12,2) NOT NULL,
    costs NUMERIC(12,2) NOT NULL,
    profit NUMERIC(12,2) NOT NULL
);

-- Seed customers (200 rows)
INSERT INTO ecommerce.customers (name, email, region, segment, created_at)
SELECT
    'Customer ' || i,
    'customer' || i || '@example.com',
    (ARRAY['North','South','East','West','Central','International'])[floor(random()*6+1)::int],
    (ARRAY['Consumer','Corporate','Home Office'])[floor(random()*3+1)::int],
    '2020-01-01'::date + (random() * 2000)::int
FROM generate_series(1, 200) AS s(i);

-- Seed products (50 rows)
INSERT INTO ecommerce.products (name, category, price, stock)
SELECT
    'Product ' || i,
    (ARRAY['Electronics','Clothing','Books','Home','Sports','Food','Toys','Office'])[floor(random()*8+1)::int],
    round((random() * 495 + 5)::numeric, 2),
    floor(random() * 1000)::int
FROM generate_series(1, 50) AS s(i);

-- Seed orders (1000 rows)
INSERT INTO ecommerce.orders (customer_id, order_date, total_amount, status)
SELECT
    floor(random() * 200 + 1)::int,
    '2023-01-01'::date + (random() * 1094)::int,
    round((random() * 990 + 10)::numeric, 2),
    (ARRAY['Pending','Shipped','Delivered','Returned'])[floor(random()*4+1)::int]
FROM generate_series(1, 1000);

-- Seed order_items (2500 rows)
INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price)
SELECT
    floor(random() * 1000 + 1)::int,
    floor(random() * 50 + 1)::int,
    floor(random() * 19 + 1)::int,
    round((random() * 295 + 5)::numeric, 2)
FROM generate_series(1, 2500);

-- Seed monthly_revenue (36 rows)
INSERT INTO ecommerce.monthly_revenue (month, revenue, costs, profit)
SELECT
    d::date,
    round((random() * 50000 + 30000)::numeric, 2) as rev,
    round((random() * 50000 + 30000)::numeric * (random() * 0.3 + 0.4), 2) as cost,
    0  -- will update below
FROM generate_series('2023-01-01'::date, '2025-12-01'::date, '1 month'::interval) AS s(d);

UPDATE ecommerce.monthly_revenue SET profit = revenue - costs;


-- ═══════════════════════════════════════════════════════════════════════
-- SCHEMA: hr
-- ═══════════════════════════════════════════════════════════════════════

DROP SCHEMA IF EXISTS hr CASCADE;
CREATE SCHEMA hr;

CREATE TABLE hr.departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    location VARCHAR(50) NOT NULL,
    budget NUMERIC(12,2) NOT NULL
);

CREATE TABLE hr.employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department_id INTEGER NOT NULL REFERENCES hr.departments(id),
    salary NUMERIC(10,2) NOT NULL,
    hire_date DATE NOT NULL
);

CREATE TABLE hr.performance_reviews (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES hr.employees(id),
    review_date DATE NOT NULL,
    score NUMERIC(3,1) NOT NULL,
    reviewer VARCHAR(100)  -- ~20% NULL to test null warnings
);

-- Seed departments (8 rows)
INSERT INTO hr.departments (name, location, budget) VALUES
    ('Engineering', 'San Francisco', 4500000),
    ('Sales', 'New York', 3200000),
    ('Marketing', 'London', 2100000),
    ('HR', 'Berlin', 1500000),
    ('Finance', 'Tokyo', 2800000),
    ('Operations', 'Sydney', 1900000),
    ('Legal', 'Toronto', 1700000),
    ('Support', 'Singapore', 1200000);

-- Seed employees (500 rows)
INSERT INTO hr.employees (name, department_id, salary, hire_date)
SELECT
    'Employee ' || i,
    floor(random() * 8 + 1)::int,
    round((random() * 160000 + 40000)::numeric, 2),
    '2015-01-01'::date + (random() * 3833)::int
FROM generate_series(1, 500) AS s(i);

-- Seed performance_reviews (1000 rows, ~20% NULL reviewer)
INSERT INTO hr.performance_reviews (employee_id, review_date, score, reviewer)
SELECT
    floor(random() * 500 + 1)::int,
    '2020-01-01'::date + (random() * 2190)::int,
    round((random() * 4 + 1)::numeric, 1),
    CASE WHEN random() > 0.2 THEN 'Reviewer_' || floor(random()*10+1)::int ELSE NULL END
FROM generate_series(1, 1000);


-- ═══════════════════════════════════════════════════════════════════════
-- SCHEMA: analytics
-- ═══════════════════════════════════════════════════════════════════════

DROP SCHEMA IF EXISTS analytics CASCADE;
CREATE SCHEMA analytics;

CREATE TABLE analytics.page_views (
    id SERIAL PRIMARY KEY,
    view_date DATE NOT NULL,
    page_url VARCHAR(200) NOT NULL,
    view_count INTEGER NOT NULL,
    avg_duration_seconds NUMERIC(6,2)  -- ~5% NULL
);

CREATE TABLE analytics.ab_test_results (
    id SERIAL PRIMARY KEY,
    variant VARCHAR(20) NOT NULL,
    segment VARCHAR(30) NOT NULL,
    conversion_rate NUMERIC(5,4) NOT NULL,
    avg_revenue NUMERIC(10,2) NOT NULL
);

-- Seed page_views (730 rows — 2 years daily)
INSERT INTO analytics.page_views (view_date, page_url, view_count, avg_duration_seconds)
SELECT
    '2024-01-01'::date + i,
    '/page/' || floor(random()*15+1)::int,
    floor(random() * 9950 + 50)::int,
    CASE WHEN random() > 0.05 THEN round((random() * 290 + 10)::numeric, 2) ELSE NULL END
FROM generate_series(0, 729) AS s(i);

-- Seed ab_test_results (60 rows — 6 variants x 5 segments x 2 records)
INSERT INTO analytics.ab_test_results (variant, segment, conversion_rate, avg_revenue)
SELECT
    v, s,
    round((random() * 0.14 + 0.01)::numeric, 4),
    round((random() * 95 + 5)::numeric, 2)
FROM
    unnest(ARRAY['control','variant_a','variant_b','variant_c','variant_d','variant_e']) AS v,
    unnest(ARRAY['new_users','returning','mobile','desktop','tablet']) AS s,
    generate_series(1, 2);


-- ═══════════════════════════════════════════════════════════════════════
-- ANALYZE for row count estimates
-- ═══════════════════════════════════════════════════════════════════════

ANALYZE ecommerce.customers;
ANALYZE ecommerce.products;
ANALYZE ecommerce.orders;
ANALYZE ecommerce.order_items;
ANALYZE ecommerce.monthly_revenue;
ANALYZE hr.departments;
ANALYZE hr.employees;
ANALYZE hr.performance_reviews;
ANALYZE analytics.page_views;
ANALYZE analytics.ab_test_results;
