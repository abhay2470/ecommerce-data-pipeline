-- ── Dimension Table: Products ─────────────────
CREATE TABLE IF NOT EXISTS dim_products (
    product_id      INTEGER PRIMARY KEY,
    title           VARCHAR(255),
    category        VARCHAR(100),
    price           NUMERIC(10,2),
    price_bucket    VARCHAR(20),
    rating_score    NUMERIC(3,2),
    rating_count    INTEGER,
    high_rated      VARCHAR(3),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Dimension Table: Users ────────────────────
CREATE TABLE IF NOT EXISTS dim_users (
    user_id         INTEGER PRIMARY KEY,
    username        VARCHAR(100),
    email           VARCHAR(150),
    city            VARCHAR(100),
    state           VARCHAR(100),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Fact Table: Orders ────────────────────────
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id        SERIAL PRIMARY KEY,
    cart_id         INTEGER,
    user_id         INTEGER REFERENCES dim_users(user_id),
    product_id      INTEGER REFERENCES dim_products(product_id),
    quantity        INTEGER,
    unit_price      NUMERIC(10,2),
    total_amount    NUMERIC(10,2),
    order_date      DATE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Summary Table ─────────────────────────────
CREATE TABLE IF NOT EXISTS agg_category_sales (
    category        VARCHAR(100),
    total_orders    INTEGER,
    total_revenue   NUMERIC(12,2),
    avg_order_value NUMERIC(10,2),
    report_date     DATE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Indexes ───────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_orders_user    ON fact_orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_product ON fact_orders(product_id);
CREATE INDEX IF NOT EXISTS idx_orders_date    ON fact_orders(order_date);