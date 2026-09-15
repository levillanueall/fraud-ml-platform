CREATE TABLE IF NOT EXISTS fraud_predictions (
    id SERIAL PRIMARY KEY,
    event_id TEXT UNIQUE,
    customer_id INTEGER NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    merchant TEXT NOT NULL,
    hour INTEGER NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    decision TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);