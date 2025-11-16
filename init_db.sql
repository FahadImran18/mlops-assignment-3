-- Initialize PostgreSQL database for NASA APOD ETL Pipeline

-- Create the apod_data table if it doesn't exist
-- This will be created by the Airflow DAG, but we can pre-create it here
CREATE TABLE IF NOT EXISTS apod_data (
    id SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    title TEXT,
    url TEXT,
    explanation TEXT,
    media_type VARCHAR(50),
    hdurl TEXT,
    copyright TEXT,
    extracted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on date for faster lookups
CREATE INDEX IF NOT EXISTS idx_apod_data_date ON apod_data(date);

-- Create index on created_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_apod_data_created_at ON apod_data(created_at);

