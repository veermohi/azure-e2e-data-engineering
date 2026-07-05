-- ---------------------------------------------------------------------------
-- Create External Table (Gold layer)
-- Materializes a Gold-layer external table backed by the Gold container,
-- built on top of the gold.sales view.
-- Replace <storage_account> and <credential_name> with your own values.
-- ---------------------------------------------------------------------------

CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION   = 'https://<storage_account>.dfs.core.windows.net/gold',
    CREDENTIAL = <credential_name>
);

CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE      = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL TABLE gold.extsales
WITH (
    LOCATION    = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales;

-- Sanity check
SELECT * FROM gold.extsales;
