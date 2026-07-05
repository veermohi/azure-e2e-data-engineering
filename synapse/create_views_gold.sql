-- ---------------------------------------------------------------------------
-- Create Views (Gold layer)
-- Serverless SQL pool views pointing directly at the cleaned Parquet/Delta
-- files sitting in the Silver container of ADLS Gen2, via OPENROWSET.
-- Replace <storage_account> with your own ADLS Gen2 account name.
-- ---------------------------------------------------------------------------

CREATE VIEW gold.territories
AS
SELECT * FROM OPENROWSET
(
    BULK 'https://<storage_account>.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) AS query1;

-- Repeat the same pattern for the remaining Silver folders, e.g.:
-- CREATE VIEW gold.calendar    AS SELECT * FROM OPENROWSET(BULK '.../AdventureWorks_Calendar/', FORMAT='PARQUET') AS q;
-- CREATE VIEW gold.customers   AS SELECT * FROM OPENROWSET(BULK '.../AdventureWorks_Customers/', FORMAT='PARQUET') AS q;
-- CREATE VIEW gold.products    AS SELECT * FROM OPENROWSET(BULK '.../AdventureWorks_Products/', FORMAT='PARQUET') AS q;
-- CREATE VIEW gold.returns     AS SELECT * FROM OPENROWSET(BULK '.../AdventureWorks_Returns/', FORMAT='PARQUET') AS q;
-- CREATE VIEW gold.sales       AS SELECT * FROM OPENROWSET(BULK '.../AdventureWorks_Sales/', FORMAT='PARQUET') AS q;
-- CREATE VIEW gold.subcat      AS SELECT * FROM OPENROWSET(BULK '.../AdventureWorks_SubCategories/', FORMAT='PARQUET') AS q;
