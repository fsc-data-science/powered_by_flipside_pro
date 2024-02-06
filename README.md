# Powered by Flipside Pro
 Leveraging Flipside Pro to curate custom tables and manage incremental models with views. With some best practices in Tidy Data Engineering.

# Tidyness in Data Curation

Hardcore database admins may be aware of the origins of relational databases. 
[Codd, 1971: Third Normal Form (3NF)](https://en.wikipedia.org/wiki/Third_normal_form) 

Being more on the data *science* side of things as opposed to engineering I prefer to think of data curation in terms of Tidyness:

[Wickham, 2014: Tidy data.](https://cran.r-project.org/web/packages/tidyr/vignettes/tidy-data.html)

1. Each variable is a column; each column is a variable.
2. Each observation is a row; each row is an observation.
3. Each value is a cell; each cell is a single value.

With these, I will lay out 3 Engineering Principles for building Tidy data on top of data *other engineers developed*. Acknowledging your dependencies and implementing defensive engineering will make your life curating data much easier.

 # 3 Engineering Principles

 Building new curations on top of other engineers' data feeds requires assumptions about the pipelines and incremental modeling of those feeds. 

 This repo implements the following defensive engineering practices on curations: 

 1. Reproducibility - a curated table is essentially a query output. The most defensive means of keeping a table live is to not create it- instead you simply query the foundational data every time.
 
 The benefit is your curation is a single source of truth, easily edited (e.g., to handle new, renamed, or reformatted columns in the foundational data). The cost is potentially expensive and unnecessary repeated computation (calculating every past day every new day over and over).

Imagine calculating the # of new users each day on Ethereum. You can simply identify all addresses submitting a 0 nonce transaction, since nonces increment on transaction count.

If the foundational data changed (e.g., origin_from_address is renamed to from_address), you can simply re-calculate from genesis with the required corrects, no data engineering needed.

 2. Incrementalism - A curated table is more optimized when it is incremental. For example, only calculating the new users for the most recently completed day, and appending it to the historical table of new users each day.

 3. Uniqueness - Foundational data may be modified, retroactively inserted, or otherwise corrected. For example, the USD price of a token uses the hourly closing price; if the hour is not yet closed, some data may be available (amount of tokens transferred) while related data is to-be-modified (USD value). 

 Data updates may not be perfectly aligned to incremental models, to avoid duplication, rows must be de-duplicated. It is critical to understand the `unit` of a table. 

 ## Incremental Example 

Imagine you want to track the number of transactions each hour on Ethereum. Your `unit` would be the hour. To defend against updating data, while preserving uniqueness, and gaining the efficiencies of incrementalism you can do the following:

1. Create your table with a clear `unit`. Here, hour is timestamp and # of transactions is an integer.

 ```sql 
-- my PRO account has a database pregenerated for me: pro_charliemarketplace
-- making a tests schema within this database.

CREATE SCHEMA IF NOT EXISTS pro_charliemarketplace.tests; 

CREATE TABLE IF NOT EXISTS pro_charliemarketplace.tests.aggregated_hourly_transactions (
    hour TIMESTAMP,
    transaction_count INTEGER
);

```

2. Identify a lookback `source` period in your table. Here, let's say `4 hours` from the latest data available in the table, using UNIX start time '1970-01-01' if the table is empty.

3. Use this lookback to `target` within a MERGE & MATCH pattern.
    a. Merge into your target table, the source, matching on our `unit` (hour)
    b. if a match occurs, make the `target` match the `source` via `UPDATE`
    c. if there's no match, simply `INSERT` the new data.

```sql
with lookback AS (
SELECT
        DATE_TRUNC('hour', block_timestamp) AS hour,
        COUNT(*) AS transaction_count
    FROM
        ethereum.core.fact_transactions
    WHERE
    -- COALESCE protects against a NULL result by providing an alternative
        block_timestamp > COALESCE(
            DATEADD(hour, -4, 
                (SELECT MAX(hour) FROM pro_charliemarketplace.tests.aggregated_hourly_transactions)
                ),
            '1970-01-01' -- Default start date for the first run
            )
    GROUP BY
        DATE_TRUNC('hour', block_timestamp)
)

MERGE INTO pro_charliemarketplace.tests.aggregated_hourly_transactions AS target
USING (
   select * from lookback   
) AS source
ON target.hour = source.hour
WHEN MATCHED THEN
    UPDATE SET target.transaction_count = source.transaction_count
WHEN NOT MATCHED THEN
    INSERT (hour, transaction_count)
    VALUES (source.hour, source.transaction_count)
 ```

This incremental model meets all 3 of the engineering principles. 

1. We can simply empty the table, and start from scratch (1970-01-01) if we need a full refresh.
2. Otherwise, if we have some data, we only re-calculate the last 4 hours, saving compute. 
3. We use the hour as the `unit` to ensure we meet uniqueness criteria, and ultimately keep the data Tidy.