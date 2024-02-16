    /*
    Not meant to be run sequentially. Contains all snippets required for creating the evm_daily_contracts model.
    */

    -- EVM Schema 

CREATE SCHEMA IF NOT EXISTS pro_charliemarketplace.evm_metrics; 

DROP TABLE pro_charliemarketplace.evm_metrics.daily_contracts;

CREATE TABLE IF NOT EXISTS pro_charliemarketplace.evm_metrics.daily_contracts (
    chain VARCHAR,
    day_ TIMESTAMP,
    n_contracts INTEGER,
    n_deployers INTEGER
);

call pro_charliemarketplace.evm_metrics.update_daily_contracts();

CREATE OR REPLACE PROCEDURE pro_charliemarketplace.evm_metrics.update_daily_contracts()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
MERGE INTO pro_charliemarketplace.evm_metrics.daily_contracts AS target
    USING (
    with eth_contracts AS (
    select 
    'ethereum' as chain,
    date_trunc('day', created_block_timestamp) as day_,
    count(*) as n_contracts,
    count(distinct creator_address) as n_deployers
    from ethereum.core.dim_contracts
    where created_block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    group by day_
    ),

    arb_contracts AS (
    select 
    'arbitrum' as chain,
    date_trunc('day', created_block_timestamp) as day_,
    count(*) as n_contracts,
    count(distinct creator_address) as n_deployers
    from arbitrum.core.dim_contracts
    where created_block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    group by day_
    ),


    avax_contracts AS (
    select 
    'avalanche' as chain,
    date_trunc('day', created_block_timestamp) as day_,
    count(*) as n_contracts,
    count(distinct creator_address) as n_deployers
    from avalanche.core.dim_contracts
    where created_block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    group by day_
    ),

    op_contracts AS (
    select 
    'optimism' as chain,
    date_trunc('day', created_block_timestamp) as day_,
    count(*) as n_contracts,
    count(distinct creator_address) as n_deployers
    from optimism.core.dim_contracts
    where created_block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    group by day_
    ),

    bsc_contracts AS (
    select 
    'bsc' as chain,
    date_trunc('day', created_block_timestamp) as day_,
    count(*) as n_contracts,
    count(distinct creator_address) as n_deployers
    from bsc.core.dim_contracts
    where created_block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    group by day_
    ),

    base_contracts AS (
    select 
    'base' as chain,
    date_trunc('day', created_block_timestamp) as day_,
    count(*) as n_contracts,
    count(distinct creator_address) as n_deployers
    from base.core.dim_contracts
    where created_block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    group by day_
    ),

    polygon_contracts AS (
    select 
    'polygon' as chain,
    date_trunc('day', created_block_timestamp) as day_,
    count(*) as n_contracts,
    count(distinct creator_address) as n_deployers
    from polygon.core.dim_contracts
    where created_block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    group by day_
    )

    select * from eth_contracts 
    UNION ALL
    select * from arb_contracts 
    UNION ALL 
    select * from bsc_contracts 
    UNION ALL 
    select * from op_contracts 
    UNION ALL 
    select * from polygon_contracts 
    UNION ALL 
    select * from base_contracts 
    UNION ALL 
    select * from avax_contracts 
    )
    AS source
    ON target.day_ = source.day_ AND target.chain = source.chain
    WHEN MATCHED THEN
    UPDATE SET target.n_contracts = source.n_contracts, target.n_deployers = source.n_deployers
    WHEN NOT MATCHED THEN
    INSERT (chain, day_, n_contracts, n_deployers)
    VALUES (source.chain, source.day_, source.n_contracts, source.n_deployers);

    RETURN 'EVM daily contracts updated successfully';
END;
$$;

