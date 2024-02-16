    /*
    Not meant to be run sequentially. Contains all snippets required for creating the evm_daily_contracts model.
    */

    -- EVM Schema 

CREATE SCHEMA IF NOT EXISTS pro_charliemarketplace.evm_metrics; 

CREATE TABLE IF NOT EXISTS pro_charliemarketplace.evm_metrics.daily_contracts (
    chain VARCHAR,
    day TIMESTAMP,
    n_contracts INTEGER
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
    date_trunc('day', block_timestamp) as day_,
    count(*) as n_contracts
    from ethereum.core.fact_traces
    where block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    AND TYPE IN ('CREATE', 'CREATE2')
    AND OUTPUT IS NOT NULL
    AND TX_STATUS = 'SUCCESS'
    AND TRACE_STATUS = 'SUCCESS'
    group by day_
    ),

    arb_contracts AS (
    select 
    'arbitrum' as chain,
    date_trunc('day', block_timestamp) as day_,
    count(*) as n_contracts
    from arbitrum.core.fact_traces
    where block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    AND TYPE IN ('CREATE', 'CREATE2')
    AND OUTPUT IS NOT NULL
    AND TX_STATUS = 'SUCCESS'
    AND TRACE_STATUS = 'SUCCESS'
    group by day_
    ),


    avax_contracts AS (
    select 
    'avalanche' as chain,
    date_trunc('day', block_timestamp) as day_,
    count(*) as n_contracts
    from avalanche.core.fact_traces
    where block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    AND TYPE IN ('CREATE', 'CREATE2')
    AND OUTPUT IS NOT NULL
    AND TX_STATUS = 'SUCCESS'
    AND TRACE_STATUS = 'SUCCESS'
    group by day_
    ),

    op_contracts AS (
    select 
    'optimism' as chain,
    date_trunc('day', block_timestamp) as day_,
    count(*) as n_contracts
    from optimism.core.fact_traces
    where block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    AND TYPE IN ('CREATE', 'CREATE2')
    AND OUTPUT IS NOT NULL
    AND TX_STATUS = 'SUCCESS'
    AND TRACE_STATUS = 'SUCCESS'
    group by day_
    ),

    bsc_contracts AS (
    select 
    'bsc' as chain,
    date_trunc('day', block_timestamp) as day_,
    count(*) as n_contracts
    from bsc.core.fact_traces
    where block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    AND TYPE IN ('CREATE', 'CREATE2')
    AND OUTPUT IS NOT NULL
    AND TX_STATUS = 'SUCCESS'
    AND TRACE_STATUS = 'SUCCESS'
    group by day_
    ),

    base_contracts AS (
    select 
    'base' as chain,
    date_trunc('day', block_timestamp) as day_,
    count(*) as n_contracts
    from base.core.fact_traces
    where block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    AND TYPE IN ('CREATE', 'CREATE2')
    AND OUTPUT IS NOT NULL
    AND TX_STATUS = 'SUCCESS'
    AND TRACE_STATUS = 'SUCCESS'
    group by day_
    ),

    polygon_contracts AS (
    select 
    'polygon' as chain,
    date_trunc('day', block_timestamp) as day_,
    count(*) as n_contracts
    from polygon.core.fact_traces
    where block_timestamp > COALESCE(
    -- 2 day lookback 
                dateadd(day, -2, (select max(day_) from pro_charliemarketplace.evm_metrics.daily_contracts)),
            '1970-01-01' -- Default start date for the first run
            )
    AND TYPE IN ('CREATE', 'CREATE2')
    AND OUTPUT IS NOT NULL
    AND TX_STATUS = 'SUCCESS'
    AND TRACE_STATUS = 'SUCCESS'
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
    UPDATE SET target.n_contracts = source.n_contracts
    WHEN NOT MATCHED THEN
    INSERT (chain, day_, n_contracts)
    VALUES (source.chain, source.day_, source.n_contracts);

    RETURN 'EVM daily contracts updated successfully';
END;
$$;

