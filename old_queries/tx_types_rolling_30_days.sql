
-- CREATE VIEW pro_charliemarketplace.ecosystem.tx_types_rolling_30_days AS 
with any_successful_tx AS (
select     
date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_successful_tx
    from ethereum.core.fact_transactions 
    where block_timestamp >= current_date - 30
    and STATUS = 'SUCCESS'
    group by day_
),

  -- may overlap with other tx like nft mints paid in ETH, dex swaps, etc.
eth_transfers AS (
select     
date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_eth_transfer_tx
    from ethereum.core.ez_eth_transfers
    where block_timestamp >= current_date - 30
    group by day_
),

token_transfers AS (
select     
date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_token_transfer_tx
    from ethereum.core.ez_token_transfers
    where block_timestamp >= current_date - 30
    group by day_
),

    -- subset of transfers 
dex_tx AS (
select     
date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_dex_tx
    from ethereum.defi.ez_dex_swaps 
    where block_timestamp >= current_date - 30
    group by day_
),
    
nft_transfer AS (
select     
date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_nft_transfer_tx
    from ethereum.nft.ez_nft_transfers
    where block_timestamp >= current_date - 30
    group by day_
),
    -- subset of nft transfers
nft_mints AS (
select     
date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_nft_mint_tx
    from ethereum.nft.ez_nft_mints 
    where block_timestamp >= current_date - 30
    group by day_
),

    -- subset of nft transfers
nft_swaps AS (
select     
date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_nft_swap_tx
    from ethereum.nft.ez_nft_sales
    where block_timestamp >= current_date - 30
    group by day_
)

-- natural join identifies day_
select *
from any_successful_tx 
    natural full join eth_transfers 
    natural full join token_transfers
    natural full join  dex_tx 
    natural full join  nft_transfer 
    natural full join  nft_mints
    natural full join  nft_swaps
order by day_ asc

