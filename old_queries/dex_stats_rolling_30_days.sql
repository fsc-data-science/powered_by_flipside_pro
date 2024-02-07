/*

-- from dex swaps 
# Swaps
# Estimated USD Dex Swaps Volume
# Uniswap v2+v3 Swap dominance (estimated USD)

*/

-- CREATE OR REPLACE VIEW pro_charliemarketplace.ecosystem.dex_stats_rolling_30_days AS 
select 
date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_unique_swap_tx,
count(distinct origin_from_address) as n_unique_swappers, 
sum(amount_in_usd) as usd_swap_volume,
sum(case when platform IN ('uniswap-v2', 'uniswap-v3') THEN amount_in_usd ELSE 0 END) as uniswap_volume,
uniswap_volume/usd_swap_volume as uniswap_volume_dominance
from ethereum.defi.ez_dex_swaps 
where block_timestamp >= current_date - 30
group by day_
order by day_ asc
;