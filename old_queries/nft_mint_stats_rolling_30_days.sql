-- CREATE OR REPLACE VIEW pro_charliemarketplace.ecosystem.nft_mint_stats_rolling_30_days AS 
select date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_mint_tx,
count(distinct nft_address) as n_projects_minting,
sum(nft_count) as n_nfts_minted,
count(distinct nft_to_address) as n_minters,
sum(case when mint_price_eth IS NULL THEN nft_count
    when mint_price_eth = 0 THEN nft_count ELSE 0 END) as n_free_mints,
sum(mint_price_eth) as eth_mint_revenue,
sum(tx_fee) as eth_mint_tx_fees
 from ethereum.nft.ez_nft_mints 
 where block_timestamp >= current_date - 30
 group by day_
 order by day_ asc;