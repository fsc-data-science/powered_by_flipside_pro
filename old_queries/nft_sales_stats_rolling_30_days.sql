/*
-- from nft 
# NFT Swaps
# Opensea NFT Swap dominance
# NFT swap volume (ETH)
*/
-- CREATE OR REPLACE VIEW pro_charliemarketplace.ecosystem.nft_sales_stats_rolling_30_days AS 
with select_sales AS (
select 
    block_timestamp, tx_hash, event_type,
    platform_name, 
    seller_address, buyer_address, nft_address, project_name,
    COALESCE(erc1155_value, 1) as n_ids, 
    case when erc1155_value IS NOT NULL then 'erc1155' else 'erc721' end as nft_type,  
    price, total_fees, platform_fee, creator_fee,
    tx_fee
     from ethereum.nft.ez_nft_sales 
    where block_timestamp >= current_date - 30
    and event_type IN ('bid_won', 'sale')
    and currency_address IN (
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', -- WETH 
    'ETH', 
    '0x0000000000a39bb272e79075ade125fd351887ac' -- BLUR Bidding ETH
    )
)

select date_trunc('day', block_timestamp) as day_,
count(distinct tx_hash) as n_nft_sales_tx,
sum(n_ids) as n_nfts_sold,
sum(case when nft_type = 'erc1155' then n_ids end) as n_erc1155,
sum(case when nft_type = 'erc721' then n_ids end) as n_erc721,
sum(case when event_type = 'bid_won' then 1 else 0 end) as n_type_bidwon,
sum(case when event_type = 'sale' then 1 else 0 end) as n_type_sale,
sum(price) as eth_nft_sales_revenue,
sum(total_fees) as eth_platform_plus_creator_fees, 
sum(platform_fee) as eth_platform_fees, 
sum(creator_fee) as eth_creator_fees,
sum(tx_fee) as eth_sales_tx_fees,
count(distinct seller_address) as n_unique_sellers,
count(distinct buyer_address) as n_unique_buyers,
count(distinct nft_address) as n_projects_with_sales
from select_sales
group by day_ 
order by day_ asc;
