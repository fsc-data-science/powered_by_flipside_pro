/*
-- from fact transactions 
Day, # Transactions, Median Nonce, # Unique EOAs, # Unique TO addresses, 
total raw eth transfers (can be payments in ETH or just transfers),
total tx fees in ETH 
avg GWEI per transaction
# failed transactions
% failed transactions 
*/

-- CREATE OR REPLACE VIEW pro_charliemarketplace.ecosystem.tx_stats_rolling_30_days AS 
select     
date_trunc('day', block_timestamp) as day_,
count(*) as n_fact_tx,
median(nonce) as median_nonce,
sum(case when nonce = 0 then 1 else 0 end) as n_first_tx,
count(distinct from_address) as n_unique_from,
count(distinct to_address) as n_unique_to,
sum(case when eth_value > 0 then 1 else 0 end) as n_raw_eth_tranfers,
sum(eth_value) as total_raw_eth_transfers,
sum(tx_fee) as total_tx_fees_eth,
sum(case when status = 'FAIL' then tx_fee else 0 end) failed_eth_fees,
avg(gas_price) as avg_gwei,
sum(CASE WHEN STATUS = 'FAIL' THEN 1 ELSE 0 END) as n_failed_tx,
DIV0(n_failed_tx, n_fact_tx)*100 as percent_failed_tx
    from ethereum.core.fact_transactions 
    where block_timestamp >= current_date - 30
    group by day_
    order by day_ ASC;
