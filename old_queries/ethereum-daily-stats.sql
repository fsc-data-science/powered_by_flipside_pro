/*
This creates a view for Ethereum covering DAILY stats for past 30 days using other views:
*/

-- day level aggregated 
select * from pro_charliemarketplace.ecosystem.tx_stats_rolling_30_days;
select * from pro_charliemarketplace.ecosystem.dex_stats_rolling_30_days;
select * from pro_charliemarketplace.ecosystem.contracts_created_rolling_30_days;
select * from pro_charliemarketplace.ecosystem.nft_mint_stats_rolling_30_days;
select * from pro_charliemarketplace.ecosystem.nft_sales_stats_rolling_30_days;

-- CREATE OR REPLACE VIEW pro_charliemarketplace.ecosystem.day_level_stats AS 
select * from 
 pro_charliemarketplace.ecosystem.tx_stats_rolling_30_days left join
 pro_charliemarketplace.ecosystem.contracts_created_rolling_30_days USING (day_) left join
 pro_charliemarketplace.ecosystem.dex_stats_rolling_30_days USING (day_) left join
 pro_charliemarketplace.ecosystem.nft_mint_stats_rolling_30_days USING (day_) left join
 pro_charliemarketplace.ecosystem.nft_sales_stats_rolling_30_days USING (day_) 
;

-- day level but not used in ethmetrics
select * from pro_charliemarketplace.ecosystem.tx_types_rolling_30_days;   

-- platform day level
select * from pro_charliemarketplace.ecosystem.nft_platform_sales_stats_rolling_30_days;

-- hourly level 
select * from pro_charliemarketplace.ecosystem.ethusd_ohlc_rolling_30_days;


-- Ecosystem 18 Stats 
-- SEE: ETH Metrics
select * from pro_charliemarketplace.ecosystem.ethmetrics_eco17; 
/*
ecosystem
 24hr forecast -- need to do this separately

*/
-- 



