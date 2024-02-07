-- CREATE OR REPLACE VIEW pro_charliemarketplace.ecosystem.ethusd_ohlc_rolling_30_days AS 
with uniswap_ETH_stable_swaps AS (
SELECT block_number, block_timestamp, 
amount0_adjusted, amount1_adjusted, token0_symbol, token1_symbol
FROM ethereum.uniswapv3.ez_swaps WHERE POOL_ADDRESS IN (
'0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8', -- ETH USDC 0.3%
'0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', -- ETH USDC 0.05%
'0x4e68ccd3e89f51c3074ca5072bbac773960dfa36', -- ETH USDT 0.3%
'0x11b815efb8f581194ae79006d24e0d814b7697f6', -- ETH USDT 0.05%
'0x7bea39867e4169dbe237d55c8242a8f2fcdcc387', -- ETH USDC 1%
'0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'   -- ETH DAI 0.3% 
    )
   -- Previous 30 Days
AND block_timestamp >= current_date - 30
  ),
    
eth_stable_price AS (
SELECT BLOCK_NUMBER, BLOCK_TIMESTAMP, 
IFF(TOKEN1_SYMBOL = 'WETH', ABS(DIV0(AMOUNT0_ADJUSTED, AMOUNT1_ADJUSTED)), 
           ABS(DIV0(AMOUNT1_ADJUSTED, AMOUNT0_ADJUSTED))
   ) as eth_stable_trade_price,
  IFF(TOKEN1_SYMBOL = 'WETH', ABS(AMOUNT1_ADJUSTED), ABS(AMOUNT0_ADJUSTED)) as eth_volume,
IFF(TOKEN1_SYMBOL = 'WETH', TOKEN0_SYMBOL, TOKEN1_SYMBOL) as stable           
           FROM uniswap_eth_stable_swaps
   		 WHERE ABS(AMOUNT0_ADJUSTED) > 1e-8 AND ABS(AMOUNT1_ADJUSTED) > 1e-8
),

eth_block_price AS ( 
SELECT BLOCK_NUMBER, BLOCK_TIMESTAMP, 
  div0(SUM(eth_stable_trade_price * eth_volume),sum(eth_volume)) as eth_vwap,
  SUM(eth_volume) as eth_volume,
  COUNT(*) as num_swaps
    FROM eth_stable_price
    GROUP BY BLOCK_NUMBER, BLOCK_TIMESTAMP
order by block_number asc
),

hourly1 AS (
SELECT date_trunc('hour', block_timestamp) as hour_, 
min(eth_vwap) as low, 
median(eth_vwap) as med_price, 
avg(eth_vwap) as avg_price, 
max(eth_vwap) as high, 
sum(eth_volume) as volume,
sum(num_swaps) as swaps
 FROM eth_block_price
group by hour_
ORDER BY hour_ asc
),

hourly2 AS (
SELECT distinct date_trunc('hour', block_timestamp) as hour_, 
first_value(eth_vwap) over (partition by hour_ order by block_number asc) as open, 
last_value(eth_vwap) over (partition by hour_ order by block_number asc) as close
from eth_block_price
order by hour_ asc
)

 select hour_, open, low, high, close, volume, swaps 
  from hourly1 natural join hourly2
  where hour_ < date_trunc('hour', SYSDATE()) -- only latest COMPLETED hour using UTC 
order by hour_ asc;

