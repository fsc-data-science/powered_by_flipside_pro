-- CREATE VIEW pro_charliemarketplace.ecosystem.contracts_created_rolling_30_days AS 
select date_trunc('day', block_timestamp) as day_,
count(*) as n_contracts,
sum(case when type = 'CREATE2' THEN 1 ELSE 0 END) as create2,
DIV0(create2, n_contracts) as create2_dominance 
from ethereum.core.fact_traces
WHERE block_timestamp > current_date - 30
AND TYPE IN ('CREATE', 'CREATE2')
AND OUTPUT IS NOT NULL
AND TX_STATUS = 'SUCCESS'
AND TRACE_STATUS = 'SUCCESS'
group by day_
ORDER BY day_ ASC;


