SELECT  
  DOLocationID,
  max(tip_amount)
FROM `dinnerbot-448608.dinnerbotDEV.green_taxy` 
where 1=1 
  AND EXTRACT(MONTH from lpep_pickup_datetime) = 11
  AND  EXTRACT(YEAR from lpep_pickup_datetime) = 2025
  and PULocationID = 74 --ID for East Harlem North
group by 
  DOLocationID
order by   max(tip_amount) desc
