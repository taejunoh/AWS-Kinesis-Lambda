SELECT ticker, hour, highest_price, datetime
FROM 
    (SELECT T1.ticker, T2.highest_price, T1.datetime, T1.Hour
     FROM 
         (SELECT name AS ticker, high, ts AS datetime, SUBSTRING(ts, 12, 2) AS Hour FROM "16") T1
INNER JOIN 
         (SELECT name, SUBSTRING(ts, 12, 2) AS hour, MAX(high) AS highest_price FROM "16"
GROUP BY  name, SUBSTRING(ts, 12, 2)) T2
ON T1.ticker = T2.name AND T1.high = T2.highest_price AND T1.Hour = T2.hour)
ORDER BY  ticker, Hour, datetime