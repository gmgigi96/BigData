-- hive --hiveconf input=<input path> -f <script path>

DROP TABLE IF EXISTS variations;
DROP TABLE IF EXISTS stats;
DROP TABLE IF EXISTS prices;
DROP TABLE IF EXISTS stock_statistics;


CREATE EXTERNAL TABLE IF NOT EXISTS prices
                    (ticker STRING, open FLOAT, close FLOAT, adj_close FLOAT,
                     lowThe FLOAT, highThe FLOAT, volume FLOAT, datee STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:input}';



-- compute variation for each stock
CREATE TABLE variations AS
    SELECT p1.ticker, (max_date_close - min_date_close) / min_date_close * 100 as variation
    FROM (SELECT ticker, close as min_date_close
        FROM prices p
        WHERE datee = (SELECT min(to_date(datee))
                        FROM prices
                        WHERE p.ticker = ticker AND year(datee) >= 2008 AND year(datee) <= 2018
                        GROUP BY ticker)) p1
        JOIN
        (SELECT ticker, close as max_date_close
        FROM prices p
        WHERE datee = (SELECT max(to_date(datee))
                        FROM prices
                        WHERE p.ticker = ticker AND year(datee) >= 2008 AND year(datee) <= 2018
                        GROUP BY ticker)) p2
    WHERE p1.ticker = p2.ticker;

-- compute min close price, max close price, average volume for each stock
CREATE TABLE stats AS
    SELECT ticker, min(close) as min_close, max(close) as max_close, avg(volume) as mean_volume
    FROM prices
    WHERE year(datee) >= 2008 AND year(datee) <= 2018
    GROUP BY ticker;

-- statistics of all stocks
CREATE TABLE stock_statistics AS
    SELECT s.ticker, min_close, max_close, mean_volume, variation
    FROM stats s, variations v
    WHERE s.ticker = v.ticker
    ORDER BY variation DESC;
