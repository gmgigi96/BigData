-- time hive --hiveconf input1=<input hsp> --hiveconf input2=<input stocks> -f <script hive>


DROP TABLE IF EXISTS prices;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS filtered_prices;
DROP TABLE IF EXISTS annual_volume;
DROP TABLE IF EXISTS quotation;
DROP TABLE IF EXISTS prices_start_year;
DROP TABLE IF EXISTS prices_end_year;
DROP TABLE IF EXISTS variations;
DROP TABLE IF EXISTS trend_sector;


CREATE EXTERNAL TABLE IF NOT EXISTS prices
                    (ticker STRING, open FLOAT, close FLOAT, adj_close FLOAT,
                     lowThe FLOAT, highThe FLOAT, volume FLOAT, datee STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:input1}';


CREATE EXTERNAL TABLE IF NOT EXISTS stocks
              (ticker STRING, ex STRING, name STRING, sector STRING, industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\" )
STORED AS TEXTFILE
LOCATION '${hiveconf:input2}';



CREATE TABLE filtered_prices AS
    SELECT *
    FROM prices
    WHERE year(datee) >= 2008 AND year(datee) <= 2018;


-- annual volume
CREATE TABLE annual_volume AS
    SELECT ticker, year(datee) as yy, sum(volume) as annual_volume
    FROM filtered_prices
    GROUP BY ticker, year(datee);


-- daily quotation
CREATE TABLE quotation AS
    SELECT ticker, year(datee) as yy, avg(close) as quotation
    FROM filtered_prices
    GROUP BY ticker, year(datee);



-- annual variation


CREATE TABLE prices_start_year AS
    SELECT ticker, year(o.datee) as yy, o.close as close_start_year
    FROM filtered_prices o
    WHERE datee = (SELECT min(to_date(datee))
                   FROM filtered_prices
                   WHERE ticker = o.ticker AND year(datee) = year(o.datee)
                   GROUP BY ticker, year(datee));

CREATE TABLE prices_end_year AS
    SELECT ticker, year(o.datee) as yy, o.close as close_end_year
    FROM filtered_prices o
    WHERE datee = (SELECT max(to_date(datee))
                   FROM filtered_prices
                   WHERE ticker = o.ticker AND year(datee) = year(o.datee)
                   GROUP BY ticker, year(datee));


CREATE TABLE variations AS
    SELECT pe.ticker, pe.yy, (close_end_year - close_start_year) / close_start_year * 100 as variation
    FROM prices_end_year pe, prices_start_year ps
    WHERE pe.ticker = ps.ticker AND pe.yy = ps.yy;

DROP TABLE prices_start_year;
DROP TABLE prices_end_year;


CREATE TABLE trend_sector AS
    SELECT s.sector, v.yy, avg(av.annual_volume), avg(q.quotation), avg(v.variation)
    FROM annual_volume av, quotation q, variations v, stocks s
    WHERE av.ticker = q.ticker AND av.ticker = v.ticker AND av.yy = q.yy AND av.yy = v.yy AND av.ticker=s.ticker
    GROUP BY s.sector, v.yy;
