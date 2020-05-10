DROP TABLE IF EXISTS prices;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS filtered_prices;
DROP TABLE IF EXISTS prices_start_year;
DROP TABLE IF EXISTS prices_end_year;
DROP TABLE IF EXISTS variations;
DROP TABLE IF EXISTS company_trends;
DROP TABLE IF EXISTS same_trends;


CREATE EXTERNAL TABLE IF NOT EXISTS prices
                    (ticker STRING, open FLOAT, close FLOAT, adj_close FLOAT,
                     lowThe FLOAT, highThe FLOAT, volume FLOAT, datee STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/gianmaria/input/in1000/';


CREATE EXTERNAL TABLE IF NOT EXISTS stocks
              (ticker STRING, ex STRING, name STRING, sector STRING, industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/gianmaria/input/stocks';

add jar /home/gianmaria/Documenti/Proj/BigData/hive/UDFUtils/out/artifacts/LastThreeYearsUDF/LastThreeYearsUDF.jar;
create temporary function last_three_years as 'proj.hive.job3.LastThreeYearsUDF';

CREATE TABLE filtered_prices AS
    SELECT *
    FROM prices
    WHERE year(datee) >= 2008 AND year(datee) <= 2018;

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


CREATE TABLE company_trends AS
    SELECT name, last_three_years(collect_set(yy), collect_set(variation)) as last_years
    FROM variations v JOIN stocks s ON v.ticker = s.ticker
    GROUP BY name;

CREATE TABLE same_trends AS
    SELECT last_years, collect_set(name)
    FROM company_trends
    WHERE last_years != "{}"
    GROUP BY last_years;
