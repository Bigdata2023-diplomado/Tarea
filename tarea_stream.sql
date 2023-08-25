CREATE STREAM registros_1 (symbol VARCHAR, price DOUBLE, volume DOUBLE, timestamp STRING) WITH (kafka_topic='Registros', value_format='json', partitions=1);

SELECT * FROM registros_1;

CREATE TABLE reg_record AS SELECT symbol, count(price) AS total_registros FROM registros_1 GROUP BY symbol EMIT CHANGES;


CREATE TABLE reg_prom AS SELECT symbol, count(price) AS total_registros, sum(price*volume) AS weighted_price, sum(volume) AS total_volume, sum(price*volume)/sum(volume) AS promedio FROM registros_1 GROUP BY symbol EMIT CHANGES;

SELECT * FROM reg_prom WHERE symbol = 'BINANCE:BTCUSDT';

SELECT * FROM reg_prom WHERE symbol = 'AAPL';
SELECT * FROM reg_prom WHERE symbol = 'AMZN';
CREATE TABLE reg_record AS SELECT symbol, count(price) AS total_registros FROM registros_1 GROUP BY symbol EMIT CHANGES;

CREATE TABLE maxprice_record AS
 SELECT symbol,
 max(price) AS precio_maximo
 FROM registros_1
 GROUP BY symbol
 EMIT CHANGES;



SELECT * FROM maxprice_record
WHERE symbol = 'BINANCE:BTCUSDT';
SELECT * FROM maxprice_record
WHERE symbol = 'AAPL';
SELECT * FROM maxprice_record
WHERE symbol = 'AMZN';

CREATE TABLE minprice_record AS
 SELECT symbol,
 min(price) AS precio_minimo
 FROM registros_1
 GROUP BY symbol
 EMIT CHANGES;

SELECT * FROM minprice_record;

SELECT * FROM minprice_record
WHERE symbol = 'BINANCE:BTCUSDT';
SELECT * FROM minprice_record
WHERE symbol = 'AAPL';
SELECT * FROM minprice_record
WHERE symbol = 'AMZN';


