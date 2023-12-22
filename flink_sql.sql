CREATE TABLE user_behavior (
    user_id BIGINT,                 
    item_id BIGINT,                 
    category_id BIGINT,             
    behavior STRING,                 
    ts TIMESTAMP(3),                 
    proctime AS PROCTIME(),          
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  
) WITH (
    'connector' = 'kafka',                  
    'topic' = 'user_behavior',             
    'properties.bootstrap.servers' = 'localhost:9092',  
    'properties.group.id' = 'testGroup',   
    'properties.zookeeper.connect' = 'localhost:2181',     
    'format' = 'json',                      
    'scan.startup.mode' = 'earliest-offset',  
    'json.fail-on-missing-field' = 'false',  
    'json.ignore-parse-errors' = 'true'     
);

SELECT * FROM user_behavior;


CREATE TABLE buy_cnt_per_hour (
    hour_of_day BIGINT,    
    buy_cnt BIGINT,        
    event_time TIMESTAMP(3),  
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND  
) WITH (
    'connector' = 'elasticsearch-7',  
    'hosts' = 'http://localhost:9200',  
    'index' = 'buy_cnt_per_hour',       
    'format' = 'json'                   
);

INSERT INTO buy_cnt_per_hour
SELECT 
    HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)) AS hour_of_day, 
    COUNT(*) AS buy_cnt,  
    TUMBLE_START(ts, INTERVAL '1' HOUR) AS event_time  
FROM 
    user_behavior  
WHERE 
    behavior = 'buy'  
GROUP BY 
    TUMBLE(ts, INTERVAL '1' HOUR);


CREATE TABLE cumulative_uv (
    time_str STRING,       
    uv BIGINT              
) WITH (
    'connector' = 'elasticsearch-7',   
    'hosts' = 'http://localhost:9200',  
    'index' = 'cumulative_uv',          
    'format' = 'json'                   
);

CREATE VIEW uv_per_10min AS
SELECT
     DATE_FORMAT(TUMBLE_END(ts, INTERVAL '10' MINUTE), 'yyyy-MM-dd HH:mm:ss') as time_str,
     COUNT(DISTINCT user_id) as uv
FROM 
    user_behavior 
GROUP BY 
    TUMBLE(ts, INTERVAL '10' MINUTE);

INSERT INTO cumulative_uv
SELECT time_str, uv
FROM uv_per_10min;


CREATE TABLE category_dim (
    sub_category_id BIGINT,             
    parent_category_name STRING         
) WITH (
    'connector' = 'jdbc',                           
    'url' = 'jdbc:mysql://localhost:3306/flink?serverTimezone=UTC', 
    'table-name' = 'category',                      
    'driver' = 'com.mysql.cj.jdbc.Driver',          
    'username' = 'root',                            
    'password' = 'syclmh102309',                            
    'lookup.cache.max-rows' = '5000',               
    'lookup.cache.ttl' = '10min'                    
);


CREATE TABLE top_category (
    category_name STRING,  
    buy_cnt BIGINT         
) WITH (
    'connector' = 'elasticsearch-7',       
    'hosts' = 'http://localhost:9200',  
    'index' = 'top_category',              
    'sink.bulk-flush.max-actions' = '1',   
    'format' = 'json'                      
);

CREATE VIEW rich_user_behavior AS
SELECT 
    U.user_id, U.item_id, U.behavior, 
    C.parent_category_name as category_name  
FROM 
    user_behavior AS U
LEFT JOIN 
    category_dim FOR SYSTEM_TIME AS OF U.proctime AS C  
ON 
    U.category_id = C.sub_category_id;

INSERT INTO top_category
SELECT 
    category_name, 
    COUNT(*) AS buy_cnt  
FROM 
    rich_user_behavior
WHERE 
    behavior = 'buy'  
GROUP BY 
    category_name;