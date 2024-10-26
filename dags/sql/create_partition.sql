-- create_partition.sql (template-based SQL)
-- Creating multiple partitions for a table can improve performance, scalability, and maintenance in databases like PostgreSQL. The 5 partitions (main partition and 4 hash sub-partitions) are set up to optimize query handling and data organization based on specific factors:

-- The main partition is created based on a date range (e.g., monthly), which enables efficient management and querying of time-based data. By partitioning based on dates, only relevant partitions are scanned during queries within a particular date range.

-- Within the main date-based partition, further sub-partitions are created using hash partitioning on search_term. Hash partitioning divides data evenly across multiple partitions, ensuring balanced distribution based on the search_term column

-- Date-based data management: By range-partitioin (monthly), we could achieve efficient querying, retention, and archiving

-- Search-based performance: By hash-partitioning on search_term, it reduces query times and improves write distribution.

CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF books
FOR VALUES FROM ('{current_month_start}') TO ('{next_month_start}')
PARTITION BY HASH (search_term);

CREATE TABLE IF NOT EXISTS {partition_name}_part_0 PARTITION OF {partition_name} FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE IF NOT EXISTS {partition_name}_part_1 PARTITION OF {partition_name} FOR VALUES WITH (MODULUS 4, REMAINDER 1);

CREATE TABLE IF NOT EXISTS {partition_name}_part_2 PARTITION OF {partition_name} FOR VALUES WITH (MODULUS 4, REMAINDER 2);

CREATE TABLE IF NOT EXISTS {partition_name}_part_3 PARTITION OF {partition_name} FOR VALUES WITH (MODULUS 4, REMAINDER 3);
