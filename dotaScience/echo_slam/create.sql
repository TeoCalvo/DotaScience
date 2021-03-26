DROP TABLE IF EXISTS context.tb_book_player;
CREATE TABLE context.tb_book_player
USING DELTA
OPTIONS(PATH='{table_path}')
PARTITIONED BY (partition_year, partition_month, partition_day)
AS
{query}
