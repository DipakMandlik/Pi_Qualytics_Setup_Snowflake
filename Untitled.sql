CREATE TABLE DBT.SOURCE.ITEMS(
ID int,
NAME STRING,
CATEGORY STRING,
UPDATEDDATE TIMESTAMP
)

INSERT INTO DBT.SOURCE.ITEMS
VALUES
(4,'ITEM4_new','CATEGORY4',CURRENT_TIMESTAMP());


select * from dbt.gold.gold_items;
select * from dbt.gold.source_gold_items;
select * from dbt.source.items;