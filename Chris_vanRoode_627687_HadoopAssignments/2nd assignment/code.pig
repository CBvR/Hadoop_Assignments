-- Import CSVExcelStorage into the script
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

-- Get the orders from HDFS
All_orders = LOAD '/user/maria_dev/diplomacy/orders.csv' USING CSVExcelStorage() AS
(game_id:int,unit_id:int,unit_order:chararray,location:chararray,target:chararray,target_dest:chararray,success:int,reason:int,turn_num:int);

-- Filter the target rows with only Holland in them
Orders_target_holland = FILTER All_orders BY target == 'Holland';

-- Group the csv according to the location
Orders_grouped_by_location = GROUP Orders_target_holland BY (location,target);

-- Count the amount of times holland was targeted by a particular location
counted_locations = FOREACH Orders_grouped_by_location GENERATE group, COUNT(Orders_target_holland);

-- Order the list according to the locations name
Counted_locations_ascending = ORDER counted_locations BY $0 ASC;

-- Show the result of the script
DUMP Counted_locations_ascending;