USE ROLE ACCOUNTADMIN;

--Create Test Database
CREATE OR REPLACE DATABASE alm_test_db;
USE DATABASE alm_test_db;

--Create Test Schema in Test DB
CREATE OR REPLACE SCHEMA alm_test_schema;
USE SCHEMA alm_test_schema;

USE WAREHOUSE compute_wh;

-- Create Sample Data Table
CREATE OR REPLACE TABLE table_a (
  trainer_id NUMBER
, starttime  TIMESTAMP_NTZ
, endtime    TIMESTAMP_NTZ
);

-- Change format of input datetimes
ALTER SESSION SET TIMESTAMP_INPUT_FORMAT='DD/MM/YYYY HH24:MI';

-- Insert Sample Data
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(1234, '01/10/2018 08:30', '01/10/2018 09:00');
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(1234, '01/10/2018 08:45', '01/10/2018 09:15');
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(1234, '01/10/2018 09:30', '01/10/2018 10:00');
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(2345, '01/10/2018 08:45', '01/10/2018 09:15');
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(2345, '01/10/2018 09:30', '01/10/2018 10:00');
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(2345, '01/10/2018 10:50', '01/10/2018 11:00');
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(2345, '01/10/2018 09:50', '01/10/2018 10:00');

-----------------------------
-- SOLUTION: Clashes Query --
-----------------------------

WITH tf_qry AS(
SELECT 
  trainer_id
, starttime
, endtime
, LAG(endtime) OVER(PARTITION BY trainer_id ORDER BY starttime) AS end_time_lag
FROM table_a
ORDER BY trainer_id, starttime)
SELECT
  tf_qry.trainer_id
, tf_qry.starttime
, tf_qry.endtime
FROM tf_qry
WHERE tf_qry.starttime <= tf_qry.end_time_lag;

--------------------------------------------
-- Test data inserts, including NULL tests--
--------------------------------------------

-- Expected outcome: query will include this data in output
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(2345, '01/10/2018 10:56', '01/10/2018 11:00');

-- Expected outcome: query will force VALUES(1234, '01/10/2018 08:30', '01/10/2018 09:00') 
--                   data into output
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(1234, '01/10/2018 08:17', '01/10/2018 08:34');

-- Expected outcome: query will include this data in output
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(2345, '01/10/2018 10:58', NULL);

-- Expected outcome: query will exclude this data from output
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(1234, NULL, NULL);

-- Expected outcome: query will exclude this data from output
INSERT INTO table_a (trainer_id, starttime, endtime)
VALUES(1234, NULL, '01/10/2018 10:04');

-- Remove Test data 
DELETE FROM table_a 
WHERE starttime IS NULL 
OR endtime IS NULL
OR starttime = '01/10/2018 10:56'
OR starttime = '01/10/2018 08:17';
