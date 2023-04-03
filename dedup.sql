-- you have 5 minutes to solve this
-- -------------------------------------------
-- delete duplicate records from the table given below
-- -- should be a pure SQL command
-- -- without using the CTE
--
--
-- table 1
-- ----------
--
-- eid	ename		salary		deptno		dept_name 		description 				account_number   	price
-- 1		prashant	100			1					DE				some								2						5
-- 1		prashant	100			1					DE				some								2						10
-- 1		prashant	100			1					DE				some								2						10
-- 1		prashant	100			1					DE				some								3						5
-- 1		prashant	100			1					DE				something							6						12


DELETE FROM table1
WHERE eid IN (
  SELECT t.eid
  FROM (
    SELECT eid, ename, salary, deptno, dept_name, description, account_number, price,
           ROW_NUMBER() OVER (PARTITION BY HASH(eid, ename, salary, deptno, dept_name, description, account_number, price) ORDER BY eid) AS rn
    FROM table1
  ) AS t
  WHERE t.rn > 1
);