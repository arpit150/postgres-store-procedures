-- FUNCTION: arpit_test.road_table_backup(character varying, character varying, character varying)

-- DROP FUNCTION arpit_test.road_table_backup(character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION arpit_test.road_table_backup(
	schema_name character varying,
	new_schema_name character varying,
	table_name character varying DEFAULT 'ALL'::character varying)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE
DECLARE SqlQuery text;
DECLARE returnvalue INTEGER;
DECLARE tbl_nme_road text;
DECLARE i integer;
DECLARE j integer;
DECLARE k integer;
DECLARE r record;
DECLARE count integer;
DECLARE yyyy_mm varchar(254);
DECLARE tablename text;
DECLARE stat_code text;
DECLARE arr text [];
DECLARE conquery text;
DECLARE conquery1 text;
DECLARE state_abbr text;
DECLARE stt_count INTEGER;

BEGIN
    returnvalue = 0;
	stat_code = UPPER(LEFT(UPPER(table_name), 2));
	RAISE INFO 'State Code -> %', stat_code;
	state_abbr = ''||Schema_name||'."STATE_ABBR"';
	
--CREATE NEW SCHEMA
	
	EXECUTE 'DROP SCHEMA IF EXISTS '||new_Schema_name||' CASCADE';
	EXECUTE 'CREATE SCHEMA '||new_Schema_name||' ';

IF UPPER(table_name) = 'ALL' THEN

	EXECUTE FORMAT('SELECT count(*) FROM %1$s',state_abbr) INTO stt_count;
	RAISE INFO 'STT_CODE COUNT:%',stt_count;
	k = 0;
	FOR r IN EXECUTE FORMAT('SELECT "STT_CODE" FROM %1$s',state_abbr)

	LOOP
	stat_code = UPPER(r."STT_CODE");
	k = k+1;
	RAISE INFO 'STATE CODE: %',stat_code;
	BEGIN 
		i = 0;
		j = 0;
		
		SqlQuery = 'SELECT COUNT(table_name) FROM information_schema.tables WHERE ((table_name) LIKE '''||UPPER(stat_code)||'_ROAD_NETWORK'' OR (table_name) LIKE '''||UPPER(stat_code)||'%___ROAD_NETWORK'')   AND (TABLE_SCHEMA) LIKE '''||Schema_name||''' ';
		EXECUTE SQLQuery INTO count;
		RAISE INFO 'table count -> %', count;
		RAISE INFO ' QUERY  ---> :%',SqlQuery;
		EXECUTE SqlQuery;

		IF count > 1 THEN

			FOR r IN EXECUTE FORMAT('SELECT table_name FROM information_schema.tables WHERE ((table_name) LIKE '''||UPPER(stat_code)||'%%___ROAD_NETWORK'') AND TABLE_SCHEMA ='''||Schema_name||''' ') 
			LOOP
				tablename = UPPER(r.table_name);
				arr[i]=tablename;
				RAISE WARNING 'table name % AA :%',arr[i],'';
				--EXECUTE 'CREATE TABLE '||new_Schema_name||'."'||tbl_nme_road||'" AS TABLE '||tablename||'';
				i:=i+1;   
			END LOOP;
			i = i-1;

			conquery = 'SELECT * FROM '||Schema_name||'."'||UPPER(arr[0])||'" ';
			LOOP
				EXIT WHEN i = 0;
				conquery1='union all  SELECT * FROM '||Schema_name||'."'||arr[i]||'" ';
				conquery = CONCAT(conquery,  conquery1);
				i=i-1;
			END LOOP;

			EXECUTE'drop table if exists '|| UPPER(stat_code) ||'_ROAD_NETWORK';
			EXECUTE'create table '||new_Schema_name||'."'|| UPPER(stat_code) ||'_ROAD_NETWORK" As ('|| conquery ||')';

			tbl_nme_road = ''|| UPPER(stat_code) ||'_ROAD_NETWORK';
			RAISE INFO 'TABLE ROAD ->%',tbl_nme_road;

			returnvalue = 1;
		ELSE
			EXECUTE 'SELECT COUNT(table_name) FROM information_schema.tables WHERE ((table_name) LIKE '''||UPPER(stat_code)||'_ROAD_NETWORK'')   AND (TABLE_SCHEMA) LIKE '''||Schema_name||''' ' INTO count;
			IF count = 1 THEN
				tbl_nme_road = ''|| UPPER(stat_code) ||'_ROAD_NETWORK';

				EXECUTE 'CREATE TABLE '||new_Schema_name||'."'||tbl_nme_road||'" AS TABLE '||Schema_name||'."'||tbl_nme_road||'"';
				RAISE INFO 'TABLE ROAD ->%',tbl_nme_road;
			ELSE
				RAISE INFO '<>|<>|%_ROAD_NETWORK DOES NOT EXISTS<>|<>|',UPPER(stat_code);
			END IF;

			returnvalue = 1;
		END IF;	
	END;
	END LOOP;
	RETURN returnvalue;
ELSE

	IF UPPER(table_name) LIKE '%ROAD_NETWORK%' THEN
		BEGIN 
			i = 0;
			j = 0;
			EXECUTE 'SELECT COUNT(table_name) FROM information_schema.tables WHERE ((table_name) LIKE '''||UPPER(stat_code)||'_ROAD_NETWORK'' OR (table_name) LIKE '''||UPPER(stat_code)||'%___ROAD_NETWORK'')   AND (TABLE_SCHEMA) LIKE '''||Schema_name||''' ' INTO count;
			RAISE INFO 'table count -> %', count;

			IF count > 1 THEN

			FOR r IN EXECUTE FORMAT('SELECT table_name FROM information_schema.tables WHERE ((table_name) LIKE '''||UPPER(stat_code)||'%%___ROAD_NETWORK'') AND TABLE_SCHEMA ='''||Schema_name||''' ') 
			LOOP
				tablename = UPPER(r.table_name);
				arr[i]=tablename;
				RAISE WARNING 'table name % AA :%',arr[i],'';
				--EXECUTE 'CREATE TABLE '||new_Schema_name||'."'||tbl_nme_road||'" AS TABLE '||tablename||'';
				i:=i+1;
			END LOOP;
			i = i-1;

			conquery = 'SELECT * FROM '||Schema_name||'."'||UPPER(arr[0])||'" ';
			LOOP
				EXIT WHEN i = 0;
				conquery1='union all  SELECT * FROM '||Schema_name||'."'||arr[i]||'" ';
				conquery = CONCAT(conquery,  conquery1);
				i=i-1;
			END LOOP;

			EXECUTE'drop table if exists '|| UPPER(stat_code) ||'_ROAD_NETWORK';
			EXECUTE'create table '||new_Schema_name||'."'|| UPPER(stat_code) ||'_ROAD_NETWORK" As ('|| conquery ||')';

			tbl_nme_road = ''|| UPPER(stat_code) ||'_ROAD_NETWORK';
			RAISE INFO 'TABLE ROAD ->%',tbl_nme_road;

			returnvalue = 1;
	ELSE
			EXECUTE 'SELECT COUNT(table_name) FROM information_schema.tables WHERE ((table_name) LIKE '''||UPPER(stat_code)||'_ROAD_NETWORK'')   AND (TABLE_SCHEMA) LIKE '''||Schema_name||''' ' INTO count;
			IF count = 1 THEN
				tbl_nme_road = ''|| UPPER(stat_code) ||'_ROAD_NETWORK';

				EXECUTE 'CREATE TABLE '||new_Schema_name||'."'||tbl_nme_road||'" AS TABLE '||Schema_name||'."'||tbl_nme_road||'"';
				RAISE INFO 'TABLE ROAD ->%',tbl_nme_road;
			ELSE
				RAISE INFO '|<>|<>|%_ROAD_NETWORK DOES NOT EXISTS<>|<>|',UPPER(stat_code);
			END IF;

			returnvalue = 1;
		END IF;	
	RETURN returnvalue;
	END;	
		
	END IF;
END IF;

END;

$BODY$;

ALTER FUNCTION arpit_test.road_table_backup(character varying, character varying, character varying)
    OWNER TO postgres;
