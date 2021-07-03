-- FUNCTION: arpit_test.road_style(character varying, character varying)

-- DROP FUNCTION arpit_test.road_style(character varying, character varying);

CREATE OR REPLACE FUNCTION arpit_test.road_style(
	schema_name character varying,
	table_name character varying DEFAULT 'ALL'::character varying)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

Declare                                                    
road_style_tab text;
update_log_tab_name text;
road_tab_name text;
error_tab_name text;
count integer;
road_count integer;
r record;
r_tab_name record;
SQLQuery text;
SQLQuery1 text;
SQLQuery2 text;
road_condition text;
pen_style text;
table_count integer;
v_cnt integer;
returnstatus integer;
tab_name character varying;
DECLARE 
f1 text; f2 text;
t1 text; t2 text;

BEGIN
	
	returnstatus = -1;
	road_style_tab = 'arpit_test.road_style';
	update_log_tab_name = 'arpit_test.log_table';
	error_tab_name = 'arpit_test.error';
	--CHECK IF DEFAULT TABLE NAME THEN SERACH FOR ALL TABLES INTO GIVEN SCHEMA NAME
	IF (table_name='ALL') THEN
		--SEARCH FOR ALL ROAD_NETWORK
		SQLQuery = FORMAT ('select count(*) from information_schema.tables where table_schema=''%1$s'' and table_name like ''%%_ROAD_NETWORK''',schema_name);
		RAISE INFO 'QUERY:%',SQLQuery;
		EXECUTE SQLQuery INTO road_count;
		RAISE INFO 'COUNT:%',road_count;
		IF(road_count>0) THEN
			SQLQuery2 = FORMAT ('select table_name from information_schema.tables where table_schema=''%1$s'' and table_name like ''%%_ROAD_NETWORK'' order by table_name',schema_name);
			for r_tab_name in execute SQLQuery2                           
			loop
				road_tab_name = r_tab_name.table_name;
				RAISE INFO 'PROCESS FOR TABLE:%',road_tab_name;
				SQLQuery = FORMAT('SELECT COUNT(*) FROM %1$s',road_style_tab);
				RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
				EXECUTE SQLQuery INTO count;
				RAISE INFO 'COUNT:%',count;
				IF count>0 THEN
					SQLQuery = FORMAT('SELECT "CONDITION","MI_STYLE" FROM %1$s',road_style_tab);
					RAISE INFO 'COUNT:%',SQLQuery;
					for r in execute SQLQuery
					loop
						road_condition = r."CONDITION";
						pen_style = r."MI_STYLE";
						SQLQuery1 = FORMAT('UPDATE %1$s."%2$s" SET "MI_STYLE"=''%3$s'' WHERE %4$s',schema_name,road_tab_name,pen_style,road_condition);
						RAISE INFO 'UPDATE QUERY:%',SQLQuery1;
						EXECUTE SQLQuery1;			
						GET DIAGNOSTICS v_cnt = ROW_COUNT;
						road_condition = REPLACE(road_condition,'''','''''');
						SQLQuery1 = FORMAT('INSERT INTO %1$s (table_name,condition,style,col_count) VALUES(''%2$s'',''%3$s'',''%4$s'',%5$s)',update_log_tab_name,road_tab_name,road_condition,pen_style,v_cnt);
						RAISE INFO 'UPDATE LOG:%',SQLQuery1;
						EXECUTE SQLQuery1;
					end loop;
				ELSE
					RAISE INFO 'NO RECORDS WERE FOUND INTO %',road_style_tab;
				END IF;
			end loop;
			returnstatus = 1;
		ELSE
			RAISE INFO 'ROAD_NETWORK NOT FOUND INTO SCHEMA %',schema_name;
		END IF;
		
	ELSE
		SQLQuery = FORMAT ('select count(*) from information_schema.tables where table_schema=''%1$s'' and table_name=''%2$s''',schema_name,table_name);
		RAISE INFO 'QUERY:%',SQLQuery;
		EXECUTE SQLQuery INTO road_count;
		RAISE INFO 'COUNT:%',road_count;
		IF(road_count>0) THEN
			SQLQuery = FORMAT('SELECT COUNT(*) FROM %1$s',road_style_tab);
			RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
			EXECUTE SQLQuery INTO count;
			RAISE INFO 'COUNT:%',count;
			IF count>0 THEN
				SQLQuery = FORMAT('SELECT "CONDITION","MI_STYLE" FROM %1$s order by id',road_style_tab);
				for r in execute SQLQuery
				loop
					road_condition = r."CONDITION";
					pen_style = r."MI_STYLE";
					SQLQuery1 = FORMAT('UPDATE %1$s."%2$s" SET "MI_STYLE"=''%3$s'' WHERE %4$s',schema_name,table_name,pen_style,road_condition);
					RAISE INFO 'UPDATE QUERY:%',SQLQuery1;
					EXECUTE SQLQuery1;			
					GET DIAGNOSTICS v_cnt = ROW_COUNT;
					road_condition = REPLACE(road_condition,'''','''''');
					SQLQuery1 = FORMAT('INSERT INTO %1$s (table_name,condition,style,col_count) VALUES(''%2$s'',''%3$s'',''%4$s'',%5$s)',update_log_tab_name,table_name,road_condition,pen_style,v_cnt);
					RAISE INFO 'UPDATE LOG:%',SQLQuery1;
					EXECUTE SQLQuery1;
				end loop;
				returnstatus = 1;
			ELSE
				RAISE INFO 'NO RECORDS WERE FOUND INTO %',road_style_tab;
			END IF;
		ELSE 
			RAISE INFO '% NOT FOUND INTO SCHEMA %',table_name,schema_name;
		END IF;
		
	END IF;
	RETURN returnstatus;
	EXCEPTION
	WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS 
		f1=MESSAGE_TEXT,
		f2=PG_EXCEPTION_CONTEXT; 
		RAISE info 'error caught:%',f1;
		RAISE info 'error caught:%',f2;
		SQLQuery = FORMAT('INSERT INTO %1$s (table_name,table_schema,message,context) Values(''%2$s'',''%3$s'',''%4$s'',''%5$s'')',error_tab_name,table_name,schema_name,f1,f2);
		EXECUTE SQLQuery;
		
	RETURN returnstatus;
END 

$BODY$;

ALTER FUNCTION arpit_test.road_style(character varying, character varying)
    OWNER TO postgres;
