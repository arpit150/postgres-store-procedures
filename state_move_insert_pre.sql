-- FUNCTION: mmi_state_move.state_move_insert_pre()

-- DROP FUNCTION mmi_state_move.state_move_insert_pre();

CREATE FUNCTION mmi_state_move.state_move_insert_pre()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF 
AS $BODY$

DECLARE 
f1 text; f2 text;
SQLQuery text;
SQLQuery1 text;
tab_id text;
master_data_schecma text;
user_lock_schema text;
user_lock_table text;
poi_ver integer;
tab_name text;
state_cd integer;
count integer;
r record;
log_table_name text;
log_schema_name text;
sremark  text;
sstatus text;
begin
      	master_data_schecma = 'mmi_master';
	  
	  	user_lock_schema = 'mmi_lock';
	  	user_lock_table = 'userlock';
		
		log_table_name='log_table';
		log_schema_name='mmi_state_move';
	 
		tab_id=NEW.table_id;
		tab_name=NEW.table_name;
		
If Exists(Select * From information_schema.tables where table_schema=''||master_data_schecma||'' and table_name=''||tab_name||'') then
	      EXECUTE'SELECT count(*) FROM '||master_data_schecma||'."'|| tab_name ||'"  where "ID"='||tab_id||'' into count;
		  if count>0 then
				SQLQuery = 'INSERT INTO '||user_lock_schema||'.'||user_lock_table||'(table_id,user_id,user_name,user_type,status,ip_address,editable_table_name,db_ver) 
				Values('||tab_id||','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''',0,''Automatic'','''||tab_name||''','||new.db_ver||')';
				RAISE info 'UserLockSql1:%',SQLQuery;
				EXECUTE SQLQuery;
			    sremark = 'Success';
		        sStatus = 0;
			    SQLQuery1 = 'INSERT INTO '||log_schema_name||'.'||log_table_name||' (table_id,table_name,user_id,user_name,user_type,remark,status,db_ver) VALUES('||tab_id||','''||new.table_name||''','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''','''||sremark||''','||sstatus||','||new.db_ver||')';
	     	    Raise Info 'Success';
		        EXECUTE SQLQuery1;
				return new;
		 else
		    sremark = 'Table Id Not Found';
		    sStatus = 0;
		    SQLQuery1 = 'INSERT INTO '||log_schema_name||'.'||log_table_name||' (table_id,table_name,user_id,user_name,user_type,remark,status,db_ver) VALUES('||tab_id||','''||new.table_name||''','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''','''||sremark||''','||sstatus||','||new.db_ver||')';
			RAISE INFO 'UPDATE LOG:%',SQLQuery1;
			EXECUTE SQLQuery1;
		    Raise Info 'Table Id Not Found';
				return null;
		 end if;
		     Else
			    sremark = 'Table Id Not Found';
		        sStatus = 0;
				SQLQuery1 = 'INSERT INTO '||log_schema_name||'.'||log_table_name||' (table_id,table_name,user_id,user_name,user_type,remark,status,db_ver) VALUES('||tab_id||','''||new.table_name||''','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''','''||sremark||''','||sstatus||','||new.db_ver||')';
				RAISE INFO 'UPDATE LOG:%',SQLQuery1;
				EXECUTE SQLQuery1;
				Raise Info 'Table Id Not Found';
			    return null;
		End If;
	EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS	
		f1 = MESSAGE_TEXT,
		f2 = PG_EXCEPTION_CONTEXT;													 
		RAISE info 'Error Description:%',f1;
		RAISE info 'SQL Statement:%',f2;
		sremark = f1;
		sStatus = 0;
		SQLQuery1 = 'INSERT INTO '||log_schema_name||'.'||log_table_name||' (table_id,table_name,user_id,user_name,user_type,remark,status,db_ver) VALUES('||tab_id||','''||new.table_name||''','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''','''||sremark||''','||sstatus||','||new.db_ver||')';
		RAISE INFO 'UPDATE LOG:%',SQLQuery1;
		EXECUTE SQLQuery1;
		return null;
end;

$BODY$;

ALTER FUNCTION mmi_state_move.state_move_insert_pre()
    OWNER TO postgres;
