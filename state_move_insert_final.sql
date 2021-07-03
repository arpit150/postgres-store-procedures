-- FUNCTION: mmi_state_move.state_move_insert_final()

-- DROP FUNCTION mmi_state_move.state_move_insert_final();

CREATE FUNCTION mmi_state_move.state_move_insert_final()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF 
AS $BODY$

DECLARE f1 text; 
DECLARE f2 text;
DECLARE SQLQuery text;
DECLARE SQLQuery1 text;
DECLARE tab_id text;
DECLARE master_data_schecma text;
DECLARE user_lock_schema text;
DECLARE user_lock_table text;
DECLARE poi_ver integer;
DECLARE tab_name text;
DECLARE from_tab_name_el text;
DECLARE from_tab_name_ens text;
DECLARE from_tab_name_es text;
DECLARE state_cd integer;
DECLARE from_tab_poi text;
DECLARE log_state_move_pre_schecma text; 
DECLARE log_state_move_pre_table text;
DECLARE count integer;
DECLARE final_schecma text;
DECLARE final_table text;
DECLARE rollback_schecma text;
DECLARE rollback_table text;
DECLARE r record;
DECLARE to_tab_poi text;
DECLARE to_tab_name_el text;
DECLARE to_tab_name_ens text;
DECLARE to_tab_name_es text;
DECLARE colstring text;
DECLARE colstring_el text;
DECLARE colstring_ens text;
DECLARE colstring_es text;
DECLARE sqlq text;
DECLARE tab_name_el  text;
DECLARE tab_name_ens  text;
DECLARE tab_name_es  text;
DECLARE log_table_name text;
DECLARE log_schema_name text;
DECLARE sremark  text;
DECLARE sstatus text;
begin
      	master_data_schecma = 'mmi_master';
	  
	  	user_lock_schema = 'mmi_lock';
	  	user_lock_table = 'userlock';
		
		log_table_name='log_table';
		log_schema_name='mmi_state_move';
	 
		tab_id=NEW.table_id;
		tab_name=NEW.table_name;
		RAISE info 'table_name :%',tab_name;
		
		   -- from table name
		from_tab_poi = new.from_state_code||'_POI';
		from_tab_name_el = new.from_state_code||'_POI_EDGELINE';
		from_tab_name_ens = new.from_state_code||'_POI_ENTRYNONSHIFT';
		from_tab_name_es = new.from_state_code||'_POI_ENTRYSHIFT';
		
		  -- to table name
		to_tab_poi =new.to_state_code||'_POI';
		to_tab_name_el = new.to_state_code||'_POI_EDGELINE';
		to_tab_name_ens = new.to_state_code||'_POI_ENTRYNONSHIFT';
		to_tab_name_es = new.to_state_code||'_POI_ENTRYSHIFT';
		
		log_state_move_pre_schecma = 'mmi_state_move';
	    log_state_move_pre_table = 'log_state_move_pre';
		
		rollback_schecma = 'mmi_state_move';
	    rollback_table = 'log_state_move_rollback';
		
		final_schecma = 'mmi_state_move';
	    final_table = 'log_state_move_final';
		RAISE info 'Status :%',new.status;
		
		
		if(NEW.status not in (1,2)) then
			sremark = 'Status Not Valid It Must Be 1 and 2';
			sStatus = NEW.status;
		    SQLQuery1 = 'INSERT INTO '||log_schema_name||'.'||log_table_name||' (table_id,table_name,user_id,user_name,user_type,remark,status,db_ver) VALUES('||tab_id||','''||new.table_name||''','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''','''||sremark||''','||sstatus||','||new.db_ver||')';
			RAISE INFO 'UPDATE LOG:%',SQLQuery1;
			EXECUTE SQLQuery1;
			Raise Info 'Status Not Valid It Must Be 1 and 2 <%>',NEW.status;
			return null;
		end if;
	
		if(new.status=1) then
				EXECUTE 'INSERT INTO '||rollback_schecma||'.'||rollback_table||'(table_id, table_name, from_state_code, to_state_code, user_id, user_name, user_type, status, db_ver) 
					Values('||tab_id||','''||tab_name||''','''||new.from_state_code||''','''||new.to_state_code||''','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''','''||new.status||''','||new.db_ver||')';
				
			    --Delete From log_state_move_pre
			    EXECUTE 'Delete From '||log_state_move_pre_schecma||'."'|| log_state_move_pre_table ||'"  where "table_id"='||tab_id||'';
			
			    --Delete From user lock												 
			    EXECUTE 'Delete From '||user_lock_schema||'."'|| user_lock_table ||'"  where "table_id"='||tab_id||'';
			    sremark = 'Success';
		        sStatus = NEW.status;
			    SQLQuery1 = 'INSERT INTO '||log_schema_name||'.'||log_table_name||' (table_id,table_name,user_id,user_name,user_type,remark,status,db_ver) VALUES('||tab_id||','''||new.table_name||''','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''','''||sremark||''','||sstatus||','||new.db_ver||')';
	     	    Raise Info 'Success';
		        EXECUTE SQLQuery1;
	       		return new;
     	end if;
    
		if(new.status=2) then
			If Exists(Select * From information_schema.tables where table_schema=''||master_data_schecma||'' and table_name=''||tab_name||'') then
				 --Get Col String Without MI_PRINX 
				colstring = array_to_string(ARRAY(SELECT CASE WHEN COLUMN_NAME::text=UPPER(COLUMN_NAME::text) THEN '"'||COLUMN_NAME::text||'"' ELSE COLUMN_NAME::text END FROM information_schema.columns where table_name=''||from_tab_poi||'' AND table_schema=''||master_data_schecma||'' AND COLUMN_NAME NOT IN ('MI_PRINX')),',');
				RAISE info 'Col String POI:%',colstring;
				colstring_el = array_to_string(ARRAY(SELECT CASE WHEN COLUMN_NAME::text=UPPER(COLUMN_NAME::text) THEN '"'||COLUMN_NAME::text||'"' ELSE COLUMN_NAME::text END FROM information_schema.columns where table_name=''||from_tab_name_el||'' AND table_schema=''||master_data_schecma||'' AND COLUMN_NAME NOT IN ('MI_PRINX')),',');
				RAISE info 'Col String POI_EL:%',colstring_el;
				colstring_ens = array_to_string(ARRAY(SELECT CASE WHEN COLUMN_NAME::text=UPPER(COLUMN_NAME::text) THEN '"'||COLUMN_NAME::text||'"' ELSE COLUMN_NAME::text END FROM information_schema.columns where table_name=''||from_tab_name_ens||'' AND table_schema=''||master_data_schecma||'' AND COLUMN_NAME NOT IN ('MI_PRINX')),',');
				RAISE info 'Col String POI_ENS:%',colstring_ens;
				colstring_es = array_to_string(ARRAY(SELECT CASE WHEN COLUMN_NAME::text=UPPER(COLUMN_NAME::text) THEN '"'||COLUMN_NAME::text||'"' ELSE COLUMN_NAME::text END FROM information_schema.columns where table_name=''||from_tab_name_es||'' AND table_schema=''||master_data_schecma||'' AND COLUMN_NAME NOT IN ('MI_PRINX')),',');
				RAISE info 'Col String POI_ES:%',colstring_es;

				--Check Poi Record Exits Into Table Or Not
				 EXECUTE'SELECT count(*) FROM '||master_data_schecma||'."'|| tab_name ||'"  where "ID"='||tab_id||'' into count;
				 RAISE info 'Count:%',count;
				 if(count>0) then
						execute 'INSERT INTO '||final_schecma||'.'||final_table||'(table_id, table_name, from_state_code, to_state_code, user_id, user_name, user_type, status, db_ver) 
					    		Values('||tab_id||','''||tab_name||''','''||new.from_state_code||''','''||new.to_state_code||''','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''','''||new.status||''','||new.db_ver||')';
						--insert From Master Tables
						sqlq = 'insert into '||master_data_schecma||'."'||to_tab_poi||'"('||colstring||') Select '||colstring||' From '||master_data_schecma||'."'|| from_tab_poi ||'"  where "ID"='||tab_id||'';
						RAISE info 'Sql1:%',sqlq;
						EXECUTE sqlq;
						sqlq = 'insert into '||master_data_schecma||'."'||to_tab_name_el||'"('||colstring_el||') Select '||colstring_el||' From '||master_data_schecma||'."'|| from_tab_name_el ||'"  where "ID"='||tab_id||'';
						RAISE info 'Sql2:%',sqlq;
						EXECUTE sqlq;
						sqlq = 'insert into '||master_data_schecma||'."'||to_tab_name_ens||'"('||colstring_ens||') Select '||colstring_ens||' From '||master_data_schecma||'."'|| from_tab_name_ens ||'"  where "ID"='||tab_id||'';
						RAISE info 'Sql3:%',sqlq;
						EXECUTE sqlq;
						sqlq = 'insert into '||master_data_schecma||'."'||to_tab_name_es||'"('||colstring_es||') Select '||colstring_es||' From '||master_data_schecma||'."'|| from_tab_name_es ||'"  where "ID"='||tab_id||'';
						RAISE info 'Sql4:%',sqlq;
						EXECUTE sqlq;

						  --Delete From Master Tables
						sqlq = 'Delete From '||master_data_schecma||'."'|| tab_name ||'"  where "ID"='||tab_id||'';
						RAISE info 'DelSql1:%',sqlq;
						EXECUTE sqlq;
						sqlq = 'Delete From '||master_data_schecma||'."'|| from_tab_name_el ||'"  where "ID"='||tab_id||'';
						RAISE info 'DelSql1:%',sqlq;
						EXECUTE sqlq;
						sqlq = 'Delete From '||master_data_schecma||'."'|| from_tab_name_ens ||'"  where "ID"='||tab_id||'';
						RAISE info 'DelSql1:%',sqlq;
						EXECUTE sqlq;
						sqlq = 'Delete From '||master_data_schecma||'."'|| from_tab_name_es ||'"  where "ID"='||tab_id||'';
						RAISE info 'DelSql1:%',sqlq;
						EXECUTE sqlq;

						 --Update userlock													 
						sqlq = 'DELETE FROM mmi_lock.userlock Where table_id='||tab_id||'';
						RAISE info 'UserLockSql:%',sqlq;
						EXECUTE sqlq;
						sremark = 'Success';
		                sStatus = NEW.status;
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
		    end if;
		end if;	
    	RAISE info 'check';
		EXCEPTION WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS	
		f1 = MESSAGE_TEXT,
		f2 = PG_EXCEPTION_CONTEXT;													 
		RAISE info 'Error Description:%',f1;
		RAISE info 'SQL Statement:%',f2;
		sremark = f1;
		sStatus = NEW.status;
		SQLQuery1 = 'INSERT INTO '||log_schema_name||'.'||log_table_name||' (table_id,table_name,user_id,user_name,user_type,remark,status,db_ver) VALUES('||tab_id||','''||new.table_name||''','''||new.user_id||''','''||new.user_name||''','''||new.user_type||''','''||sremark||''','||sstatus||','||new.db_ver||')';
		RAISE INFO 'UPDATE LOG:%',SQLQuery1;
		EXECUTE SQLQuery1;
		return null;
end;

$BODY$;

ALTER FUNCTION mmi_state_move.state_move_insert_final()
    OWNER TO postgres;
