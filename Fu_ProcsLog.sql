--Procedimiento para actualizar log de procesos de sincronizacion
create or replace procedure F_ProcsLog(
  Pv_Schema VARCHAR,
  Pv_TableName VARCHAR,
  Pv_Operation char(1)
)
language plpgsql    
AS $BODY$
declare 
Lv_comando VARCHAR;
begin 
  --Crear un  registro en la tabla de logs
  begin
    Lv_comando = 'INSERT INTO '||Pv_Schema||'.Sync_procs_log (schema_table, table_name, sync_type, stamp, user_proc)';
    Lv_comando = Lv_comando||' VALUES ('||chr(39)||Pv_Schema||chr(39)||','||chr(39)||Pv_TableName||chr(39)||','||chr(39)||Pv_Operation||chr(39)||','||chr(39)||NOW()||chr(39)||','||chr(39)|| user||chr(39)||')';
      --RAISE NOTICE 'COMANDO % ',lv_comando;
	EXECUTE Lv_Comando;  

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE NOTICE 'Error insertando registro ';
      rollback;
	end;
end $BODY$;