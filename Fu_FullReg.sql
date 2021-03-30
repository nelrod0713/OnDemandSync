--Procedimiento para sincromizar BDs Destino y Origen con los registros Modificados en  ambas BDs
create or replace PROCEDURE Fu_FullReg(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem character varying, 
  Pv_TableName character varying 
) 
language plpgsql    
AS $BODY$
declare 
Lv_cursor varchar;
Lc_Recs refcursor;
Lr_Recs RECORD; --record;  ojo
Lr_Audit RECORD; --ori.usuarios_log%ROWTYPE; --record;  ojo
Lr_Cols RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lr_Record RECORD; 
Lv_sql VARCHAR;
--Lv_instance VARCHAR;
Lv_CurrentDB VARCHAR;
begin 
  select current_database()
  into Lv_CurrentDB;
  --conexion a la BD remota para manejo de rollback
  perform dblink_connect('pg', 'dbname='||Pv_Instance||' user=postgres
  password=postnrt1964 host='||Pv_Host);
  Lv_sql := 'begin;';
  perform dblink_exec('pg', Lv_sql, false);

    --Actualizar registroa que no se deben procesar
  BEGIN
    Lv_Cursor = 'select orig.*'||
                  ' from '||Pv_SchemaLoc||'.v_'||Pv_TableName||'_aud_full_no orig ';
    raise notice E' cursor  no audit  ====> %\n', lv_cursor;    

    open Lc_Recs for execute Lv_Cursor; 
    fetch next from Lc_recs into Lr_Recs;
    while found 
    loop
      IF Lr_Recs.db_instance <>  Lv_CurrentDB THEN
        Lv_sql = 'UPDATE '||Pv_SchemaRem||'.'||Pv_TableName||'_log  set synced = now() '||
              'where secuencia =  '||Lr_Recs.Secuencia;
    --raise notice E' update  ====> % %\n',Lr_Recs.db_instance, lv_sql;    
        perform dblink_exec('pg', Lv_sql, false);
      ELSE
        Lv_sql = 'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||'_log  set synced = now() '||
              'where secuencia =  '||Lr_Recs.Secuencia;
    --raise notice E' update  ====> % %\n',Lr_Recs.db_instance, lv_sql;    
        EXECUTE Lv_sql;
      END IF;  
      fetch next from Lc_Recs into Lr_Recs; 
    end loop;
    close Lc_Recs;
    Lv_sql := 'commit;';
    perform dblink_exec('pg', Lv_sql, false);
    perform dblink_disconnect('pg');
    exception
      WHEN OTHERS THEN
        raise notice E' Error Actualizar registroS que no se deben procesar %s \n',sqlerrm;       
        Lv_sql := 'rollback;';
        perform dblink_exec('pg', Lv_sql, false);
        perform dblink_disconnect('pg');
        rollback;
        RETURN;
  END;    
--return;
  --conexion a la BD remota para manejo de rollback
  perform dblink_connect('pg', 'dbname='||Pv_Instance||' user=postgres
  password=postnrt1964 host='||Pv_Host);
  Lv_sql := 'begin;';
  perform dblink_exec('pg', Lv_sql, false);

  --Registros de auditoria de las BDs, pendientes de aplicar
  Lv_Cursor = 'select orig.*'||
                 ' from '||Pv_SchemaLoc||'.v_'||Pv_TableName||'_aud_full orig ';
  raise notice E' cursor  audit  ====> %\n', lv_cursor;    

  open Lc_Recs for execute Lv_Cursor; 
  fetch next from Lc_recs into Lr_Recs;
  while found 
  loop
    --raise notice 'Recs %', Lr_Recs; 
    --Si es un INSERT
    IF Lr_Recs.operation = 'I' THEN
      IF Lr_Recs.db_instance =  Lv_CurrentDB THEN
        --Sincrinizar destino
        call Fu_DesSyncNew(
          Pv_Instance,
          Pv_Host,
          Pv_SchemaLoc,
          Pv_SchemaRem, 
          Pv_TableName,
          Lr_Recs --Lr_Audit
        );
        Lv_Cursor = 'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||'_log  set synced = now() '||
                    'where secuencia =  '||Lr_Recs.Secuencia;
        execute Lv_Cursor;          
      ELSE -- Lr_Recs.db_instance <>  Lv_CurrentDB THEN
        --Sincrinizar Origen
        call Fu_OriSyncNew(
          Pv_Instance,
          Pv_Host,
          Pv_SchemaLoc,
          Pv_SchemaRem, 
          Pv_TableName,
          Lr_Recs --Lr_Audit
        );
        Lv_sql := 'UPDATE '||Pv_SchemaRem||'.'||Pv_TableName||'_log  set synced = now() '||
                    'where secuencia =  '||Lr_Recs.Secuencia;
        perform dblink_exec('pg', Lv_sql, false);
      END IF;           
    --Si es un DELETE
    ELSIF Lr_Recs.operation = 'D' THEN
      IF Lr_Recs.db_instance =  Lv_CurrentDB THEN
        --Sincrinizar destino
        call Fu_DesSyncDel(
          Pv_Instance,
          Pv_Host,
          Pv_SchemaLoc,
          Pv_SchemaRem, 
          Pv_TableName,
          Lr_Recs --Lr_Audit
        );
        Lv_Cursor = 'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||'_log  set synced = now() '||
                    'where secuencia =  '||Lr_Recs.Secuencia;
        execute Lv_Cursor;          
      ELSE -- Lr_Recs.db_instance <>  Lv_CurrentDB THEN
        --Sincrinizar Origen
        call Fu_OriSyncDel(
          Pv_Instance,
          Pv_Host,
          Pv_SchemaLoc,
          Pv_SchemaRem, 
          Pv_TableName,
          Lr_Recs --Lr_Audit
        );
        Lv_sql := 'UPDATE '||Pv_SchemaRem||'.'||Pv_TableName||'_log  set synced = now() '||
                    'where secuencia =  '||Lr_Recs.Secuencia;
        perform dblink_exec('pg', Lv_sql, false);
      END IF;  
    --Si es un UPDATE
    ELSIF Lr_Recs.operation = 'U' THEN
      IF Lr_Recs.db_instance =  Lv_CurrentDB THEN
        --Sincrinizar destino
          call Fu_DesSyncUpd(
            Pv_Instance,
            Pv_Host,
            Pv_SchemaLoc,
            Pv_SchemaRem, 
            Pv_TableName,
            Lr_Recs --Lr_Audit
          );
      ELSE -- Lr_Recs.db_instance <>  Lv_CurrentDB THEN
        --Sincrinizar Origen
          call Fu_OriSyncUpd(
            Pv_Instance,
            Pv_Host,
            Pv_SchemaLoc,
            Pv_SchemaRem, 
            Pv_TableName,
            Lr_Recs --Lr_Audit
          );
      END IF;  
    END IF;

    fetch next from Lc_Recs into Lr_Recs; 
  end loop;
  close Lc_Recs;
  Lv_sql := 'commit;';
  perform dblink_exec('pg', Lv_sql, false);
  perform dblink_disconnect('pg');
exception
  WHEN OTHERS THEN
    raise notice E' Error Fu_OriReg %s \n',sqlerrm;       
    Lv_sql := 'rollback;';
    perform dblink_exec('pg', Lv_sql, false);
    perform dblink_disconnect('pg');
    rollback;
END $BODY$;
