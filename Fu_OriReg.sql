--Procedimiento para sincromizar BD Origen con los registros Modificados en la BD Origen
create or replace PROCEDURE Fu_OriReg(
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
--Lc_RegAud refcursor;
Lr_Recs RECORD; --record;  ojo
Lr_Users ori.usuarios_log%ROWTYPE; --record;  ojo
Lr_UsersCol ori.usuarios_log_col%ROWTYPE; --record;  ojo
Lr_Fact ori.facturacion_log%ROWTYPE; --record;  ojo
Lr_FactCol ori.facturacion_log_col%ROWTYPE; --record;  ojo
Lr_Cols RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lr_Record RECORD; 
Lv_sql varchar;

--Lv_instance VARCHAR;
--Lv_CurrentDB VARCHAR;
begin 
  --conexion a la BD remota para manejo de rollback
  perform dblink_connect('pg', 'dbname='||Pv_Instance||' user=postgres
  password=postnrt1964 host='||Pv_Host);
  Lv_sql := 'begin;';
  perform dblink_exec('pg', Lv_sql, false);
  --Registros de auditoria en la BD Destino, pendientes de aplicar En Origen
  Lv_Cursor = 'select orig.*'||
                 ' from '||Pv_SchemaLoc||'.v_'||Pv_TableName||'_aud_des orig ';
  raise notice E' cursor  audit  ====> %\n', lv_cursor;    

  open Lc_Recs for execute Lv_Cursor; 
  fetch next from Lc_recs into Lr_Recs;
  while found 
  loop
    raise notice 'Recs %', Lr_Recs; 
    --Si es un INSERT
    IF Lr_Recs.operation = 'I' THEN
      Lv_Cursor = 'select orig.*'||
                 ' from '||Pv_SchemaLoc||'.v_'||Pv_TableName||'_log orig '||
                  'where orig.secuencia =  '||Lr_Recs.Secuencia;
        IF Pv_TableName = 'usuarios' THEN          
          execute Lv_Cursor into Lr_Users;          
          call Fu_OriSyncNew(
            Pv_Instance,
            Pv_Host,
            Pv_SchemaLoc,
            Pv_SchemaRem, 
            Pv_TableName,
            Lr_Users
          );
        ELSIF Pv_TableName = 'facturacion' THEN          
          execute Lv_Cursor into Lr_Fact;          
          call Fu_OriSyncNew(
            Pv_Instance,
            Pv_Host,
            Pv_SchemaLoc,
            Pv_SchemaRem, 
            Pv_TableName,
            Lr_Fact
          );
        END IF;           
    --Si es un DELETE
    ELSIF Lr_Recs.operation = 'D' THEN
      Lv_Cursor = 'select orig.*'||
                 ' from '||Pv_SchemaLoc||'.v_'||Pv_TableName||'_log orig '||
                  'where orig.secuencia =  '||Lr_Recs.Secuencia;
        IF Pv_TableName = 'usuarios' THEN          
          execute Lv_Cursor into Lr_Users;          
          call Fu_OriSyncDel(
            Pv_Instance,
            Pv_Host,
            Pv_SchemaLoc,
            Pv_SchemaRem, 
            Pv_TableName,
            Lr_Users
          );
        ELSIF Pv_TableName = 'facturacion' THEN          
          execute Lv_Cursor into Lr_Fact;          
          call Fu_OriSyncDel(
            Pv_Instance,
            Pv_Host,
            Pv_SchemaLoc,
            Pv_SchemaRem, 
            Pv_TableName,
            Lr_Fact
          );
        END IF;           
    --Si es un UPDATE
    ELSIF Lr_Recs.operation = 'U' THEN
      IF Lr_Recs.db_instance = 'bd_ori' THEN
        Lv_Cursor = 'select orig.*'||
                  ' from '||Pv_SchemaLoc||'.'||Pv_TableName||'_log_col orig '||
                    'where orig.secuencia =  '||Lr_Recs.Secuencia;
      ELSE
        Lv_Cursor = 'select orig.*'||
                  ' from '||Pv_SchemaLoc||'.v_'||Pv_TableName||'_log_col orig '||
                    'where orig.secuencia =  '||Lr_Recs.Secuencia;
      END IF;
      IF Pv_TableName = 'usuarios' THEN          
        execute Lv_Cursor into Lr_UsersCol;          
        call Fu_OriSyncUpd(
          Pv_Instance,
          Pv_Host,
          Pv_SchemaLoc,
          Pv_SchemaRem, 
          Pv_TableName,
          Lr_UsersCol
        );
      ELSIF Pv_TableName = 'facturacion' THEN          
        execute Lv_Cursor into Lr_FactCol;          
        call Fu_OriSyncUpd(
          Pv_Instance,
          Pv_Host,
          Pv_SchemaLoc,
          Pv_SchemaRem, 
          Pv_TableName,
          Lr_FactCol
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
