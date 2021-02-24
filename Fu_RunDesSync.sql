--Procedimiento para correr la sincronizacion de la BD Destino
create or replace procedure Fu_RunDesSync(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem VARCHAR, 
  Pv_TableName VARCHAR
)
language plpgsql    
AS $BODY$
declare 
  Lc_SyncType char(1) := 'D'; --Sincronizar Destino 
begin
  -- sincronizar reg nuevos Destino
  BEGIN
    call Fu_DesSyncNew(
      Pv_Instance,
      Pv_Host,
      Pv_SchemaLoc,
      Pv_SchemaRem, 
      Pv_TableName
    );
    Raise notice E'Paso nuevos \n';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE NOTICE 'Error F_OriSyncNew % \n ',SQLERRM;
      rollback;
  END;
  -- sincronizar reg actualizados Destino
  BEGIN
    call Fu_DesSyncUpd(
      Pv_Instance,
      Pv_Host,
      Pv_SchemaLoc,
      Pv_SchemaRem, 
      Pv_TableName
    );
    Raise notice E'Paso update \n';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE NOTICE 'Error F_OriSyncNew % \n ',SQLERRM;
      rollback;
  END;
  -- sincronizar reg eliminados
  BEGIN
    perform Fu_DesSyncDel(
      Pv_Instance,
      Pv_Host,
      Pv_SchemaLoc,
      Pv_SchemaRem, 
      Pv_TableName);
    Raise notice E'Paso delete  \n';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE NOTICE 'Error F_OriSyncNew % \n ',SQLERRM;
      rollback;
  END;

--Generar log del proceso de sincronizacion
  BEGIN
    call F_ProcsLog(
      Pv_SchemaLoc ,
      Pv_TableName,
      Lc_SyncType 
    );
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE NOTICE 'Error F_OriSyncNew % \n ',SQLERRM;
      rollback;
  END;
end $BODY$;
