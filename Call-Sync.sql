DO $$
DECLARE
  Pv_Instance varchar = 'bd_des';
  Pv_Host VARCHAR = '192.168.1.108';
  Pv_SchemaLoc VARCHAR = 'ori';
  Pv_SchemaRem varchar = 'des'; 
  Pr_Old ori.usuarios%ROWTYPE;
  Pv_Schema VARCHAR = 'ori';
  --Pv_TableName VARCHAR= 'facturacion';
  Pv_TableName VARCHAR= 'usuarios';
  Pv_Operation VARCHAR='D';
  Pr_Reg ori.v_facturacion_log%ROWTYPE;
  v_sql varchar;
  v_int int;

begin
I Pv_Operation = 'D' THEN
  -- sincronizar reg en Destino
  call Fu_DesReg(
    Pv_Instance,
    Pv_Host,
    Pv_SchemaLoc,
    Pv_SchemaRem, 
    Pv_TableName 
  );
  Raise notice E'Paso Sync Des \n';

ELSIF Pv_Operation = 'O' THEN
  -- sincronizar reg en  Origen
  call Fu_OriReg(
    Pv_Instance,
    Pv_Host,
    Pv_SchemaLoc,
    Pv_SchemaRem, 
    Pv_TableName 
  );
  Raise notice E'Paso Sync Ori \n';
ELSIF Pv_Operation = 'F' THEN
  -- sincronizar reg en ambas BDs
  call Fu_FullReg(
    Pv_Instance,
    Pv_Host,
    Pv_SchemaLoc,
    Pv_SchemaRem, 
    Pv_TableName 
  );
  Raise notice E'Paso Sync full \n';
END IF;
end;
$$