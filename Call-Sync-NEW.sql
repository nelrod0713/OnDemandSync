DO $$
DECLARE
  Pv_Instance varchar = 'bd_des';
  Pv_Host VARCHAR = 'localhost'; --'192.168.1.108';
  Pv_SchemaLoc VARCHAR = 'ori';
  Pv_SchemaRem varchar = 'des'; 
  Pr_Old ori.usuarios%ROWTYPE;
  --Pv_Schema VARCHAR = 'ori';
  --Pv_TableName VARCHAR= 'facturacion';
  Pv_TableName VARCHAR= 'usuarios';
  Pv_Operation VARCHAR='F';
  Pr_Reg ori.v_facturacion_log%ROWTYPE;
  v_sql varchar;
  v_int int;
  id_comp int =2;

begin
  select Fu_CallSync(id_comp,Pv_TableName,'F') into v_sql;
  Raise notice E'Paso Sync full %\n',v_sql;
end;
$$