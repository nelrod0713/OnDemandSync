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
  Pv_Operation VARCHAR='F';
  Pr_Reg ori.v_facturacion_log%ROWTYPE;

begin
-- llamar proceso sincroniza origen
call Fu_RunDesSync(
  Pv_Instance,
  Pv_Host,
  Pv_SchemaLoc ,
  Pv_SchemaRem , 
  Pv_TableName 
);
Raise notice E'termino sync \n';
exception 
  when others then 
    Raise notice E' Error en el proceso % \n', SQLERRM;
end;
$$