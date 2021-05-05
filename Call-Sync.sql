DO $$
DECLARE
  Pv_TableName VARCHAR= 'usuarios';
  Pv_Operation VARCHAR='O';
  v_sql varchar;
  id_comp int =1;

begin
  select Fu_CallSync(id_comp,Pv_TableName,Pv_Operation) into v_sql;
  Raise notice E'Paso Sync full %\n',v_sql;
end;
$$