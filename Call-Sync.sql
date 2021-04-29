DO $$
DECLARE
  Pn_IdCompany INTEGER =2; --Codigo de la compañia a sincronizar
  Pv_Instance varchar; -- 'bd_des';
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

begin
  BEGIN
    SELECT db_instance, host, schema_name into Pv_Instance, Pv_Host, Pv_SchemaRem
      FROM companys where id_company = Pn_IdCompany;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN  
      Raise notice E'Compañia no existe % \n',Pn_IdCompany;
      Pv_Instance = NULL;
      RETURN;
  END;  
If Pv_Instance is not null Then
  IF Pv_Operation = 'D' THEN
    -- sincronizar reg en Destino
    call Fu_DesReg(
        Pn_IdCompany,
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
        Pn_IdCompany,
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
        Pn_IdCompany,
      Pv_Instance,
      Pv_Host,
      Pv_SchemaLoc,
      Pv_SchemaRem, 
      Pv_TableName 
    );
    Raise notice E'Paso Sync full \n';
  END IF;
  call F_ProcsLog(
    Pv_SchemaLoc ,
    Pv_TableName,
    Pv_Operation 
  );
End If; -- Lv_Instance is not null Then
end;
$$