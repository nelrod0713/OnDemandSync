create or replace FUNCTION Fu_CallSync(
  Pn_IdCompany INTEGER, --Codigo de la compañia a sincronizar
  Pv_TableName VARCHAR, 
  Pv_Operation VARCHAR  --D- Destino, O Origen, F-Full
) RETURNS VARCHAR AS $$
DECLARE
  Lv_Instance varchar; -- 'bd_des';
  Lv_Host VARCHAR; -- = 'localhost'; 
  Lv_SchemaLoc VARCHAR; --'ori';
  Pv_SchemaRem varchar; -- 'des'; 
  Ln_idCompanyC integer;
  v_sql varchar;
  v_int int;

begin
  BEGIN
    SELECT id_company, schema_name into Ln_idCompanyC,Lv_SchemaLoc
      FROM companys where db_central = 'S';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN  
      Raise notice E'Compañia central no existe % \n',Pn_IdCompany;
      Lv_Instance = NULL;
      RETURN 'Compañia central no existe';
  END;  
      Raise notice E'Ln_idCompanyC % Lv_SchemaLoc %\n',Ln_idCompanyC,Lv_SchemaLoc;
  if Ln_idCompanyC = Pn_IdCompany THEN
      Raise notice E'Compañia no puede ser la central % \n',Pn_IdCompany;
      Lv_Instance = NULL;
      RETURN 'Compañia no puede ser la central';
  end if;
      Raise notice E'Pn_idCompany % \n',Pn_idCompany;
  BEGIN
    SELECT db_instance, host, schema_name into Lv_Instance, Lv_Host, Pv_SchemaRem
      FROM companys where id_company = Pn_IdCompany;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN  
      Raise notice E'Compañia no existe % \n',Pn_IdCompany;
      Lv_Instance = NULL;
      RETURN 'Compañia no existe';
  END;  
      Raise notice E'paso Lv_Instance % \n',Lv_Instance;
 
  IF Lv_Instance IS NOT NULL THEN
    IF Pv_Operation = 'D' THEN
      -- sincronizar reg en Destino
      call Fu_DesReg(
        Pn_IdCompany,
        Lv_Instance,
        Lv_Host,
        Lv_SchemaLoc,
        Pv_SchemaRem, 
        Pv_TableName 
      );
      Raise notice E'Paso Sync Des \n';

    ELSIF Pv_Operation = 'O' THEN
      -- sincronizar reg en  Origen
      call Fu_OriReg(
        Pn_IdCompany,
        Lv_Instance,
        Lv_Host,
        Lv_SchemaLoc,
        Pv_SchemaRem, 
        Pv_TableName 
      );
      Raise notice E'Paso Sync Ori \n';
    ELSIF Pv_Operation = 'F' THEN
      -- sincronizar reg en ambas BDs
      Raise notice E'Por full Lv_SchemaLoc \%n',Lv_SchemaLoc;
      call Fu_FullReg(
        Pn_IdCompany,
        Lv_Instance,
        Lv_Host,
        Lv_SchemaLoc,
        Pv_SchemaRem, 
        Pv_TableName 
      );
      Raise notice E'Paso Sync full \n';
    END IF;
    call F_ProcsLog(
      Pn_IdCompany,
      Lv_SchemaLoc ,
      Pv_TableName,
      Pv_Operation 
    );
  END IF; -- db_instance IS NOT NULL
  return 'TERMINO';
exception
  WHEN OTHERS THEN
    raise notice E' Error Fu_RemAudUpdComand %s \n',sqlerrm;       
    rollback;
    return SQLERRM;
end;
$$ LANGUAGE plpgsql VOLATILE;
