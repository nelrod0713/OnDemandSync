--Procedimiento para sincromizar BD Destino con registros actuaizados de la BD Origen
create or replace procedure Fu_DesSyncUpd(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem character varying, 
  Pv_TableName character varying,
  Pr_Reg RECORD
)
language plpgsql    
AS $BODY$
declare 
Lv_cursor varchar;
Lc_Users refcursor;
Lc_RegAud refcursor;
--Lr_users ori.facturacion%ROWTYPE; --record;
Lr_users ori.usuarios%ROWTYPE; --record; ojo
Lr_Cols RECORD;
--Lr_userDes ori.facturacion_log_col%ROWTYPE;
Lr_userDes record; --ori.usuarios_log_col%ROWTYPE; --ojo
--Lr_userDes RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_instance VARCHAR;
Lv_CurrentDB VARCHAR;
Lv_column VARCHAR;
--Lr_Tabla RECORD;
--Lv_text TEXT;
--Lv_SchemaLocal VARCHAR = 'ori';
begin 
  select db_instance
    into Lv_instance
    from companys --v_companys
  where id_company = Pr_Reg.id_company;

  select current_database()
    into Lv_CurrentDB;

  select   Fu_TableUpdComand(Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName , Pr_Reg,'U' )
    into Lv_comando; 

  --raise notice E' comando update ====> %\n', lv_comando;    
  EXECUTE Lv_comando ;
  --Valida si se actualiza la BD Local o la Remota
  IF Pr_Reg.db_instance = Lv_CurrentDB THEN
      --Actualizar la fecha de sincronizacion del log local
    Lv_comando =  'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||'_log_col  SET synced= now() ';
    --armar el where               
    FOR Lr_Cols IN
      SELECT k.ordinal_position, k.column_name, c.data_type
      FROM information_schema.key_column_usage k,
          information_schema.columns c
      WHERE	k.constraint_catalog = c.table_catalog
        and k.constraint_schema = c.table_schema
        and k.table_name = c.table_name
        and k.column_name = c.column_name
        and k.constraint_schema = Pv_SchemaLoc
        AND k.table_name = Pv_TableName||'_log_col' 
      ORDER BY ordinal_position
      LOOP
      --raise notice E'  col ====> %\n', Lr_Cols.column_name;    
      --  EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Pr_Reg;
      Lv_column = Lr_Cols.column_name;
      select Fu_GetValColumn(Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName||'_log_col' , Lv_column ,  Pr_Reg.secuencia ) 
        into Lv_Texto;       
      --raise notice E'  texto ====> %\n', Lv_Texto;    
        if Lr_Cols.ordinal_position = 1 then
          if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character') Then
            Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
          else
            Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||Lv_Texto;
          end if;
        else
          if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character') Then
            Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
          else
            Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||Lv_Texto;
          end if;
        end if;  
      END LOOP;

    EXECUTE Lv_comando ;

  END IF;

end $BODY$;

--Procedimiento para sincromizar BD Destino con los registros eliminados de la BD Origen
create or replace procedure Fu_DesSyncDel(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem character varying, 
  Pv_TableName character varying, 
  Pr_Reg RECORD
) 
language plpgsql    
AS $BODY$
declare 
Lv_cursor varchar;
--Lc_Users refcursor;
--Lc_RegAud refcursor;
--Lr_users record; --ori.usuarios_log%ROWTYPE; --record;  ojo
--Lr_users ori.facturacion_log%ROWTYPE; --record;
Lr_Cols RECORD;
--Lr_userDes ori.usuarios_log_col%ROWTYPE;
--Lr_userDes RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_instance VARCHAR;
Lv_CurrentDB VARCHAR;
--Lr_Tabla RECORD;
--Lv_text TEXT;
--Lv_SchemaLocal VARCHAR = 'ori';
--Lr_usuarios ori.usuarios_log%ROWTYPE;  --ojo
--Lr_fact ori.facturacion_log%ROWTYPE;  --ojo
begin 
  --Registros elimiandos de la BD Origen
    Pr_Reg.updated_function= 'F_OriSync';
    Pr_Reg.synced= now();

    --raise notice '%', Lr_users; 
    select db_instance
      into Lv_instance
      from companys --v_companys
    where id_company = Pr_Reg.id_company;

    select current_database()
      into Lv_CurrentDB;

    --Armar el comando para borrar registros de la BD Destino
    select   Fu_TableUpdComand(Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName , Pr_Reg,'D' )
      into Lv_comando; 

    --raise notice E' comando DELETE ====> %\n', lv_comando;    
    EXECUTE Lv_comando ;
    --Actualizar la fecha de sincronizacion de la auditoria de la BD Origen
    select   Fu_LocAudUpdComand(Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName||'_log' , Pr_Reg )
    into Lv_comando; 
    --raise notice E' comando update ====> %\n', lv_comando;    
    EXECUTE Lv_comando ;

END $BODY$;   

--Procedimiento para sincromizar BD Destino con los registros nuevos de la BD Origen
create or replace procedure Fu_DesSyncNew(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem character varying, 
  Pv_TableName character varying,
  Pr_Reg RECORD
)
language plpgsql    
AS $BODY$
declare 
Lv_cursor varchar;
--Lc_Users refcursor;
--Lc_RegAud refcursor;
--Lr_users ori.facturacion_log%ROWTYPE; --record;
--Lr_users RECORD; --ori.usuarios_log%ROWTYPE; --record;
Lr_Cols RECORD;
--Lr_userDes ori.usuarios_log_col%ROWTYPE;
--Lr_userDes RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_instance VARCHAR;
Lv_CurrentDB VARCHAR;
Ln_Cant integer;
--Lr_Tabla RECORD;
--Lv_text TEXT;
--Lv_SchemaLocal VARCHAR = 'ori';
begin 
    Pr_reg.updated_function= 'F_OriSync';
    Pr_reg.synced= now();

    --raise notice '%', Lr_users; 
    select db_instance
      into Lv_instance
      from companys --v_companys
    where id_company = Pr_Reg.id_company;

    select current_database()
      into Lv_CurrentDB;
    --validar si el registro existe
    select Fu_TableRecCount(  Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName ,  Pr_Reg )
      into Lv_comando; 
    EXECUTE Lv_comando  into Ln_Cant;  
    --raise notice ' Ln_Cant %', Ln_Cant; 

if Ln_Cant = 0 then
    --Armar el comando para Insertar en la BD Destino los regIstros NUEVOS de la BD Origen
    select   Fu_TableInsComand(Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName , Pr_Reg) --Lr_users)
      into Lv_comando; 

    EXECUTE Lv_comando ;
    --Actualizar la fecha de sincronizacion del log Origen
    select   Fu_LocAudUpdComand(Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName||'_log' , Pr_Reg) --Lr_Users )
    into Lv_comando; 
    EXECUTE Lv_comando ;
end if;    

end $BODY$;



--Funcion para Actualizar la tabla remota
create or replace FUNCTION Fu_TableUpdComand(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem character varying, 
  Pv_TableName character varying,
  Pr_Reg RECORD,
  Pc_OperType character(1) --U Update D Delete
) RETURNS VARCHAR AS $$
DECLARE
--Lv_cursor varchar;
--Lc_Users refcursor;
--Lc_RegAud refcursor;
--Lr_users ori.usuarios_log%ROWTYPE; --record;
Lr_Cols RECORD;
--Lr_userDes ori.usuarios_log_col%ROWTYPE;
--Lr_userDes RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_column VARCHAR;
--Lv_instance VARCHAR;
--Lv_CurrentDB VARCHAR;
--Lv_Texto TEXT;
--Lr_Tabla RECORD;
--Lv_SchemaLocal VARCHAR = 'ori';
begin 
  DISCARD PLANS;

  IF Pc_OperType = 'U' THEN
     Lv_comando = 'select dblink_exec(''dbname='||Pv_Instance||
      ' host='||Pv_host||' user=postgres password=postnrt1964'',
      ''UPDATE '||Pv_SchemaRem||'.'||Pv_TableName||' SET ';
     If   Pr_Reg.campo = 'synced' THEN
       Lv_comando =  Lv_comando||' '||Pr_Reg.campo||' = '||CHR(39)||CHR(39)||Pr_Reg.valor||CHR(39)||CHR(39);
     ELSE
       Lv_comando =  Lv_comando||' '||Pr_Reg.campo||' = '||CHR(39)||CHR(39)||Pr_Reg.valor||CHR(39)||CHR(39)||', synced= now(), updated_function= '||CHR(39)||chr(39)||'F_OriSync'||CHR(39)||chr(39);
     END IF;
  ELSIF Pc_OperType = 'D' THEN
     Lv_comando = 'select dblink_exec(''dbname='||Pv_Instance||
      ' host='||Pv_host||' user=postgres password=postnrt1964'',
      ''DELETE FROM '||Pv_SchemaRem||'.'||Pv_TableName;
  END IF;
      --raise notice E' 1 comando update ====> %\n', lv_comando;    
  --armar el where               
  FOR Lr_Cols IN
      SELECT k.ordinal_position, k.column_name, c.data_type
      FROM information_schema.key_column_usage k,
          information_schema.columns c
      WHERE	k.constraint_catalog = c.table_catalog
        and k.constraint_schema = c.table_schema
        and k.table_name = c.table_name
        and k.column_name = c.column_name
        and k.constraint_schema = Pv_SchemaLoc
        AND k.table_name = Pv_TableName
      ORDER BY ordinal_position
  LOOP
    --EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Pr_Reg;
    Lv_column = Lr_Cols.column_name;
    IF Pc_OperType = 'U' THEN
    select Fu_GetValColumn(Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName||'_log_col' , Lv_column ,  Pr_Reg.secuencia ) 
      into Lv_Texto;       
    ELSIF Pc_OperType = 'D' THEN
    select Fu_GetValColumn(Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName||'_log' , Lv_column ,  Pr_Reg.secuencia ) 
      into Lv_Texto;       
    END IF;
    if Lv_texto is Null then
      Lv_texto = 'null';
    end if;
--raise notice E' where ====> % % \n', Lr_Cols.column_name,Lv_Texto;    
    if Lr_Cols.ordinal_position = 1 then
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character') Then
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||chr(39)||chr(39)||Lv_Texto||chr(39)||chr(39);
      else
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    else
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character') Then
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||chr(39)||chr(39)||Lv_Texto||chr(39)||chr(39);
      else
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    end if;  
  END LOOP;

  Lv_comando = Lv_comando||chr(39)||')';
-------------------------------------------------------------------------
  --raise notice E' comando Oper % ====> %\n', Pc_OperType, lv_comando;    
  RETURN Lv_comando ;
end;
$$ LANGUAGE plpgsql VOLATILE;

--Funcion para Insertar en  la tabla remota
create or replace FUNCTION Fu_TableInsComand(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem character varying, 
  Pv_TableName character varying,
  Pr_Reg RECORD
) RETURNS VARCHAR AS $$
DECLARE
Lr_Cols RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_column VARCHAR;
begin 
  DISCARD PLANS;
  Lv_comando = 'select dblink_exec('||chr(39)||'dbname='||Pv_Instance||
  ' host='||Pv_Host||' user=postgres password=postnrt1964'||chr(39)||','||chr(39)||
  'insert into '||Pv_SchemaRem||'.'||Pv_TableName||' (';
  --id_company,id_user, db_instance) VALUES (';
  FOR Lr_Cols IN
      SELECT ordinal_position, column_name
      FROM information_schema.columns
      WHERE
          table_schema = Pv_SchemaLoc
      AND table_name = Pv_TableName
      ORDER BY ordinal_position
  LOOP
    --EXECUTE 'SELECT ($1).' || ri.column_name || '::text' INTO t USING OLD;
    if Lr_Cols.ordinal_position = 1 then
      Lv_comando = Lv_comando||' '||Lr_Cols.column_name;
    else
      Lv_comando = Lv_comando||','||Lr_Cols.column_name;
    end if;  

  END LOOP;
  Lv_comando = Lv_comando||') Values (';
--raise notice E' insert ====> % \n', Lv_comando;    
  FOR Lr_Cols IN
      SELECT ordinal_position, column_name, data_type
      FROM information_schema.columns
      WHERE
          table_schema = Pv_SchemaLoc
      AND table_name = Pv_TableName
      ORDER BY ordinal_position
  LOOP
    --EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Pr_Reg;
    Lv_column = Lr_Cols.column_name;
    select Fu_GetValColumn(Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName||'_log' , Lv_column ,  Pr_Reg.secuencia ) 
      into Lv_Texto;       
    if Lv_texto is Null then
      Lv_texto = 'null';
      Lr_Cols.data_type = 'int';
    end if;
    if Lv_column = 'synced' then
      select now() into Lv_Texto;
      Lr_Cols.data_type = 'date';
      --raise notice E' texto % % %\n',Lr_Cols.ordinal_position, Lr_Cols.data_type, Lv_texto;
    end if;
    if Lr_Cols.ordinal_position = 1 then
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character','money') Then
        Lv_comando = Lv_comando||' '||chr(39)||chr(39)||Lv_Texto||chr(39)||chr(39);
      else  
        Lv_comando = Lv_comando||' '||Lv_Texto;
      end if;  
    else
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character','money') Then
        Lv_comando = Lv_comando||','||chr(39)||chr(39)||Lv_Texto||chr(39)||chr(39);
      else  
        Lv_comando = Lv_comando||','||Lv_Texto;
      end if;  
    end if;  

  END LOOP;
      Lv_comando = Lv_comando||' ) '||chr(39)||')';

  RETURN Lv_comando ;
end;
$$ LANGUAGE plpgsql VOLATILE;

--Funcion para Actualizar la fecha de sincronizacion de la auditoria Origen 
create or replace FUNCTION Fu_LocAudUpdComand(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem character varying, 
  Pv_TableName character varying,
  Pr_Reg RECORD
) RETURNS VARCHAR AS $$
DECLARE
Lr_Cols RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_column VARCHAR;
 begin 
--Actualizar la fecha de sincronizacion del log 
  Lv_comando = 'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||' SET synced= now() ';
  FOR Lr_Cols IN
      SELECT k.ordinal_position, k.column_name, c.data_type
      FROM information_schema.key_column_usage k,
          information_schema.columns c
      WHERE	k.constraint_catalog = c.table_catalog
        and k.constraint_schema = c.table_schema
        and k.table_name = c.table_name
        and k.column_name = c.column_name
        and k.constraint_schema = Pv_SchemaLoc
        AND k.table_name = Pv_TableName
      ORDER BY ordinal_position
  LOOP
    --EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Pr_Reg;
    Lv_column = Lr_Cols.column_name;
    select Fu_GetValColumn(Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName , Lv_column ,  Pr_Reg.secuencia ) 
      into Lv_Texto;       
    if Lr_Cols.ordinal_position = 1 then
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character') Then
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
      else
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    else
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character') Then
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
      else
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    end if;  
  END LOOP;

  --raise notice E' comando update aud ====> %\n', lv_comando;    
  RETURN Lv_comando ;
end;
$$ LANGUAGE plpgsql VOLATILE;

--Funcion para validar si el registro exiate
create or replace FUNCTION Fu_TableRecCount(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem character varying, 
  Pv_TableName character varying,
  Pr_Reg RECORD
) RETURNS VARCHAR AS $$
DECLARE
Lr_Cols RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_column VARCHAR;
begin 
  DISCARD PLANS;

  --Lv_comando = 'select count(*) from dblink('||chr(39)||'dbname='||Lv_instance||
  --            ' host='||Lv_host||' user=postgres password=postnrt1964'||chr(39)||','||chr(39)||'select ';


  Lv_comando = 'select count(*) FROM '||Pv_SchemaLoc||'.v_'||Pv_TableName;
  --raise notice E' 1 comando update ====> %\n', lv_comando;    
  --armar el where               
  FOR Lr_Cols IN
      SELECT k.ordinal_position, k.column_name, c.data_type
      FROM information_schema.key_column_usage k,
          information_schema.columns c
      WHERE	k.constraint_catalog = c.table_catalog
        and k.constraint_schema = c.table_schema
        and k.table_name = c.table_name
        and k.column_name = c.column_name
        and k.constraint_schema = Pv_SchemaLoc
        AND k.table_name = Pv_TableName
      ORDER BY ordinal_position
  LOOP
    --EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Pr_Reg;
    Lv_column = Lr_Cols.column_name;
    select Fu_GetValColumn(Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName||'_log' , Lv_column ,  Pr_Reg.secuencia ) 
      into Lv_Texto;       
    if Lv_texto is Null then
      Lv_texto = 'null';
    end if;
--raise notice E' where ====> % % \n', Lr_Cols.column_name,Lv_Texto;    
    if Lr_Cols.ordinal_position = 1 then
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character') Then
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||chr(39)||chr(39)||Lv_Texto||chr(39)||chr(39);
      else
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    else
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character') Then
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||chr(39)||chr(39)||Lv_Texto||chr(39)||chr(39);
      else
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    end if;  
  END LOOP;

-------------------------------------------------------------------------
  --raise notice E' comando count  ====> %\n', lv_comando;    
  RETURN Lv_comando ;
end;
$$ LANGUAGE plpgsql VOLATILE;

