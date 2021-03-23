--Procedimiento para sincromizar BD Origen con registros actuaizados de la BD Destino
create or replace procedure Fu_OriSyncUpd(
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
Lr_Cols RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_instance VARCHAR;
Lv_CurrentDB VARCHAR;
Lv_column VARCHAR;
begin 

  --raise notice 'reg.fecha %', Lr_users; 
  select db_instance
    into Lv_instance
    from companys --v_companys
  where id_company = Pr_Reg.id_company;

  select current_database()
    into Lv_CurrentDB;

  If   Pr_Reg.campo = 'synced' THEN
    Lv_comando =  'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||' SET '||
                  Pr_Reg.campo||' = '||CHR(39)||Pr_Reg.valor||CHR(39);
  ELSE
    Lv_comando =  'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||' SET '||
                  Pr_Reg.campo||' = '||CHR(39)||Pr_Reg.valor||CHR(39)||', synced= now(), updated_function= '||
                  chr(39)||'F_OriSync'||chr(39);
  END IF;               
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
    select Fu_GetValColumn(Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName||'_log_col' , Lv_column ,  Pr_Reg.secuencia ) 
      into Lv_Texto;       

    if Lr_Cols.ordinal_position = 1 then
      if Lr_Cols.data_type in ('character varying','date') Then
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
      else
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    else
      if Lr_Cols.data_type in ('character varying','date') Then
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
      else
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    end if;  
  END LOOP;

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
    --ojo nrt EXECUTE Lv_comando ;
  ELSE
    --Aactualizar la auditoria de la BD Remota
    select   Fu_RemAudUpdComand(Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName||'_log_col' , Pr_Reg )
    into Lv_comando; 
        EXECUTE Lv_comando ;
  END IF;
exception
  WHEN OTHERS THEN
    raise notice E' Error Fu_OriSyncUpd %s \n',sqlerrm;       
    rollback;

end $BODY$;
CREATE OR REPLACE FUNCTION Fu_Comando(Pv_SchemaLoc character varying , Pv_TableName character varying , Pv_ColumnName character varying , Pr_Record RECORD) RETURNS VARCHAR AS $$
DECLARE
  Ln_cantLog integer;
  Lv_Texto  VARCHAR;
  lv_comando VARCHAR;
  ri RECORD;

BEGIN
     Lv_comando = 'select '||Pv_SchemaLoc||'.'||Pv_TableName||'.'||Pv_ColumnName||' into Lv_texto ';
    FOR ri IN
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
      EXECUTE 'SELECT ($1).' || ri.column_name || '::text' INTO Lv_Texto USING Pr_Record;
      if ri.ordinal_position = 1 then
        if ri.data_type in ('character varying','date') Then
          Lv_comando = Lv_comando||' where '||ri.column_name ||'='||chr(39)||Lv_Texto||chr(39);
        else
          Lv_comando = Lv_comando||' where '||ri.column_name ||'='||Lv_Texto;
        end if;
      else
        if ri.data_type in ('character varying','date') Then
          Lv_comando = Lv_comando||' and  '||ri.column_name ||'='||chr(39)||Lv_Texto||chr(39);
        else
          Lv_comando = Lv_comando||' and  '||ri.column_name ||'='||Lv_Texto;
        end if;
      end if;  
    END LOOP;

      --raise notice E' COMANDO  ====> %\n', lv_comando;
      execute lv_comando;    
    return Lv_texto;
END;
$$ LANGUAGE plpgsql VOLATILE;

--Procedimiento para sincromizar BD Origen con los registros eliminados de la BD Destino
create or replace procedure Fu_OriSyncDel(
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
Lr_Cols RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_instance VARCHAR;
Lv_CurrentDB VARCHAR;
Lv_column VARCHAR;
begin 
  --Registros elimiandos de la BD Destino
  Pr_reg.updated_function= 'F_OriSync';
  Pr_reg.synced= now();

  --raise notice '%', Lr_users; 
  select db_instance
    into Lv_instance
    from companys --v_companys
  where id_company = Pr_Reg.id_company;

  select current_database()
    into Lv_CurrentDB;

  --Armar el comando para borrar regostros de la BD Origen
  Lv_comando =  'DELETE FROM '||Pv_SchemaLoc||'.'||Pv_TableName;
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
    --EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Pr_reg;
    Lv_column = Lr_Cols.column_name;
    select Fu_GetValColumn(Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName||'_log' , Lv_column ,  Pr_Reg.secuencia ) 
      into Lv_Texto;       
    --select   Fu_Comando(Pv_SchemaLoc ,Pv_TableName ,Lr_Cols.column_name, Lr_Users ) into Lv_texto;

    if Lr_Cols.ordinal_position = 1 then
      if Lr_Cols.data_type in ('character varying','date') Then
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
      else
        Lv_comando = Lv_comando||' where '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    else
      if Lr_Cols.data_type in ('character varying','date') Then
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
      else
        Lv_comando = Lv_comando||' and  '||Lr_Cols.column_name ||'='||Lv_Texto;
      end if;
    end if;  
  END LOOP;

  --raise notice E' comando DELETE ====> %\n', lv_comando;    
  EXECUTE Lv_comando ;
  --Actualizar la fecha de sincronizacion del log local
  select   Fu_RemAudUpdComand(Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName||'_log' , Pr_reg )
  into Lv_comando; 
  --raise notice E' comando update ====> %\n', lv_comando;    
  EXECUTE Lv_comando ;
exception
  WHEN OTHERS THEN
    raise notice E' Error Fu_OriSyncDel %s \n',sqlerrm;       
    rollback;
END $BODY$;
--end $BODY$;
--Funcion para Actualizar la fecha de sincronizacion del log
create or replace FUNCTION Fu_RemAudUpdComand(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem character varying, 
  Pv_TableName character varying,
  Pr_Reg RECORD
) RETURNS VARCHAR AS $$
DECLARE
Lv_cursor varchar;
Lc_Users refcursor;
Lc_RegAud refcursor;
Lr_users ori.usuarios_log%ROWTYPE; --record;
Lr_Cols RECORD;
Lr_userDes ori.usuarios_log_col%ROWTYPE;
--Lr_userDes RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_instance VARCHAR;
Lv_CurrentDB VARCHAR;
Lv_column VARCHAR;
--Lv_Texto TEXT;
--Lr_Tabla RECORD;
--Lv_SchemaLocal VARCHAR = 'ori';
 begin 
  --Actualizar la fecha de sincronizacion del log 
  Lv_comando = 'select dblink_exec(''dbname='||Pv_Instance||
    ' host='||Pv_host||' user=postgres password=postnrt1964'',
    ''UPDATE '||Pv_SchemaRem||'.'||Pv_TableName||' SET synced= now() ';
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
  --raise notice E' comando update ====> %\n', lv_comando;    
  RETURN Lv_comando ;
exception
  WHEN OTHERS THEN
    raise notice E' Error Fu_RemAudUpdComand %s \n',sqlerrm;       
    rollback;
    return null;
end;
$$ LANGUAGE plpgsql VOLATILE;
--Procedimiento para sincromizar BD Origen con los registros nuevos de la BD Destino
create or replace procedure Fu_OriSyncNew(
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
Lr_Cols RECORD;
Lv_comando VARCHAR;
Lv_column VARCHAR;
Lv_Texto  VARCHAR;
Lv_instance VARCHAR;
Lv_CurrentDB VARCHAR;
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

  --Armar el comando para borrar regostros de la BD Origen
  Lv_comando = 'insert into '||Pv_SchemaLoc||'.'||Pv_TableName||' (';
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
  FOR Lr_Cols IN
      SELECT ordinal_position, column_name, data_type
      FROM information_schema.columns
      WHERE
          table_schema = Pv_SchemaLoc
      AND table_name = Pv_TableName
      ORDER BY ordinal_position
  LOOP
    --EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Pr_Reg;  ojo
    Lv_column = Lr_Cols.column_name;
    select Fu_GetValColumn(Pv_Instance ,  Pv_Host ,  Pv_SchemaLoc ,  Pv_SchemaRem ,  Pv_TableName||'_log' , Lv_column ,  Pr_Reg.secuencia ) 
      into Lv_Texto;       
    --raise notice E' col % valor ====> %\n', Lr_Cols.column_name,Lv_Texto ;    
    if Lv_Texto is null then
      Lv_Texto = 'null';
      Lr_Cols.data_type = 'int';
    end if;   
    if Lv_column = 'synced' then
      select now() into Lv_Texto;
      Lr_Cols.data_type = 'date';
      --raise notice E' Id % %\n',Pr_reg.id, Lv_texto;
    end if;

    if Lr_Cols.ordinal_position = 1 then
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character','money') Then
        Lv_comando = Lv_comando||' '||chr(39)||Lv_Texto||chr(39);
      else  
        Lv_comando = Lv_comando||' '||Lv_Texto;
      end if;  
    else
      if Lr_Cols.data_type in ('character varying','date','timestamp without time zone', 'character','money') Then
        Lv_comando = Lv_comando||','||chr(39)||Lv_Texto||chr(39);
      else  
        Lv_comando = Lv_comando||','||Lv_Texto;
      end if;  
    end if;  

  END LOOP;
  Lv_comando = Lv_comando||' ) ';

  --raise notice E' comando INSERT ====> %\n', lv_comando;    
  EXECUTE Lv_comando ;
  --Actualizar la fecha de sincronizacion del log Destino
  select   Fu_RemAudUpdComand(Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName||'_log' , Pr_Reg )
    into Lv_comando; 
  --raise notice E' comando update ====> %\n', lv_comando;    
  EXECUTE Lv_comando ;

EXCEPTION
  WHEN OTHERS THEN
    raise notice E' Error Fu_OriSyncNew ====> %\n', SQLERRM;  
    ROLLBACK;  
    RETURN;
end $BODY$;
--Funcion para Actualizar la fecha de sincronizacion del log
create or replace FUNCTION Fu_GetValColumn(
  Pv_Instance varchar,
  Pv_Host VARCHAR,
  Pv_SchemaLoc VARCHAR,
  Pv_SchemaRem VARCHAR, 
  Pv_TableName VARCHAR,
  Pv_ColumnName VARCHAR,
  Pn_Sec integer
) RETURNS VARCHAR AS $$
DECLARE
  Lr_rec RECORD;
  Lv_comand VARCHAR;
  Lv_Comando varchar;
  BEGIN

    Lv_Comando = 'select '||Pv_ColumnName ||' ::text valor from '||Pv_SchemaLoc||'.'||Pv_TableName||' where secuencia = '||Pn_Sec;
    Lv_Comando = Lv_Comando||' union select '||Pv_ColumnName ||' ::text valor from '||Pv_SchemaLoc||'.v_'||Pv_TableName||' where secuencia = '||Pn_Sec;
    For Lr_rec in execute Lv_comando 
    LOOP
      return Lr_rec.valor;
    END LOOP;    

  RETURN null ;
exception
  WHEN OTHERS THEN
    raise notice E' Error Fu_GetValColumn %s \n',sqlerrm;       
    rollback;
    return null;
end;
$$ LANGUAGE plpgsql VOLATILE;


