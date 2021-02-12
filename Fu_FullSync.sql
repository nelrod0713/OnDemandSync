--Procedimiento para sincromizar tabla de usuarios
create or replace procedure F_OriSync(
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
Lc_Users refcursor;
Lc_RegAud refcursor;
Lr_users ori.usuarios%ROWTYPE; --record;
Lr_Cols RECORD;
Lr_userDes ori.usuarios_log_col%ROWTYPE;
--Lr_userDes RECORD;
Lv_comando VARCHAR;
Lv_Texto  VARCHAR;
Lv_instance VARCHAR;
Lv_CurrentDB VARCHAR;
--Lr_Tabla RECORD;
--Lv_text TEXT;
--Lv_SchemaLocal VARCHAR = 'ori';
begin 
  --Lv_cursor = 'select * from ori.usuarios';
  Lv_Cursor = 'select ori.*
                 from '||Pv_SchemaLoc||'.'||Pv_TableName||' ori 
                  where exists (select 1 from '||Pv_SchemaLoc||'.v_'||Pv_TableName||'_log_col des';
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
    EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Lr_userDes;
    if Lr_Cols.ordinal_position = 1 then
        Lv_Cursor = Lv_Cursor||' where ori.'||Lr_Cols.column_name ||'= des.'||Lr_Cols.column_name ;
    else
        Lv_Cursor = Lv_Cursor||' and  ori.'||Lr_Cols.column_name ||'= des.'||Lr_Cols.column_name;
    end if;  
  END LOOP;
  Lv_Cursor = Lv_Cursor||' and des.operation = '||chr(39)||'U'||chr(39)||'and (des.synced is null )) '; 
      --raise notice E' cursor  ====> %\n', lv_cursor;    

  open Lc_users for execute Lv_Cursor; 
  fetch next from Lc_users into Lr_users;
  while found 
  loop
    raise notice '%', Lr_users; 
    select db_instance
      into Lv_instance
      from companys --v_companys
    where id_company = Lr_users.id_company;

    select current_database()
      into Lv_CurrentDB;

    --Definir cursor de los cambios a aplicar
    Lv_Cursor = 'select * from '||Pv_SchemaLoc||'.'||Pv_TableName||'_log_col';
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
      EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Lr_users;
      --raise notice E' Texto ====> %\n', Lv_Texto;    
      if Lr_Cols.ordinal_position = 1 then
        if Lr_Cols.data_type in ('character varying','date') Then
          Lv_Cursor = Lv_Cursor||' where '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
        else
          Lv_Cursor = Lv_Cursor||' where '||Lr_Cols.column_name ||'='||Lv_Texto;
        end if;
      else
        if Lr_Cols.data_type in ('character varying','date') Then
          Lv_Cursor = Lv_Cursor||' and '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
        else
          Lv_Cursor = Lv_Cursor||' and '||Lr_Cols.column_name ||'='||Lv_Texto;
        end if;
      end if;  
    END LOOP;
    Lv_Cursor = Lv_Cursor||' and operation = ''U'' and synced is null 	  union';
    Lv_Cursor = Lv_Cursor||' select * from '||Pv_SchemaLoc||'.v_'||Pv_TableName||'_log_col';
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
      EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Lr_users;
      if Lr_Cols.ordinal_position = 1 then
        if Lr_Cols.data_type in ('character varying','date') Then
          Lv_Cursor = Lv_Cursor||' where '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
        else
          Lv_Cursor = Lv_Cursor||' where '||Lr_Cols.column_name ||'='||Lv_Texto;
        end if;
      else
        if Lr_Cols.data_type in ('character varying','date') Then
          Lv_Cursor = Lv_Cursor||' and '||Lr_Cols.column_name ||'='||chr(39)||Lv_Texto||chr(39);
        else
          Lv_Cursor = Lv_Cursor||' and '||Lr_Cols.column_name ||'='||Lv_Texto;
        end if;
      end if;  
    END LOOP;
    Lv_Cursor = Lv_Cursor||' and operation = ''U'' and (synced is null )	  order by stamp';
      --raise notice E' cursor regs ====> %\n', lv_cursor;    


    open Lc_RegAud for execute Lv_Cursor; 
    fetch next from Lc_RegAud into Lr_userDes;
    while found 
        --FOR Lr_userDes IN  
	  LOOP
      If   Lr_userDes.campo = 'synced' THEN
        Lv_comando =  'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||' SET '||
                     Lr_userDes.campo||' = '||CHR(39)||Lr_userDes.valor||CHR(39);
      ELSE
        Lv_comando =  'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||' SET '||
                     Lr_userDes.campo||' = '||CHR(39)||Lr_userDes.valor||CHR(39)||', synced= now(), updated_function= '||chr(39)||'F_OriSync'||chr(39);
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
        EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Lr_userDes;
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

      --select   Fu_Comando('L',Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName , Lr_UserDes )
      --into Lv_comando; 
      --raise notice E' comando update ====> %\n', lv_comando;    
      EXECUTE Lv_comando ;
      --Valida si se actualiza la BD Local o la Remota
      IF Lr_UserDes.db_instance = Lv_CurrentDB THEN
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
            EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Lr_userDes;
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

        --select   Fu_Comando('L',Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName , Lr_UserDes )
        --into Lv_comando; 
        --raise notice E' comando update log col ====> %\n', lv_comando;    
        EXECUTE Lv_comando ;

      ELSE
            Lv_comando = 'select dblink_exec(''dbname='||Lr_UserDes.db_instance||
              ' host='||Pv_host||' user=postgres password=postnrt1964'',
		         ''UPDATE '||Pv_SchemaRem||'.'||Pv_TableName||'_log_col  SET synced= now() ';
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
                EXECUTE 'SELECT ($1).' || Lr_Cols.column_name || '::text' INTO Lv_Texto USING Lr_UserDes;
         --raise notice E'  texto ====> %\n', Lv_Texto;    
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
            EXECUTE Lv_comando ;
      END IF;
      fetch next from Lc_RegAud into Lr_userDes; 

    END LOOP;

       /*begin
	       update ori.usuarios
	         set apellido = Lr_UserDes.apellido,
               updated = Lr_UserDes.updated
         where id = Lr_users.id;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'usuario inexistente UPDATE!';
            rollback;
	   end;*/

    fetch next from Lc_Users into Lr_users; 
  end loop;
  close Lc_Users; 
end $BODY$;
CREATE OR REPLACE FUNCTION Fu_Comando(Pv_LocRem VARCHAR, Pv_Instance VARCHAR, Pv_Host VARCHAR, Pv_SchemaLoc VARCHAR, Pv_SchemaRem VARCHAR, Pv_TableName VARCHAR,
                           Pr_Old RECORD) RETURNS VARCHAR AS $$
DECLARE
  Ln_cantLog integer;
  Lv_Texto  VARCHAR;
  lv_comando VARCHAR;
  ri RECORD;

BEGIN
  IF Pv_LocRem = 'L' THEN
    Lv_comando = 'UPDATE '||Pv_SchemaLoc||'.'||Pv_TableName||' SET ';
    --RAISE NOTICE E'\n    comand : % ',Lv_comando;
    FOR ri IN
      SELECT ordinal_position, column_name, data_type
      FROM information_schema.columns
      WHERE
          table_schema = Pv_SchemaLoc
      AND table_name = Pv_TableName
      ORDER BY ordinal_position
    LOOP

      EXECUTE 'SELECT ($1).' || ri.column_name || '::text' INTO Lv_Texto USING Pr_Old;
      if ri.column_name = 'synced' then
        Lv_texto = now();
      end if;
      if Lv_Texto is null then
          Lv_Texto = 'null';
          ri.data_type = 'int';
      end if;   
      if ri.ordinal_position = 1 then
        if ri.data_type in ('character varying','date','money','timestamp without time zone') Then
          Lv_comando = Lv_comando||'  '||ri.column_name ||'='||chr(39)||Lv_Texto||chr(39);
        else
          Lv_comando = Lv_comando||'  '||ri.column_name ||'='||Lv_Texto;
        end if;
      else
        if ri.data_type in ('character varying','date','money','timestamp without time zone') Then
          Lv_comando = Lv_comando||' , '||ri.column_name ||'='||chr(39)||Lv_Texto||chr(39);
        else
          Lv_comando = Lv_comando||' , '||ri.column_name ||'='||Lv_Texto;
        end if;
      end if;  
    END LOOP;
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
      EXECUTE 'SELECT ($1).' || ri.column_name || '::text' INTO Lv_Texto USING Pr_Old;
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
    return Lv_comando;
  END IF; --Pv_LocRem = 'L' 
END;
$$ LANGUAGE plpgsql VOLATILE;
