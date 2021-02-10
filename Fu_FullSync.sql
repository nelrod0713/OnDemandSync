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
Lr_users record;
Lr_userDes ori.usuarios_log%ROWTYPE;
Lv_comando VARCHAR;
--Lr_Tabla RECORD;
--Lv_text TEXT;
--Lv_SchemaLocal VARCHAR = 'ori';
begin 
  --Lv_cursor = 'select * from ori.usuarios';
  Lv_Cursor = 'select ori.id, ORI.NOMBRE, ori.apellido from ori.usuarios ori
                       inner join ori.v_usuarios_log des ON ori.id=des.id
                where des.operation = '||chr(39)||'U'||chr(39);

  open Lc_users for execute Lv_Cursor; 
  fetch next from Lc_users into Lr_users;
  while found 
  loop
    raise notice '%', Lr_users; 
    FOR Lr_userDes IN  
	  select * from ori.usuarios_log
       where id =  Lr_users.id and operation = 'U'
	  union
	  select * from ori.v_usuarios_log 
       where id =  Lr_users.id and operation = 'U'
	  order by stamp
    LOOP  
      select   Fu_Comando('L',Pv_Instance , Pv_Host , Pv_SchemaLoc , Pv_SchemaRem , Pv_TableName , Lr_UserDes )
      into Lv_comando; 
      raise notice E' comando update ====> %\n', lv_comando;    
      EXECUTE Lv_comando ;
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
