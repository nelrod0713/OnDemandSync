CREATE or replace FUNCTION public.Fu_AuditLog()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
  Lv_comando text;
  Lr_RecordD RECORD;
  Lr_RecordU RECORD;
  Lr_RecordI RECORD;
  Lv_Tabla VARCHAR;
  Lv_Schema character varying;
    
BEGIN
        -- Permite crear un registro en la tabla 'TG_TABLE_NAME'_log para almacenar los cambios realizados sobre TG_TABLE_NAME,
        --se hace para llevar registro de todos los cambios y poder sincronizar otra tabla en cualquier momento .
        --
        --
        Lv_Schema = TG_TABLE_SCHEMA;
        Lv_Tabla = TG_TABLE_NAME;
        DISCARD PLANS;
        IF (TG_OP = 'DELETE') THEN
  
            Lr_RecordD = OLD;
            select  Fu_ComandoInsert(Lv_Schema, Lv_Tabla,'D', Lr_RecordD)
              into Lv_comando;
             execute Lv_comando;
            --INSERT INTO ori.usu_audit SELECT 'D', now(), user, null,OLD.*;
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
              Lr_RecordU = NEW;
             --RAISE NOTICE E'\n    Old : %  New % \n',OLD.updated_function, Lr_RecordU.updated_function;
            if coalesce(OLD.updated_function,'*') = 'F_OriSync' THEN
              Lr_RecordU.updated_function= NULL;
            end IF;
             --RAISE NOTICE E'\n    Old : %  New % \n',OLD.updated_function, Lr_RecordU.updated_function;
            If Lr_RecordU.updated_function IS NULL OR coalesce(Lr_RecordU.updated_function,'*') <> 'F_OriSync' THEN
              select  Fu_ComandoUpdate(Lv_Schema, Lv_Tabla,'U', Lr_RecordU,OLD)
                into Lv_comando;
            END IF;  
            --INSERT INTO ori.usu_audit SELECT 'U', now(), user, null, NEW.*;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            Lr_RecordI = NEW;
              --RAISE NOTICE E'\n    comand : % % ',Lr_RecordI.id,Lr_RecordI.synced;
            If (Lr_RecordI.updated_function IS NULL OR Lr_RecordI.updated_function <> 'F_OriSync') and Lr_RecordI.synced is null THEN
              select  Fu_ComandoInsert(Lv_Schema, Lv_Tabla,'I', Lr_RecordI)
                into Lv_comando;
              --RAISE NOTICE E'\n    comand : % ',Lv_comando;
              execute Lv_comando;
             end if;
            --INSERT INTO ori.usu_audit SELECT 'I', now(), user, null, NEW.*;
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger          return null;
END;
$BODY$;
CREATE OR REPLACE FUNCTION Fu_ComandoInsert(Pv_Schema VARCHAR, Pv_TableName VARCHAR, Pv_Oper VARCHAR, Pr_Reg RECORD) RETURNS VARCHAR AS $$
DECLARE
  --Esta fucnion genera el comando insert, de manera dinamica para cualquier tabla, para la tabla de auditoria
  Ln_cantLog integer;
  Lv_Texto  VARCHAR;
  lv_comando VARCHAR;
  ri RECORD;
  Lv_CurrentDB VARCHAR;
  Ln_Seq BIGINT;
  Ld_Now timestamp;
BEGIN
  select current_database()
    into Lv_CurrentDB;
  --EXECUTE 'SELECT seq_'||usuarios||'_log into Ln_Seq';
  Lv_comando = 'insert into '||Pv_Schema||'.'||Pv_TableName||'_log (operation,stamp,user_aud,sync,db_instance';
  --Lv_comando = 'insert into '||Pv_Schema||'.'||Pv_TableName||'_log (operation,stamp,user_aud,sync,db_instance, secuencia';
  FOR ri IN
      SELECT ordinal_position, column_name
      FROM information_schema.columns
      WHERE
          table_schema = Pv_Schema
      AND table_name = Pv_TableName
      ORDER BY ordinal_position
  LOOP
    Lv_comando = Lv_comando||','||ri.column_name;
  END LOOP;
  select clock_timestamp() into Ld_Now;   
  Lv_comando = Lv_comando||') Values ('||chr(39)||Pv_Oper||chr(39)||','||chr(39)||Ld_Now||chr(39)||', user, null,'||chr(39)||Lv_CurrentDB||chr(39);
  --Lv_comando = Lv_comando||') Values ('||chr(39)||Pv_Oper||chr(39)||', now(), user, null,'||chr(39)||Lv_CurrentDB||chr(39)||','||Ln_Seq;
  FOR ri IN
      SELECT ordinal_position, column_name, data_type
      FROM information_schema.columns
      WHERE
          table_schema = Pv_Schema
      AND table_name = Pv_TableName
      ORDER BY ordinal_position
  LOOP
    EXECUTE 'SELECT ($1).' || ri.column_name || '::text' INTO Lv_Texto USING Pr_Reg;
      IF Lv_Texto IS NULL THEN
          Lv_Texto = 'NULL';
      END IF;--select quote_nullable(Lv_Texto) into Lv_Texto;
      if ri.data_type in ('character varying', 'date','money','timestamp without time zone') AND Lv_Texto <> 'NULL' Then
        Lv_comando = Lv_comando||','||CHR(39)||Lv_Texto||CHR(39);
      else  
        Lv_comando = Lv_comando||','||Lv_Texto;
      end if;  
  END LOOP;
  Lv_comando = Lv_comando||' ) ';
  if Lv_comando is null then
    Rollback;
  else  
    return Lv_comando;
  end if;  
END;
$$ LANGUAGE plpgsql VOLATILE;
CREATE OR REPLACE FUNCTION Fu_ComandoUpdate(Pv_Schema VARCHAR, Pv_TableName VARCHAR, Pv_Oper VARCHAR, Pr_RegNew RECORD, Pr_RegOld RECORD) RETURNS VARCHAR AS $$
DECLARE
  --Esta fucnion genera el comando insert, de manera dinamica para cualquier tabla, para la tabla de auditoria por columnas
  Ln_cantLog integer;
  Lv_TextoNew  VARCHAR;
  Lv_TextoNewI  VARCHAR;
  Lv_TextoOld  VARCHAR;
  Lv_synced VARCHAR;
  lv_comando VARCHAR;
  Lv_CurrentDB VARCHAR;
  ri RECORD;
  Lr_Col RECORD;
  Ln_Prim integer;
  Ln_Seq BIGINT;
  Ld_Now timestamp;

BEGIN
  --Valores a actualizar
  FOR Lr_Col IN
      SELECT ordinal_position, column_name, data_type
      FROM information_schema.columns
      WHERE
          table_schema = Pv_Schema
      AND table_name = Pv_TableName
      ORDER BY ordinal_position
  LOOP
    --EXECUTE 'SELECT seq_'||usuarios||'_log into Ln_Seq';
    --RAISE NOTICE E'\n ================> Col : % ',Lr_Col.column_name;
    EXECUTE 'SELECT ($1).' || Lr_Col.column_name || '::text' INTO Lv_TextoNew USING Pr_RegNew;
    EXECUTE 'SELECT ($1).' || Lr_Col.column_name || '::text' INTO Lv_TextoOld USING Pr_RegOld;
    Lv_TextoNewI = Lv_TextoNew;
    IF Lr_Col.column_name = 'synced' THEN
      Lv_synced = quote_nullable(Lv_TextoNewI); 
    ELSE  
      Lv_synced = quote_nullable(null);
    END IF;
    Lv_synced = quote_nullable(Pr_RegNew.synced);
    Lv_comando := null;
    IF quote_nullable(Lv_TextoNew) <> quote_nullable(Lv_TextoOld) THEN
      select current_database()
      into Lv_CurrentDB;
      Lv_comando = 'insert into '||Pv_Schema||'.'||Pv_TableName||'_log_col (operation,stamp,user_aud,sync, db_instance ';

      --Columnas de la llave primaria
      FOR ri IN
        SELECT k.ordinal_position, k.column_name
        FROM information_schema.key_column_usage k,
            information_schema.columns c
        WHERE	k.constraint_catalog = c.table_catalog
          and k.constraint_schema = c.table_schema
          and k.table_name = c.table_name
          and k.column_name = c.column_name
          and k.constraint_schema = Pv_Schema
          AND k.table_name = Pv_TableName
        ORDER BY ordinal_position
        LOOP
          Lv_comando = Lv_comando||','||ri.column_name;
        END LOOP;
        select clock_timestamp() into Ld_Now;   
        Lv_comando = Lv_comando||',campo, valor, synced) Values ('||chr(39)||Pv_Oper||chr(39)||', '||chr(39)||Ld_Now||chr(39)||', user, null,'||
                     chr(39)||Lv_CurrentDB||chr(39);
        --RAISE NOTICE E'\n    1.comand : % ',Lv_comando;
        --Valores de la llave primaria
        FOR ri IN
            SELECT k.ordinal_position, k.column_name,c.data_type
            FROM information_schema.key_column_usage k,
                information_schema.columns c
            WHERE	k.constraint_catalog = c.table_catalog
              and k.constraint_schema = c.table_schema
              and k.table_name = c.table_name
              and k.column_name = c.column_name
              and k.constraint_schema = Pv_Schema
              AND k.table_name = Pv_TableName
            ORDER BY ordinal_position
        LOOP
          EXECUTE 'SELECT ($1).' || ri.column_name || '::text' INTO Lv_TextoNew USING Pr_RegNew;
            IF Lv_TextoNew IS NULL THEN
                Lv_TextoNew = 'NULL';
            END IF;--select quote_nullable(Lv_Texto) into Lv_Texto;
            if ri.data_type in ('character varying', 'date','money','timestamp without time zone') AND Lv_TextoNew <> 'NULL' Then
              Lv_comando = Lv_comando||','||CHR(39)||Lv_TextoNew||CHR(39);
            else  
              Lv_comando = Lv_comando||','||Lv_TextoNew;
            end if;  
        END LOOP;
        Lv_TextoNew = Lv_TextoNewI;
        Lv_comando = Lv_comando||','||chr(39)||Lr_Col.column_name||chr(39);
        IF Lv_TextoNew IS NULL THEN
            Lv_TextoNew = 'NULL';
        END IF;--select quote_nullable(Lv_Texto) into Lv_Texto;
        if Lr_Col.data_type in ('character varying', 'date','money','timestamp without time zone') AND Lv_TextoNew <> 'NULL' Then
          Lv_comando = Lv_comando||','||CHR(39)||Lv_TextoNew||CHR(39);
        else  
          Lv_comando = Lv_comando||','||Lv_TextoNew;
        end if;  
        --Lv_comando = Lv_comando||',null ) ';
        Lv_comando = Lv_comando||','||Lv_synced||' ) ';
    --RAISE NOTICE E'\n ================> Lv_synced  : % ',Lv_synced;
        execute lv_comando;
    END IF; --Lv_TextoNew <> Lv_TextoOld 
  END LOOP;
  return Lv_comando;
END;
$$ LANGUAGE plpgsql VOLATILE;
