DO $$
--EXEC SQL BEGIN DECLARE SECTION;
declare
rec_comp   record;
vista character varying;
DB character varying = 'Claro';
cur_comp cursor for 
select db_instance, schema_name, id_company
  from companys
 where id_company <> 0;
stmt text;
begin
  -- open the cursor
  open cur_comp;
    --rAISE warning 'antes loop %';
  loop
    vista = 'v_usuarios';    
    fetch cur_comp into rec_comp;
    exit when not found;
    ----------  VISTAS PARA LA TABLA DE USUARIOS
    rAISE notice E'BD % ',rec_comp.db_instance;
    --Crear la vista de usuarios
    stmt = 'CREATE or replace VIEW ori.'||vista||rec_comp.id_company||' AS SELECT *
    FROM dblink('||chr(39)||'dbname='||rec_comp.db_instance||' host= localhost user=postgres password=postnrt1964'||chr(39)||','||chr(39)||
    'select id_company, id , nombre, apellido, created, updated, synced, updated_function from '||rec_comp.schema_name||'.usuarios'||chr(39)||')
      AS t1(id_company integer, id integer, nombre character varying(60),  apellido character varying(60), 
			created timestamp, updated timestamp, 
      synced timestamp, updated_function character varying(60)) 
      where id_company = '||rec_comp.id_company||';';
    EXECUTE stmt;
    --Crear la vista de usuarios_log
    stmt = 'CREATE or replace VIEW ori.'||vista||'_log'||rec_comp.id_company||' AS SELECT *  
    FROM dblink('||chr(39)||'dbname='||rec_comp.db_instance||' host= localhost user=postgres password=postnrt1964'||chr(39)||','||chr(39)||
    'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, id , nombre, apellido, created, updated,
                 synced, updated_function from '||rec_comp.schema_name||'.usuarios_log'||chr(39)||')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying, secuencia integer, 
            id_company integer,id integer, nombre character varying(60),  apellido character varying(60), created timestamp, 
            updated timestamp, synced timestamp, updated_function character varying(60)) 
            where id_company = '||rec_comp.id_company||';';
    --rAISE warning 'BD % stmt  %',rec_comp.db_instance,stmt;
    EXECUTE stmt;
    --Crear la vista de usuarios_log_col
    stmt = 'CREATE or replace VIEW ori.'||vista||'_log_col'||rec_comp.id_company||' AS SELECT *  
    FROM dblink('||chr(39)||'dbname='||rec_comp.db_instance||' host= localhost user=postgres password=postnrt1964'||chr(39)||','||chr(39)||
    'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, id , campo, valor, synced from '||rec_comp.schema_name||'.usuarios_log_col'||chr(39)||')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying(30), secuencia integer, 
      id_company integer, id integer, campo character varying(60),  valor character varying(60), synced timestamp ) 
      where id_company = '||rec_comp.id_company||';';
    --rAISE warning 'BD % stmt  %',rec_comp.db_instance,stmt;
    EXECUTE stmt;
    --Crear la VIsta de registros que no se deben sincronizar en Destino
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_ori_no'||rec_comp.id_company||' AS 
    select secuencia, db_instance
      from ori.usuarios_log orig
      where synced is null 
        and operation = '||chr(39)||'I'||chr(39)||'
        and ( exists (select 1
                          from ori.'||vista||rec_comp.id_company||'
                        where id_company = orig.id_company
                          and id = orig.id)
              or  exists (select 1
                          from ori.'||vista||'_log'||rec_comp.id_company||'
                        where id_company = orig.id_company
                          and id = orig.id
                          and operation = '||chr(39)||'D'||chr(39)||' and synced is null )
              or  exists (select 1
                          from ori.usuarios_log
                        where id_company = orig.id_company
                          and id = orig.id
                          and operation = '||chr(39)||'D'||chr(39)||' and synced is not null ))
                          and id_company ='||rec_comp.id_company||' ;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;
    --Crear la VIsta de registros a sincronizar en Destino
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_ori'||rec_comp.id_company||' AS 
    --Registros de Insert/Delete de Origen
      select operation,
      stamp,
      user_aud,
      sync,
      db_instance,
      secuencia,
      id_company,
      id,
      nombre,
      apellido,
      created,
      updated,
      synced,
      updated_function,
      null campo,
      null valor
    from ori.usuarios_log orig
    where id_company ='||rec_comp.id_company||' 
      and synced is null 
      and operation in ('||chr(39)||'I'||chr(39)||','||chr(39)||'D'||chr(39)||')
    union
    --Registros de Update de Origen
    select operation,
        stamp,
        user_aud,
        sync,
        db_instance,
        secuencia,
        id_company,
        id, 
        null nombre,
        null apellido,
        null created,
        null updated,
        synced,
        null updated_function,
        campo,
        valor
    from ori.usuarios_log_col 
    where id_company ='||rec_comp.id_company||'
      and synced is null 
    union 
    --Registros de Update de Destino
    select operation,
        stamp,
        user_aud,
        sync,
        db_instance,
        secuencia,
        id_company,
        id, 
        null nombre,
        null apellido,
        null created,
        null updated,
        synced,
        null updated_function,
        campo,
        valor
    from ori.'||vista||'_log_col'||rec_comp.id_company||'  
    where id_company ='||rec_comp.id_company||'
      and synced is null 
    order by stamp;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;
    --Crear la VIsta con registros que no se deben sincronizar en Origen
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_des_no'||rec_comp.id_company||' AS 
    select secuencia, db_instance
    from ori.'||vista||'_log'||rec_comp.id_company||' des
    where synced is null 
      and operation = '||chr(39)||'I'||chr(39)||'
      --Que existan en el origen
      and (exists (select 1
                        from ori.usuarios
                      where id_company = des.id_company
                        and id = des.id)
      --que  se hayan borrado en el origen                  
          or  exists (select 1
                        from ori.usuarios_log
                      where id_company = des.id_company
                        and id = des.id
                        and operation = '||chr(39)||'D'||chr(39)||' and synced is null ))
                              and id_company ='||rec_comp.id_company||' ;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;
    --VIsta con registros a sincronizar en Origen
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_des'||rec_comp.id_company||' AS
    --Registros de Insert/ Delete de Destino
    select operation,
        stamp,
        user_aud,
        sync,
        db_instance,
        secuencia,
        id_company,
        id,
        nombre,
        apellido,
        created,
        updated,
        synced,
        updated_function,
        null campo,
        null valor
    from ori.'||vista||'_log'||rec_comp.id_company||' des
    where id_company ='||rec_comp.id_company||'
      and synced is null 
      and operation in ('||chr(39)||'I'||chr(39)||','||chr(39)||'D'||chr(39)||')
    union 
    --Registros de Update de Origen
    select operation,
        stamp,
        user_aud,
        sync,
        db_instance,
        secuencia,
        id_company,
        id, 
        null nombre,
        null apellido,
        null created,
        null updated,
        synced,
        null updated_function,
        campo,
        valor 
    from ori.usuarios_log_col 
    where id_company ='||rec_comp.id_company||'
      and synced is null 
    union 
    --Registros de Update de Destino
    select operation,
        stamp,
        user_aud,
        sync,
        db_instance,
        secuencia,
        id_company,
        id, 
        null nombre,
        null apellido,
        null created,
        null updated,
        synced,
        null updated_function,
        campo,
        valor 
    from ori.'||vista||'_log_col'||rec_comp.id_company||'  
    where id_company ='||rec_comp.id_company||'
      and synced is null 
    order by stamp;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_full_no'||rec_comp.id_company||' AS
    --Registros de Insert/ Delete de Destino
    select * from ori.'||vista||'_aud_des_no'||rec_comp.id_company||'
    union
    select * from ori.'||vista||'_aud_ori_no'||rec_comp.id_company||';';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;

    --VIsta con registros para sincronizar Origen y Destino
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_full'||rec_comp.id_company||' AS
    --Registros de Insert/ Delete de Destino
    select * from ori.'||vista||'_aud_ori'||rec_comp.id_company||' 
    union 
    --Registros de Insert/ Delete de Origen
    select * from ori.'||vista||'_aud_des'||rec_comp.id_company||' 
    order by stamp;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;

    ----------  VISTAS PARA LA TABLA DE FACTURACION
    vista = 'v_facturacion';    
    --Crear la vista de FACTURACION
    stmt = 'CREATE or replace VIEW ori.'||vista||rec_comp.id_company||' AS SELECT *
    FROM dblink('||chr(39)||'dbname='||rec_comp.db_instance||' host= localhost user=postgres password=postnrt1964'||chr(39)||','||chr(39)||
    'select id_company , date , concept ,  invoice_value , synced , updated_function from '||rec_comp.schema_name||'.facturacion'||chr(39)||')
      AS t1(id_company integer, date date, concept integer,  invoice_value money, synced timestamp, updated_function character varying(60)) 
      where id_company = '||rec_comp.id_company||';';
    EXECUTE stmt;
    --Crear la vista de facturacion_log
    stmt = 'CREATE or replace VIEW ori.'||vista||'_log'||rec_comp.id_company||' AS SELECT *  
    FROM dblink('||chr(39)||'dbname='||rec_comp.db_instance||' host= localhost user=postgres password=postnrt1964'||chr(39)||','||chr(39)||
    'select operation , stamp , user_aud, sync, db_instance , secuencia , 
            id_company,date , concept,  invoice_value , synced, updated_function from '||rec_comp.schema_name||'.facturacion_log'||chr(39)||')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying, secuencia integer, 
            id_company integer,date date, concept integer,  invoice_value money, synced timestamp, updated_function character varying(60)) 
            where id_company = '||rec_comp.id_company||';';
    --rAISE warning 'BD % stmt  %',rec_comp.db_instance,stmt;
    EXECUTE stmt;
    --Crear la vista de facturacion_log_col
    stmt = 'CREATE or replace VIEW ori.'||vista||'_log_col'||rec_comp.id_company||' AS SELECT *  
    FROM dblink('||chr(39)||'dbname='||rec_comp.db_instance||' host= localhost user=postgres password=postnrt1964'||chr(39)||','||chr(39)||
    'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, date, concept , campo, valor, synced from '||rec_comp.schema_name||'.facturacion_log_col'||chr(39)||')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying(30), secuencia integer, 
      id_company integer, date date, concept integer, campo character varying(60),  valor character varying(60), synced timestamp ) 
      where id_company = '||rec_comp.id_company||';';
    --rAISE warning 'BD % stmt  %',rec_comp.db_instance,stmt;
    EXECUTE stmt;

-------------------------------------------------------------------------------
    --Crear la VIsta de registros que no se deben sincronizar en Destino
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_ori_no'||rec_comp.id_company||' AS 
    --Registros de Insert de Origen
    select secuencia, db_instance
    from ori.facturacion_log orig
    where synced is null 
      and operation = '||chr(39)||'I'||chr(39)||'
      --Que  existan en el destino
      and ( exists (select 1
                        from ori.v_facturacion'||rec_comp.id_company||'
                      where id_company = orig.id_company
                        and date = orig.date
                        and concept = orig.concept)
      --que se hayan borrado en el destino                  
            or  exists (select 1
                        from ori.v_facturacion_log'||rec_comp.id_company||'
                      where id_company = orig.id_company
                        and date = orig.date
                        and concept = orig.concept
                        and operation = '||chr(39)||'D'||chr(39)||' and synced is null )
      --que se hayan borrado en el origen                  
            or  exists (select 1
                        from ori.facturacion_log
                      where id_company = orig.id_company
                        and date = orig.date
                        and concept = orig.concept
                        and operation = '||chr(39)||'D'||chr(39)||' and synced is not null
                        and id_company ='||rec_comp.id_company||' ))
                          and id_company ='||rec_comp.id_company||' ;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;
    --Crear la VIsta de registros a sincronizar en Destino
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_ori'||rec_comp.id_company||' AS 
    --Registros de Insert/Delete de Origen
    select operation,
      stamp,
      user_aud,
      sync,
      db_instance,
      secuencia,
      id_company,
      date,
      concept,
      invoice_value,
      synced,
      updated_function,    
      null campo,
      null valor
    from ori.facturacion_log orig
    where id_company ='||rec_comp.id_company||'
      and synced is null 
      and operation in ('||chr(39)||'I'||chr(39)||','||chr(39)||'D'||chr(39)||')
    union 
    --Registros de Update de Origen
    select operation,
      stamp,
      user_aud,
      sync,
      db_instance,
      secuencia,
      id_company,
      date,
      concept,
      null invoice_value,
      synced,
      null updated_function,    
      campo,
      valor
    from ori.facturacion_log_col 
    where id_company ='||rec_comp.id_company||'
      and synced is null 
    union 
    --Registros de Update de Destino
    select operation,
      stamp,
      user_aud,
      sync,
      db_instance,
      secuencia,
      id_company,
      date,
      concept,
      null invoice_value,
      synced,
      null updated_function,    
      campo,
      valor
    from ori.v_facturacion_log_col'||rec_comp.id_company||'  
    where id_company ='||rec_comp.id_company||'
      and synced is null 
    order by stamp;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;
    --Crear la VIsta con registros que no se deben sincronizar en Origen
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_des_no'||rec_comp.id_company||' AS 
      select secuencia, db_instance
        from ori.v_facturacion_log'||rec_comp.id_company||' des
        where synced is null 
          and operation = '||chr(39)||'I'||chr(39)||'
          --Que existan en el origen
          and (exists (select 1
                        from ori.facturacion
                      where id_company = des.id_company
                        and date = des.date
                        and concept = des.concept)
          --que  se hayan borrado en el origen                  
              or  exists (select 1
                            from ori.facturacion_log
                          where id_company = des.id_company
                            and date = des.date
                            and concept = des.concept
                            and operation = '||chr(39)||'D'||chr(39)||' and synced is null ))
          and id_company ='||rec_comp.id_company||' ;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;
    --VIsta con registros a sincronizar en Origen
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_des'||rec_comp.id_company||' AS
    --Registros de Insert/ Delete de Destino
      select operation,
          stamp,
          user_aud,
          sync,
          db_instance,
          secuencia,
          id_company,
          date,
          concept,
          invoice_value,
          synced,
          updated_function,    
          null campo,
          null valor
      from ori.v_facturacion_log'||rec_comp.id_company||' des
      where synced is null 
        and operation in ('||chr(39)||'I'||chr(39)||','||chr(39)||'D'||chr(39)||')
      union 
      --Registros de Update de Origen
      select operation,
          stamp,
          user_aud,
          sync,
          db_instance,
          secuencia,
          id_company,
          date,
          concept,
          null invoice_value,
          synced,
          null updated_function,    
          campo,
          valor
      from ori.facturacion_log_col 
      where id_company ='||rec_comp.id_company||'
        and synced is null 
      union 
      --Registros de Update de Destino
      select operation,
          stamp,
          user_aud,
          sync,
          db_instance,
          secuencia,
          id_company,
          date,
          concept,
          null invoice_value,
          synced,
          null updated_function,    
          campo,
          valor
       from ori.v_facturacion_log_col'||rec_comp.id_company||' 
      where id_company ='||rec_comp.id_company||'
        and synced is null 
      order by stamp;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_full_no'||rec_comp.id_company||' AS
    --Registros de Insert/ Delete de Destino
    select * from ori.'||vista||'_aud_des_no'||rec_comp.id_company||'
    union
    select * from ori.'||vista||'_aud_ori_no'||rec_comp.id_company||';';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;

    --VIsta con registros para sincronizar Origen y Destino
    stmt = 'CREATE or replace VIEW ori.'||vista||'_aud_full'||rec_comp.id_company||' AS
    --Registros de Insert/ Delete de Destino
    select * from ori.'||vista||'_aud_ori'||rec_comp.id_company||' 
    union 
    --Registros de Insert/ Delete de Origen
    select * from ori.'||vista||'_aud_des'||rec_comp.id_company||' 
    order by stamp;';
    --rAISE notice E'comando % ',stmt;
    EXECUTE stmt;

  end loop;
  close cur_comp;
end;
$$;
