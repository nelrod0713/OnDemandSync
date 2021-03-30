--VIsta en origen para ver la informacion de la la BD bd_des
CREATE or replace VIEW ori.v_usuarios AS
SELECT *
    FROM dblink('dbname=bd_des host= localhost user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id_company, id , nombre, apellido, created, updated, synced, updated_function from des.usuarios')
      AS t1(id_company integer, id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, 
      synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log de la BD bd_des
CREATE or replace VIEW ori.v_usuarios_log AS
SELECT *
    FROM dblink('dbname=bd_des host= localhost user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, id , nombre, apellido, created, updated,
                 synced, updated_function from des.usuarios_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying, secuencia integer, 
            id_company integer,id integer, nombre character varying(60),  apellido character varying(60), created timestamp, 
            updated timestamp, synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log por columns de la BD bd_des
CREATE or replace VIEW ori.v_usuarios_log_col AS
SELECT *
    FROM dblink('dbname=bd_des host= localhost user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, id , campo, valor, synced from des.usuarios_log_col')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying(30), secuencia integer, 
      id_company integer, id integer, campo character varying(60),  valor character varying(60), synced timestamp);

--VIsta en origen para ver la informacion de la la BD bd_des
CREATE or replace VIEW ori.v_facturacion AS
SELECT *
    FROM dblink('dbname=bd_des host= localhost user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id_company , date , concept ,  invoice_value , synced , updated_function from des.facturacion')
      AS t1(id_company integer, date date, concept integer,  invoice_value money, synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log de la BD bd_des
CREATE or replace VIEW ori.v_facturacion_log AS
SELECT *
    FROM dblink('dbname=bd_des host= localhost user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation , stamp , user_aud, sync, db_instance , secuencia , 
            id_company,date , concept,  invoice_value , synced, updated_function from des.facturacion_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying, secuencia integer, 
            id_company integer,date date, concept integer,  invoice_value money, synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log por columns de la BD bd_des
CREATE or replace VIEW ori.v_facturacion_log_col AS
SELECT *
    FROM dblink('dbname=bd_des host= localhost user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, date, concept , campo, valor, synced
                from des.facturacion_log_col')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying(30), secuencia integer, 
      id_company integer, date date, concept integer, campo character varying(60),  valor character varying(60), synced timestamp);
--VIsta de registros que nos se deben sincronizar en Destino
CREATE or replace VIEW ori.v_usuarios_aud_ori_no AS
--Registros de Insert de Origen
select secuencia, db_instance
 from ori.usuarios_log orig
where synced is null 
  and operation = 'I'
  --Que  existan en el destino
  and ( exists (select 'x'
                    from ori.v_usuarios
                  where id_company = orig.id_company
                    and id = orig.id)
  --que se hayan borrado en el destino                  
        or  exists (select 'x'
                    from ori.v_usuarios_log
                  where id_company = orig.id_company
                    and id = orig.id
                    and operation = 'D' and synced is null )
  --que se hayan borrado en el origen                  
        or  exists (select 'x'
                    from ori.usuarios_log
                  where id_company = orig.id_company
                    and id = orig.id
                    and operation = 'D' and synced is not null ));

--VIsta de registros a sincronizar en Destino
CREATE or replace VIEW ori.v_usuarios_aud_ori AS
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
where synced is null 
  and operation in ('I','D')
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
where synced is null 
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
from ori.v_usuarios_log_col --_aud_des 
where synced is null 
order by stamp;

--VIsta con registros que no se deben sincronizar en Origen
CREATE or replace VIEW ori.v_usuarios_aud_des_no AS
--Registros de Insert/ Delete de Destino
select secuencia, db_instance
 from ori.v_usuarios_log des
where synced is null 
  and operation = 'I'
  --Que existan en el origen
  and (exists (select 'x'
                    from ori.usuarios
                  where id_company = des.id_company
                    and id = des.id)
  --que  se hayan borrado en el origen                  
       or  exists (select 'x'
                    from ori.usuarios_log
                  where id_company = des.id_company
                    and id = des.id
                    and operation = 'D' and synced is null ));
--VIsta con registros a sincronizar en Origen
CREATE or replace VIEW ori.v_usuarios_aud_des AS
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
 from ori.v_usuarios_log des
where synced is null 
  and operation IN ('I','D') 
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
where synced is null 
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
from ori.v_usuarios_log_col --_aud_des 
where synced is null 
order by stamp;

--VIsta con registros que no se deben sincronizar Origen y Destino
CREATE or replace VIEW ori.v_usuarios_aud_full_no AS
--Registros de Insert/ Delete de Destino
select * from ori.v_usuarios_aud_des_no
union
select * from ori.v_usuarios_aud_ori_no;

--VIsta con registros para sincronizar Origen y Destino
CREATE or replace VIEW ori.v_usuarios_aud_full AS
--Registros de Insert/ Delete de Destino
select * from ori.v_usuarios_aud_ori 
union 
--Registros de Insert/ Delete de Origen
select * from ori.v_usuarios_aud_des 
order by stamp;

--------------------------------------------------------
--                F A C T U R A C I O N               --
--------------------------------------------------------

--VIsta con registros que no se deben sincronizar en Origen
CREATE or replace VIEW ori.v_facturacion_aud_des_no AS
--Registros de Insert/ Delete de Destino
select secuencia, db_instance
 from ori.v_facturacion_log des
where synced is null 
  and operation = 'I'
  --Que existan en el origen
  and (exists (select 'x'
                    from ori.facturacion
                  where id_company = des.id_company
                    and date = des.date
                    and concept = des.concept)
  --que  se hayan borrado en el origen                  
       or  exists (select 'x'
                    from ori.facturacion_log
                  where id_company = des.id_company
                    and date = des.date
                    and concept = des.concept
                    and operation = 'D' and synced is null ));




--VIsta con registros a sincronizar en Origen
CREATE or replace VIEW ori.v_facturacion_aud_des AS
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
from ori.v_facturacion_log des
where synced is null 
  and operation IN  ('I','D')
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
    null concept,
    null invoice_value,
    synced,
    null updated_function,    
    campo,
    valor
from ori.facturacion_log_col 
where synced is null 
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
    null concept,
    null invoice_value,
    synced,
    null updated_function,    
    campo,
    valor
from ori.v_facturacion_log_col --_aud_des 
where synced is null 
order by stamp;

--VIsta de registros que nos se deben sincronizar en Destino
CREATE or replace VIEW ori.v_facturacion_aud_ori_no AS
--Registros de Insert de Origen
select secuencia, db_instance
 from ori.facturacion_log orig
where synced is null 
  and operation = 'I'
  --Que  existan en el destino
  and ( exists (select 'x'
                    from ori.v_facturacion
                  where id_company = orig.id_company
                    and date = orig.date
                    and concept = orig.concept)
  --que se hayan borrado en el destino                  
        or  exists (select 'x'
                    from ori.v_facturacion_log
                  where id_company = orig.id_company
                    and date = orig.date
                    and concept = orig.concept
                    and operation = 'D' and synced is null )
  --que se hayan borrado en el origen                  
        or  exists (select 'x'
                    from ori.facturacion_log
                  where id_company = orig.id_company
                    and date = orig.date
                    and concept = orig.concept
                    and operation = 'D' and synced is not null ));

CREATE or replace VIEW ori.v_facturacion_aud_ori AS
--Registros de Insert/ Delete de Origen
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
where synced is null 
  and operation IN  ('I','D')
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
    null concept,
    null invoice_value,
    synced,
    null updated_function,    
    campo,
    valor
from ori.facturacion_log_col 
where synced is null 
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
    null concept,
    null invoice_value,
    synced,
    null updated_function,    
    campo,
    valor
from ori.v_facturacion_log_col --_aud_des 
where synced is null 
order by stamp;

--VIsta con registros que no se deben sincronizar Origen y Destino
CREATE or replace VIEW ori.v_facturacion_aud_full_no AS
--Registros de Insert/ Delete de Destino
select * from ori.v_facturacion_aud_des_no
union
select * from ori.v_facturacion_aud_ori_no;

--VIsta con registros para sincronizar Origen y Destino
CREATE or replace VIEW ori.v_facturacion_aud_full AS
--Registros de Insert/ Delete de Destino
select * from ori.v_facturacion_aud_ori 
union 
--Registros de Insert/ Delete de Origen
select * from ori.v_facturacion_aud_des 
order by stamp;
