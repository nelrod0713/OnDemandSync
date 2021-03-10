--VIsta en origen para ver la informacion de la la BD bd_des
CREATE or replace VIEW ori.v_usuarios AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.108 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id_company, id , nombre, apellido, created, updated, synced, updated_function from des.usuarios')
      AS t1(id_company integer, id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, 
      synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log de la BD bd_des
CREATE or replace VIEW ori.v_usuarios_log AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.108 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, id , nombre, apellido, created, updated,
                 synced, updated_function from des.usuarios_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying, secuencia integer, 
            id_company integer,id integer, nombre character varying(60),  apellido character varying(60), created timestamp, 
            updated timestamp, synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log por columns de la BD bd_des
CREATE or replace VIEW ori.v_usuarios_log_col AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.108 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, id , campo, valor, synced from des.usuarios_log_col')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying(30), secuencia integer, 
      id_company integer, id integer, campo character varying(60),  valor character varying(60), synced timestamp);


--VIsta en destino para ver la informacion de la la BD bd_ori
CREATE or replace VIEW des.v_usuarios AS
SELECT *
    FROM dblink('dbname=bd_ori host= 192.168.1.108 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id_company ,id , nombre, apellido, created, updated, synced from ori.usuarios')
      AS t1(id_company integer,id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, synced timestamp);      

--VIsta en Destino para ver la informacion del log de la BD bd_ori
CREATE or replace VIEW des.v_usuarios_log AS
SELECT *
    FROM dblink('dbname=bd_ori host= 192.168.1.108 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, id_company , id, nombre, apellido, created, updated, synced from des.usuarios_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, id_company integer,id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, synced timestamp);

--VIsta en origen para ver la informacion de la la BD bd_des
CREATE or replace VIEW ori.v_facturacion AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.108 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id_company , date , concept ,  invoice_value , synced , updated_function from des.facturacion')
      AS t1(id_company integer, date date, concept integer,  invoice_value money, synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log de la BD bd_des
CREATE or replace VIEW ori.v_facturacion_log AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.108 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation , stamp , user_aud, sync, db_instance , secuencia , 
            id_company,date , concept,  invoice_value , synced, updated_function from des.facturacion_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying, secuencia integer, 
            id_company integer,date date, concept integer,  invoice_value money, synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log por columns de la BD bd_des
CREATE or replace VIEW ori.v_facturacion_log_col AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.108 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, date, concept , campo, valor, synced
                from des.facturacion_log_col')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying(30), secuencia integer, 
      id_company integer, date date, concept integer, campo character varying(60),  valor character varying(60), synced timestamp);
--VIsta de registros a sincronizar en Destino
CREATE or replace VIEW ori.v_usuarios_aud_ori AS
--Registros de Insert de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id
 from ori.usuarios_log orig
where synced is null 
  and operation = 'I'
  --Que no exista en el destino
  and not exists (select 'x'
                    from ori.v_usuarios
                  where id_company = orig.id_company
                    and id = orig.id)
  --que no se haya borrado en el destino                  
  and not exists (select 'x'
                    from ori.v_usuarios_log
                  where id_company = orig.id_company
                    and id = orig.id
                    and operation = 'D' and synced is null )
--Registros de Delete de Origen
union
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id
 from ori.usuarios_log orig
where synced is null 
  and operation = 'D'
  --que exista en el destino
  and exists (select 'x'
                    from ori.v_usuarios
                  where id_company = orig.id_company
                    and id = orig.id)
union 
--Registros de Update de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id 
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
    id 
from ori.v_usuarios_log_col --_aud_des 
where synced is null 
order by stamp;

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
    id
 from ori.v_usuarios_log des
where synced is null 
  and operation = 'I'
  --Que no exista en el origen
  and not exists (select 'x'
                    from ori.usuarios
                  where id_company = des.id_company
                    and id = des.id)
  --que no se haya borrado en el origen                  
  and not exists (select 'x'
                    from ori.usuarios_log
                  where id_company = des.id_company
                    and id = des.id
                    and operation = 'D' and synced is null )

--Registros de Delete de Destino
union
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id
 from ori.v_usuarios_log des
where synced is null 
  and operation = 'D'
  --que exista en el origen
  and exists (select 'x'
                    from ori.usuarios
                  where id_company = des.id_company
                    and id = des.id)
union 
--Registros de Update de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id 
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
    id 
from ori.v_usuarios_log_col --_aud_des 
where synced is null 
order by stamp;

--VIsta con registros para sincronizar Origen y Destino
CREATE or replace VIEW ori.v_usuarios_aud_full AS
--Registros de Insert/ Delete de Destino
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id
 from ori.v_usuarios_aud_ori 
union 
--Registros de Insert/ Delete de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company,
    id
 from ori.v_usuarios_aud_des 
order by stamp;

--VIsta con registros a sincronizar en Origen
CREATE or replace VIEW ori.v_facturacion_aud_des AS
--Registros de Insert/ Delete de Destino
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company
 from ori.v_facturacion_log 
where synced is null 
union 
--Registros de Update de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company
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
    id_company
from ori.v_facturacion_log_col --_aud_des 
where synced is null 
order by stamp;

--VIsta con registros para sincronizar Origen y Destino
CREATE or replace VIEW ori.v_facturacion_aud_full AS
--Registros de Insert/ Delete de Destino
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company
 from ori.v_facturacion_log 
where synced is null 
union 
--Registros de Insert/ Delete de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company
 from ori.facturacion_log 
where synced is null 
union 
--Registros de Update de Origen
select operation,
    stamp,
    user_aud,
    sync,
    db_instance,
    secuencia,
    id_company
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
    id_company
from ori.v_facturacion_log_col --_aud_des 
where synced is null 
order by stamp;


