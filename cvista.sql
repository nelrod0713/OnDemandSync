--VIsta en origen para ver la informacion de la la BD bd_des
CREATE or replace VIEW ori.v_usuarios AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id_company, id , nombre, apellido, created, updated, synced, updated_function from des.usuarios')
      AS t1(id_company integer, id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, 
      synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log de la BD bd_des
CREATE or replace VIEW ori.v_usuarios_log AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, id , nombre, apellido, created, updated, synced from des.usuarios_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying, secuencia integer, 
            id_company integer,id integer, nombre character varying(60),  apellido character varying(60), created timestamp, 
            updated timestamp, synced timestamp);

--VIsta en origen para ver la informacion del log por columns de la BD bd_des
CREATE or replace VIEW ori.v_usuarios_log_col AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, id , campo, valor, synced from des.usuarios_log_col')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying(30), secuencia integer, 
      id_company integer, id integer, campo character varying(60),  valor character varying(60), synced timestamp);


--VIsta en destino para ver la informacion de la la BD bd_ori
CREATE or replace VIEW des.v_usuarios AS
SELECT *
    FROM dblink('dbname=bd_ori host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id_company ,id , nombre, apellido, created, updated, synced from ori.usuarios')
      AS t1(id_company integer,id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, synced timestamp);      

--VIsta en Destino para ver la informacion del log de la BD bd_ori
CREATE or replace VIEW des.v_usuarios_log AS
SELECT *
    FROM dblink('dbname=bd_ori host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, id_company , id, nombre, apellido, created, updated, synced from des.usuarios_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, id_company integer,id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, synced timestamp);

--VIsta en origen para ver la informacion de la la BD bd_des
CREATE or replace VIEW ori.v_facturacion AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id_company , date , concept ,  invoice_value , synced , updated_function from des.facturacion')
      AS t1(id_company integer, date date, concept integer,  invoice_value money, synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log de la BD bd_des
CREATE or replace VIEW ori.v_facturacion_log AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation , stamp , user_aud, sync, db_instance , secuencia , 
            id_company,date , concept,  invoice_value , synced, updated_function from des.facturacion_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying, secuencia integer, 
            id_company integer,date date, concept integer,  invoice_value money, synced timestamp, updated_function character varying(60));

--VIsta en origen para ver la informacion del log por columns de la BD bd_des
CREATE or replace VIEW ori.v_facturacion_log_col AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, db_instance, secuencia, id_company, date, concept , campo, valor, synced
                from des.facturacion_log_col')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, db_instance character varying(30), secuencia integer, 
      id_company integer, date date, concept integer, campo character varying(60),  valor character varying(60), synced timestamp);

