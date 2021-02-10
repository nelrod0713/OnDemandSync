--VIsta en origen para ver la informacion de la la BD bd_des
CREATE or replace VIEW ori.v_usuarios AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id , nombre, apellido, created, updated, synced from des.usuarios')
      AS t1(id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, synced timestamp);

--VIsta en origen para ver la informacion del log de la BD bd_des
CREATE or replace VIEW ori.v_usuarios_log AS
SELECT *
    FROM dblink('dbname=bd_des host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, id , nombre, apellido, created, updated, synced from des.usuarios_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, synced timestamp);

--VIsta en destino para ver la informacion de la la BD bd_ori
CREATE or replace VIEW des.v_usuarios AS
SELECT *
    FROM dblink('dbname=bd_ori host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select id , nombre, apellido, created, updated, synced from ori.usuarios')
      AS t1(id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, synced timestamp);      

--VIsta en Destino para ver la informacion del log de la BD bd_ori
CREATE or replace VIEW des.v_usuarios_log AS
SELECT *
    FROM dblink('dbname=bd_ori host= 192.168.1.102 user=postgres password=postnrt1964', -- options=-csearch_path=',
                'select operation, stamp, user_aud, sync, id , nombre, apellido, created, updated, synced from des.usuarios_log')
      AS t1(operation character(1), stamp timestamp, user_aud text, sync timestamp, id integer, nombre character varying(60),  apellido character varying(60), created timestamp, updated timestamp, synced timestamp);
