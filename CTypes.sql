    --Tabla con la informacion de las Compañias solo en BD_Central
drop type IF EXISTS ori.Reg_usuarios_aud;
CREATE TYPE ori.Reg_usuarios_aud AS (
operation         character(1),
 stamp             timestamp without time zone,
 user_aud          text,
 sync              timestamp without time zone,
 db_instance       character varying(30),
 secuencia         integer,
 id_company        integer,
 id                integer,
 nombre            character varying,
 apellido          character varying,
 created           timestamp without time zone,
 updated           timestamp without time zone,
 synced            timestamp without time zone,
 updated_function  character varying,
 campo             character varying,
 valor             character varying
);
