-- Table: usuarios
 DROP TABLE if exists :sch.usuarios;

CREATE TABLE :sch.usuarios
(
    id integer NOT NULL,
    nombre character varying(60)  NOT NULL,
    apellido character varying(60) NOT NULL,
    created timestamp,
    updated timestamp,
    synced timestamp,
    CONSTRAINT usuarios_pk PRIMARY KEY (id)
)
TABLESPACE pg_default;

ALTER TABLE :sch.usuarios
    OWNER to postgres;

COMMENT ON TABLE :sch.usuarios
    IS 'Tabla de usuarios';

-- Table: Origen.usu_audit

 DROP TABLE if exists :sch.usu_audit;

CREATE TABLE :sch.usu_audit
(
    operation char(1) NOT NULL,
    stamp timestamp NOT NULL,
    user_aud text NOT NULL,
    sync timestamp NULL,
    id integer NOT NULL,
    nombre character varying(60) NOT NULL,
    apellido character varying(60) NOT NULL,
    created timestamp,
    updated timestamp,
    synced timestamp,
    CONSTRAINT usuaud_pk PRIMARY KEY (operation,id,stamp)
)
TABLESPACE pg_default;

ALTER TABLE :sch.usu_audit
    OWNER to postgres;

COMMENT ON TABLE :sch.usu_audit
    IS 'Tabla de auditoria de usuarios';