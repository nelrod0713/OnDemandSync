-- Table: Origen.usu_audit

 DROP TABLE if exists ori.usu_audit;

CREATE TABLE ori.usu_audit
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
    CONSTRAINT usuaud_pk PRIMARY KEY (operation,id,stamp)
)
TABLESPACE pg_default;

ALTER TABLE ori.usu_audit
    OWNER to postgres;

COMMENT ON TABLE ori.usu_audit
    IS 'Tabla de auditoria de usuarios';

