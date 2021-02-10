-- Table: Origen.usuarios_log_col
\c :bd
 DROP TABLE if exists :sch.usuarios_log_col;

CREATE TABLE :sch.usuarios_log_col
(
    operation char(1) NOT NULL,
    stamp timestamp NOT NULL,
    user_aud text NOT NULL,
    sync timestamp NULL,
    id integer NOT NULL,
    campo VARCHAR NOT NULL,
    valor VARCHAR NOT NULL,
    synced timestamp,
    CONSTRAINT usulogcol_pk PRIMARY KEY (operation,id,stamp,campo)
)
TABLESPACE pg_default;

ALTER TABLE :sch.usuarios_log_col
    OWNER to postgres;

COMMENT ON TABLE :sch.usuarios_log_col
    IS 'Tabla de auditoria de usuarios por columna';

