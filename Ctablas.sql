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

 DROP TABLE if exists :sch.usuarios_log;

CREATE TABLE :sch.usuarios_log
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

ALTER TABLE :sch.usuarios_log
    OWNER to postgres;

COMMENT ON TABLE :sch.usuarios_log
    IS 'Tabla de auditoria de usuarios';

-- Table: facturacion
DROP TABLE IF EXISTS :sch.facturacion;
CREATE TABLE :sch.facturacion
(
    id_company integer NOT NULL,
    date date NOT NULL,
    concept integer NOT NULL,
    invoice_value money,
    synced timestamp,
    CONSTRAINT fact_pkey PRIMARY KEY (id_company, date, concept)	
);
DROP TABLE IF EXISTS :sch.facturacion_log;
CREATE TABLE :sch.facturacion_log
(
    operation char(1) NOT NULL,
    stamp timestamp NOT NULL,
    user_aud text NOT NULL,
    sync timestamp NULL,
    id_company integer NOT NULL,
    date date NOT NULL,
    concept integer NOT NULL,
    invoice_value money,
    synced timestamp,
    CONSTRAINT logfact_pkey PRIMARY KEY (operation, stamp, concept)	
);

--Tabla para guardar el log de sincronizaciones
DROP TABLE IF EXISTS :sch.Sync_Procs_Log;
CREATE TABLE :sch.Sync_Procs_Log
(
    Schema_table character varying(60) NOT NULL,
    Table_name character varying(60) NOT NULL,
    Sync_Type char(1) NOT NULL CHECK (Sync_Type IN ('F','O','D')), --F Full O Origen D Destino
    stamp timestamp NOT NULL,
    user_proc text NOT NULL,
    CONSTRAINT SyncProc_pkey PRIMARY KEY (Schema_table, Table_name, Sync_Type, stamp)
    --CONSTRAINT type_check );	
);
