CREATE TRIGGER user_audit
--AFTER INSERT OR UPDATE OR DELETE ON usuarios
AFTER INSERT OR UPDATE OR DELETE ON :sch.usuarios
    FOR EACH ROW EXECUTE PROCEDURE Fu_AuditLog();

CREATE TRIGGER user_audit
--AFTER INSERT OR UPDATE OR DELETE ON ori.facturacion
AFTER INSERT OR UPDATE OR DELETE ON :sch.facturacion
    FOR EACH ROW EXECUTE PROCEDURE Fu_AuditLog();
