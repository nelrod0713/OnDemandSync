--Crear registros de las comoañias
INSERT INTO public.companys(
	id_company, nombre, db_instance, db_central, host, schema_name)
	VALUES (0, 'BD de Administración', 'bd_ori', 'S','localhost','ori');

INSERT INTO public.companys(
	id_company, nombre, db_instance, db_central, host, schema_name)
	VALUES (1, 'BD Cliente1', 'bd_des1', 'N','localhost','des1');

INSERT INTO public.companys(
	id_company, nombre, db_instance, db_central, host, schema_name)
	VALUES (2, 'BD Cliente2', 'bd_des2', 'N','localhost','des2');

