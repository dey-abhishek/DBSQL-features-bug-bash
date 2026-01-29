-- TC-01: Create INVOKER procedure (explicit SQL SECURITY INVOKER)
-- This procedure should return the caller's identity
-- Using fully qualified name: catalog.schema.procedure_name
CREATE OR REPLACE PROCEDURE ad_bugbash.ad_bugbash_schema.tc01_proc_invoker()
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Test procedure with INVOKER security'
AS BEGIN
    SELECT 
        current_user() as current_user,
        session_user() as session_user,
        'INVOKER mode' as security_mode;
END;
