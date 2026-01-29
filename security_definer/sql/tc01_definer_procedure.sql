-- TC-01: Create DEFINER procedure (default mode)
-- This procedure should return the owner's identity
-- Using fully qualified name: catalog.schema.procedure_name
CREATE OR REPLACE PROCEDURE ad_bugbash.ad_bugbash_schema.tc01_proc_definer()
LANGUAGE SQL
COMMENT 'Test procedure with DEFINER security (default)'
AS BEGIN
    SELECT 
        current_user() as current_user,
        session_user() as session_user,
        'DEFINER mode' as security_mode;
END;
