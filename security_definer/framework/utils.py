"""
Utility functions for SQL SECURITY DEFINER testing
"""

from framework.config import CATALOG, SCHEMA


def fqn(object_name: str, object_type: str = "TABLE") -> str:
    """
    Generate fully qualified name for database objects
    
    Args:
        object_name: Name of the object (table, procedure, etc.)
        object_type: Type of object (TABLE, PROCEDURE, etc.) - for documentation
        
    Returns:
        Fully qualified name: catalog.schema.object_name
        
    Example:
        >>> fqn("my_table")
        'ad_bugbash.ad_bugbash_schema.my_table'
        
        >>> fqn("my_proc", "PROCEDURE")
        'ad_bugbash.ad_bugbash_schema.my_proc'
    """
    return f"{CATALOG}.{SCHEMA}.{object_name}"


def drop_if_exists(object_name: str, object_type: str = "TABLE") -> str:
    """
    Generate DROP IF EXISTS statement with fully qualified name
    
    Args:
        object_name: Name of the object
        object_type: Type (TABLE, PROCEDURE, etc.)
        
    Returns:
        DROP statement with fully qualified name
        
    Example:
        >>> drop_if_exists("my_table", "TABLE")
        'DROP TABLE IF EXISTS ad_bugbash.ad_bugbash_schema.my_table'
        
        >>> drop_if_exists("my_proc", "PROCEDURE")
        'DROP PROCEDURE IF EXISTS ad_bugbash.ad_bugbash_schema.my_proc'
    """
    return f"DROP {object_type} IF EXISTS {fqn(object_name)}"


def grant_execute(procedure_name: str, grantee: str) -> str:
    """
    Generate GRANT EXECUTE statement with fully qualified name
    
    Args:
        procedure_name: Name of the procedure
        grantee: User or service principal to grant to
        
    Returns:
        GRANT statement
        
    Example:
        >>> grant_execute("my_proc", "user@example.com")
        'GRANT EXECUTE ON PROCEDURE ad_bugbash.ad_bugbash_schema.my_proc TO `user@example.com`'
    """
    return f"GRANT EXECUTE ON PROCEDURE {fqn(procedure_name)} TO `{grantee}`"


def revoke_all(object_name: str, object_type: str, grantee: str) -> str:
    """
    Generate REVOKE ALL statement with fully qualified name
    
    Args:
        object_name: Name of the object
        object_type: Type (TABLE, PROCEDURE, etc.)
        grantee: User or service principal to revoke from
        
    Returns:
        REVOKE statement
        
    Example:
        >>> revoke_all("my_table", "TABLE", "user@example.com")
        'REVOKE ALL PRIVILEGES ON TABLE ad_bugbash.ad_bugbash_schema.my_table FROM `user@example.com`'
    """
    return f"REVOKE ALL PRIVILEGES ON {object_type} {fqn(object_name)} FROM `{grantee}`"


def call_procedure(procedure_name: str, *args) -> str:
    """
    Generate CALL statement with fully qualified name
    
    Args:
        procedure_name: Name of the procedure
        *args: Arguments to pass to procedure
        
    Returns:
        CALL statement
        
    Example:
        >>> call_procedure("my_proc")
        'CALL ad_bugbash.ad_bugbash_schema.my_proc()'
        
        >>> call_procedure("my_proc", "'param1'", "123")
        "CALL ad_bugbash.ad_bugbash_schema.my_proc('param1', 123)"
    """
    args_str = ", ".join(str(arg) for arg in args)
    return f"CALL {fqn(procedure_name)}({args_str})"


# Example usage in tests:
"""
from framework.utils import fqn, drop_if_exists, grant_execute, call_procedure
from framework.config import SERVICE_PRINCIPAL_B

setup_sql = [
    drop_if_exists("my_table", "TABLE"),
    f"CREATE TABLE {fqn('my_table')} (id INT, data STRING)",
    drop_if_exists("my_proc", "PROCEDURE"),
    f'''
    CREATE PROCEDURE {fqn("my_proc")}()
    LANGUAGE SQL
    AS BEGIN
        SELECT * FROM {fqn("my_table")};
    END
    ''',
    grant_execute("my_proc", SERVICE_PRINCIPAL_B)
]

test_sql = call_procedure("my_proc")

teardown_sql = [
    drop_if_exists("my_proc", "PROCEDURE"),
    drop_if_exists("my_table", "TABLE")
]
"""
