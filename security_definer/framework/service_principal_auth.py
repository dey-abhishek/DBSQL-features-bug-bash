"""
Service Principal Authentication Module

Supports two authentication methods:
1. PAT-based (simpler, recommended for testing)
2. OAuth 2.0 M2M (for production)
"""

from databricks import sql
from typing import Optional, Tuple, List
import time


class ServicePrincipalAuth:
    """
    Authenticates and executes queries as a service principal
    """
    
    def __init__(self, 
                 server_hostname: str,
                 http_path: str,
                 sp_token: Optional[str] = None,
                 sp_client_id: Optional[str] = None,
                 sp_client_secret: Optional[str] = None,
                 catalog: str = None,
                 schema: str = None):
        """
        Initialize service principal authentication
        
        Parameters:
        -----------
        server_hostname : str
            Databricks workspace hostname (without https://)
        http_path : str
            SQL warehouse HTTP path
        sp_token : str, optional
            Service principal PAT (Method 1 - simpler)
        sp_client_id : str, optional
            OAuth client ID (Method 2 - OAuth)
        sp_client_secret : str, optional
            OAuth client secret (Method 2 - OAuth)
        catalog : str, optional
            Default catalog to use
        schema : str, optional
            Default schema to use
        """
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.catalog = catalog
        self.schema = schema
        self._connection = None
        
        # Determine authentication method
        if sp_token:
            self.auth_method = "PAT"
            self.sp_token = sp_token
        elif sp_client_id and sp_client_secret:
            self.auth_method = "OAuth"
            self.sp_client_id = sp_client_id
            self.sp_client_secret = sp_client_secret
        else:
            raise ValueError("Must provide either sp_token (PAT) or sp_client_id + sp_client_secret (OAuth)")
    
    def connect(self) -> bool:
        """
        Establish connection using service principal credentials
        
        Returns:
        --------
        bool : True if connection successful
        """
        try:
            if self.auth_method == "PAT":
                return self._connect_with_pat()
            else:
                return self._connect_with_oauth()
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def _connect_with_pat(self) -> bool:
        """Connect using service principal PAT"""
        print(f"🔐 Connecting as service principal using PAT...")
        
        self._connection = sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.sp_token
        )
        
        # Set catalog and schema if provided
        if self.catalog:
            cursor = self._connection.cursor()
            cursor.execute(f"USE CATALOG {self.catalog}")
            if self.schema:
                cursor.execute(f"USE SCHEMA {self.schema}")
            cursor.close()
        
        print(f"✅ Connected as service principal (PAT)")
        return True
    
    def _connect_with_oauth(self) -> bool:
        """Connect using OAuth 2.0 M2M flow"""
        print(f"🔐 Connecting as service principal using OAuth M2M...")
        
        # Use databricks-sql-connector's built-in OAuth support
        # Pass client_id and client_secret directly
        self._connection = sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            auth_type="databricks-oauth",
            client_id=self.sp_client_id,
            client_secret=self.sp_client_secret
        )
        
        # Set catalog and schema if provided
        if self.catalog:
            cursor = self._connection.cursor()
            cursor.execute(f"USE CATALOG {self.catalog}")
            if self.schema:
                cursor.execute(f"USE SCHEMA {self.schema}")
            cursor.close()
        
        print(f"✅ Connected as service principal (OAuth M2M)")
        return True
    
    def execute(self, query: str) -> Tuple[Optional[List[Tuple]], Optional[str]]:
        """
        Execute a SQL query as the service principal
        
        Parameters:
        -----------
        query : str
            SQL query to execute
            
        Returns:
        --------
        Tuple[Optional[List[Tuple]], Optional[str]]
            (results, error_message)
        """
        if not self._connection:
            return None, "Not connected. Call connect() first."
        
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            
            # Fetch results if it's a SELECT
            if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE', 'CALL')):
                results = cursor.fetchall()
            else:
                results = None
            
            cursor.close()
            return results, None
            
        except Exception as e:
            return None, str(e)
    
    def get_current_user(self) -> Optional[str]:
        """Get the current user (should be service principal)"""
        result, error = self.execute("SELECT CURRENT_USER()")
        if error:
            return None
        return result[0][0] if result else None
    
    def close(self):
        """Close the connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
            print("🔌 Service principal connection closed")


def setup_service_principal_pat_instructions():
    """
    Print instructions for setting up service principal PAT
    """
    print("=" * 80)
    print("📋 SETUP: Generate Service Principal PAT")
    print("=" * 80)
    print()
    print("To authenticate as a service principal, you need to generate a PAT:")
    print()
    print("1. Go to your Databricks workspace")
    print("2. Navigate to: User Settings → Developer → Access Tokens")
    print("   OR go to: Settings → Identity and Access → Service Principals")
    print()
    print("3. Select the service principal: bugbash_ad_sp")
    print("4. Click 'Generate New Token'")
    print("5. Give it a name: 'bug_bash_testing'")
    print("6. Set lifetime (e.g., 90 days)")
    print("7. Copy the generated token")
    print()
    print("8. Add to framework/config.py:")
    print("   SERVICE_PRINCIPAL_PAT = 'dapi...' # paste token here")
    print()
    print("Alternative: Use environment variable")
    print("   export SERVICE_PRINCIPAL_PAT='dapi...'")
    print()
    print("=" * 80)


if __name__ == "__main__":
    # Show setup instructions
    setup_service_principal_pat_instructions()
    
    print()
    print("Once you have the service principal PAT, you can test authentication:")
    print()
    print("Example usage:")
    print("""
from framework.service_principal_auth import ServicePrincipalAuth
from framework.config import SERVER_HOSTNAME, HTTP_PATH, CATALOG, SCHEMA

# Method 1: PAT-based (recommended)
sp_auth = ServicePrincipalAuth(
    server_hostname=SERVER_HOSTNAME,
    http_path=HTTP_PATH,
    sp_token="dapi...",  # Service principal PAT
    catalog=CATALOG,
    schema=SCHEMA
)

if sp_auth.connect():
    # Verify we're connected as SP
    user = sp_auth.get_current_user()
    print(f"Connected as: {user}")
    
    # Execute a test query
    result, error = sp_auth.execute("SELECT CURRENT_USER(), CURRENT_CATALOG()")
    print(f"Result: {result}")
    
    sp_auth.close()
""")
