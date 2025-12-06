import uuid
import requests
import sys
import os

# Add flink_project directory to path to import config
flink_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if flink_project_path not in sys.path:
    sys.path.insert(0, flink_project_path)
from config.config import BaseConfig


class WalmartAccessTokenGenerator:
    """Singleton class for generating Walmart access tokens"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        """Singleton pattern implementation"""
        if cls._instance is None:
            cls._instance = super(WalmartAccessTokenGenerator, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration if not already initialized"""
        if not WalmartAccessTokenGenerator._initialized:
            self.config = BaseConfig().cfg
            WalmartAccessTokenGenerator._initialized = True
    
    def generate_access_token(self, account_tag):
        """Generate Walmart access token using client credentials flow
        
        Args:
            account_tag (str): Account name to generate token for (e.g., 'eForCity')
            
        Returns:
            dict: Dictionary containing access_token, token_type, and expires_in
            
        Raises:
            KeyError: If account_tag is not found in walmart_token config
            Exception: If token generation fails
        """
        if not isinstance(account_tag, str):
            raise KeyError('account_tag must be a str')
        
        walmart_token_config = self.config.get('walmart_token', {})
        if account_tag not in walmart_token_config:
            raise KeyError(f'account_tag "{account_tag}" not found in walmart_token config')
        
        account_config = walmart_token_config[account_tag]
        auth_token = account_config.get('token')
        
        if not auth_token:
            raise KeyError(f'token not found for account_tag "{account_tag}"')
        
        url = "https://marketplace.walmartapis.com/v3/token"
        headers = {
            "WM_SVC.NAME": "Walmart Marketplace",
            "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {auth_token}"
        }
        data = {
            "grant_type": "client_credentials"
        }
        
        try:
            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            return {account_tag: response.json()}
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to generate access token for {account_tag}: {str(e)}")


# Test example
if __name__ == "__main__":
    # Test Walmart access token generation
    print("Walmart Access Token Generator", "-" * 100)
    account_tag = 'eForCity'
    generator = WalmartAccessTokenGenerator()
    res = generator.generate_access_token(account_tag)
    print(f"Type: {type(res)}")
    print(f"Result: {res}")
    print()

