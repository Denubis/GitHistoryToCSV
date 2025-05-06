"""
Authentication manager for Git APIs.
"""

import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class GitAuthManager:
    """Manage authentication for different Git platforms."""
    
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Get authentication credentials
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.gitlab_token = os.getenv("GITLAB_TOKEN")
        self.bitbucket_username = os.getenv("BITBUCKET_USERNAME")
        self.bitbucket_password = os.getenv("BITBUCKET_APP_PASSWORD")
        
        # Validate credentials
        self.validate_credentials()
    
    def validate_credentials(self):
        """Validate that credentials are available for each platform."""
        if not self.github_token:
            logger.warning("GitHub token not found in environment variables")
        
        if not self.gitlab_token:
            logger.warning("GitLab token not found in environment variables")
        
        if not self.bitbucket_username or not self.bitbucket_password:
            logger.warning("Bitbucket credentials not found in environment variables")
            
    def get_github_client(self):
        """Get authenticated GitHub client."""
        if not self.github_token:
            return None
            
        try:
            from github import Github
            return Github(self.github_token)
        except ImportError:
            logger.error("PyGithub package not installed")
            return None
    
    def get_gitlab_client(self):
        """Get authenticated GitLab client."""
        if not self.gitlab_token:
            return None
            
        try:
            import gitlab
            gl = gitlab.Gitlab('https://gitlab.com', private_token=self.gitlab_token)
            gl.auth()
            return gl
        except ImportError:
            logger.error("python-gitlab package not installed")
            return None
    
    def get_bitbucket_client(self):
        """Get authenticated Bitbucket client."""
        if not self.bitbucket_username or not self.bitbucket_password:
            return None
            
        try:
            from atlassian import Bitbucket
            return Bitbucket(
                url="https://api.bitbucket.org",
                username=self.bitbucket_username,
                password=self.bitbucket_password,
                cloud=True
            )
        except ImportError:
            logger.error("atlassian-python-api package not installed")
            return None