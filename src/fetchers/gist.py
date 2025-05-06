"""
GitHub Gist commit fetcher.
"""

import logging
import requests

logger = logging.getLogger(__name__)

class GistFetcher:
    """Fetch commit histories from GitHub Gists."""
    
    def __init__(self, client):
        self.client = client
        self.github_token = None
        
        # Extract token if client is initialized
        if client:
            try:
                self.github_token = client._Github__requester._Requester__auth.token
            except (AttributeError, TypeError):
                logger.warning("Could not extract GitHub token from client")
    
    def fetch_commits(self, repo_info, max_commits=None):
        """Fetch commits from a GitHub Gist."""
        try:
            # Parse repository information
            gist_id = repo_info.get('gist', '')
            if not gist_id or not isinstance(gist_id, str):
                logger.warning(f"No valid Gist ID for {repo_info.get('item_name')}")
                return []
            
            # Extract gist ID from URL if needed
            if '/' in gist_id:
                gist_id = gist_id.split('/')[-1]
                
            logger.info(f"Fetching commits from GitHub Gist: {gist_id}")
            
            # GitHub API doesn't provide a direct way to get commit history for Gists
            # We need to use a custom approach
            headers = {}
            if self.github_token:
                headers['Authorization'] = f'token {self.github_token}'
            
            # Get gist details
            url = f"https://api.github.com/gists/{gist_id}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            gist_data = response.json()
            
            # For gists, we can only get the latest revision
            # We'll return it as a single commit
            commit_data = {
                'item_name': repo_info.get('item_name'),
                'date': gist_data.get('updated_at', ""),
                'message': "Latest Gist revision",
                'sha': gist_data.get('id', ""),
                'author': gist_data.get('owner', {}).get('login', "Unknown") if isinstance(gist_data.get('owner'), dict) else "Unknown"
            }
            
            logger.info(f"Retrieved latest revision for GitHub Gist {gist_id}")
            return [commit_data]
            
        except Exception as e:
            logger.error(f"Error fetching GitHub Gist commits: {str(e)}")
            return []