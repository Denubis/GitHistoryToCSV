"""
GitHub yearly commit fetcher.
Retrieves one commit per year using the REST API.
"""

import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

class GitHubYearlyFetcher:
    """Fetch one commit per year from GitHub repositories."""
    
    def __init__(self, client):
        self.client = client
        self.github_token = None
        
        # Extract token if client is initialized
        if client:
            try:
                self.github_token = client._Github__requester._Requester__auth.token
            except (AttributeError, TypeError):
                logger.warning("Could not extract GitHub token from client")
    
    def fetch_commits(self, repo_info, start_year=None, end_year=None):
        """Fetch one commit per year for the repository's history."""
        try:
            # Parse repository information
            repo_full_name = repo_info.get('github', '')
            if not repo_full_name:
                logger.warning(f"No GitHub repository for {repo_info.get('item_name')}")
                return []
                
            logger.info(f"Fetching one commit per year from GitHub: {repo_full_name}")
            
            # Set up the headers for API requests
            headers = {"Authorization": f"Bearer {self.github_token}"}
            
            # Get repository creation date from REST API
            repo_url = f"https://api.github.com/repos/{repo_full_name}"
            response = requests.get(repo_url, headers=headers)
            response.raise_for_status()
            
            repo_data = response.json()
            created_at = repo_data.get("created_at")
            created_year = int(created_at.split("-")[0])
            
            # Determine year range
            if not start_year:
                start_year = created_year
            if not end_year:
                end_year = datetime.now().year
            
            commits = []
            
            # For each year, get the first commit
            for year in range(start_year, end_year + 1):
                # Date range for the year
                start_date = f"{year}-01-01T00:00:00Z"
                end_date = f"{year}-12-31T23:59:59Z"
                
                # Get commits for this year
                commits_url = f"https://api.github.com/repos/{repo_full_name}/commits"
                params = {
                    'since': start_date,
                    'until': end_date,
                    'per_page': 1,  # We only need the first commit
                }
                
                try:
                    response = requests.get(commits_url, headers=headers, params=params)
                    response.raise_for_status()
                    
                    commit_data = response.json()
                    if commit_data:
                        commit = commit_data[0]
                        commit_info = {
                            'item_name': repo_info.get('item_name'),
                            'date': commit["commit"]["author"]["date"],
                            'message': commit["commit"]["message"].splitlines()[0],  # First line only
                            'sha': commit["sha"],
                            'author': commit["commit"]["author"]["name"],
                            'year': str(year)  # Add year for reference
                        }
                        commits.append(commit_info)
                        logger.info(f"Found commit for {year} on {commit_info['date']}")
                    else:
                        logger.info(f"No commits found for {year}")
                        
                except Exception as e:
                    logger.error(f"Error retrieving commits for {year}: {str(e)}")
            
            return commits
            
        except Exception as e:
            logger.error(f"Error fetching GitHub yearly commits: {str(e)}")
            return []