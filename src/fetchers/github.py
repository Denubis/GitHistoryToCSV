"""
GitHub commit fetcher.
"""

import logging
import requests
import time
logger = logging.getLogger(__name__)

class GitHubFetcher:
    """Fetch commit histories from GitHub repositories."""
    
    def __init__(self, client):
        self.client = client
        self.github_token = None
        
        # Extract token if client is initialized
        if client:
            try:
                self.github_token = client._Github__requester._Requester__auth.token
            except (AttributeError, TypeError):
                logger.warning("Could not extract GitHub token from client")
    
    def get_repo_with_redirect(self, repo_full_name):
        """Get GitHub repository with redirect handling."""
        try:
            # First try with PyGithub client
            try:
                repo = self.client.get_repo(repo_full_name)
                logger.info(f"Successfully retrieved GitHub repository: {repo_full_name}")
                return repo
            except Exception as e:
                logger.warning(f"Failed to get repository via PyGithub client: {str(e)}")
            
            # If PyGithub fails, try to handle redirect manually with REST API
            if self.github_token:
                headers = {"Authorization": f"Bearer {self.github_token}"}
                
                # Make a request to check for redirects
                check_url = f"https://api.github.com/repos/{repo_full_name}"
                response = requests.get(check_url, headers=headers, allow_redirects=False)
                
                if response.status_code in (301, 302, 307, 308):
                    redirect_location = response.headers.get('Location')
                    if redirect_location and '/repos/' in redirect_location:
                        new_repo_full_name = redirect_location.split('/repos/')[1]
                        logger.info(f"GitHub repository {repo_full_name} redirects to {new_repo_full_name}")
                        
                        # Try to get the repository again with the new name
                        try:
                            repo = self.client.get_repo(new_repo_full_name)
                            logger.info(f"Successfully retrieved GitHub repository after redirect: {new_repo_full_name}")
                            return repo
                        except Exception as e:
                            logger.error(f"Failed to get repository after redirect: {str(e)}")
                
                # Try to get repository by ID if available in the response
                if response.status_code == 200:
                    repo_data = response.json()
                    repo_id = repo_data.get('id')
                    if repo_id:
                        try:
                            repo = self.client.get_repo_by_id(repo_id)
                            logger.info(f"Successfully retrieved GitHub repository by ID: {repo_id}")
                            return repo
                        except Exception as e:
                            logger.error(f"Failed to get repository by ID: {str(e)}")
                            
                # If the response has a message about repository being moved
                if response.status_code == 404:
                    repo_data = response.json()
                    message = repo_data.get('message', '').lower()
                    
                    # Check if the message contains information about repository being moved
                    if 'moved' in message or 'renamed' in message:
                        for key, value in repo_data.get('errors', [{}])[0].items():
                            if key == 'resource' and value == 'repository':
                                for item in repo_data.get('errors', []):
                                    if 'new_location' in item:
                                        new_location = item.get('new_location')
                                        logger.info(f"Repository moved to: {new_location}")
                                        try:
                                            repo = self.client.get_repo(new_location)
                                            return repo
                                        except Exception as e:
                                            logger.error(f"Failed to get repository at new location: {str(e)}")
            
            # If we got here, we couldn't find the repository
            logger.error(f"Repository not found or not accessible: {repo_full_name}")
            return None
            
        except Exception as e:
            logger.error(f"Error handling GitHub repository redirect: {str(e)}")
            return None
    
    def fetch_commits(self, repo_info, max_commits=None):
        """Fetch commits from a GitHub repository."""
        try:
            # Parse repository information
            repo_full_name = repo_info.get('github', '')
            if not repo_full_name:
                logger.warning(f"No GitHub repository for {repo_info.get('item_name')}")
                return []
                
            logger.info(f"Fetching commits from GitHub: {repo_full_name}")
            
            # Get repository with redirect handling
            repo = self.get_repo_with_redirect(repo_full_name)
            
            if not repo:
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Repository not found or not accessible (could be moved, renamed, or private)"
                log_error_to_csv('output/errors.csv', repo_info, 'github', error_msg)
                return []
            
            # Get commits
            commits = []
            try:
                for commit in repo.get_commits():
                    commit_data = {
                        'item_name': repo_info.get('item_name'),
                        'date': commit.commit.author.date.isoformat() if commit.commit.author and commit.commit.author.date else "",
                        'message': commit.commit.message.split('\n')[0] if commit.commit.message else "",
                        'sha': commit.sha,
                        'author': commit.commit.author.name if commit.commit.author else "Unknown",
                        'year': commit.commit.author.date.year if commit.commit.author and commit.commit.author.date else ""
                    }
                    commits.append(commit_data)
                    
                    # Break if we've reached the maximum number of commits
                    if max_commits and len(commits) >= max_commits:
                        break
                        
                logger.info(f"Retrieved {len(commits)} commits from GitHub repo {repo.full_name}")
            except Exception as e:
                logger.error(f"Error fetching commits: {str(e)}")
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Error fetching commits: {str(e)}"
                log_error_to_csv('output/errors.csv', repo_info, 'github', error_msg)
            
            # Get releases and tags
            releases = []
            try:
                releases = self.fetch_releases(repo, repo_info.get('item_name'))
            except Exception as e:
                logger.error(f"Error fetching releases: {str(e)}")
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Error fetching releases: {str(e)}"
                log_error_to_csv('output/errors.csv', repo_info, 'github', error_msg)
            
            return commits + releases
            
        except Exception as e:
            logger.error(f"Error fetching GitHub commits: {str(e)}")
            # Log error to CSV
            from src.utils import log_error_to_csv
            error_msg = f"Error fetching GitHub commits: {str(e)}"
            log_error_to_csv('output/errors.csv', repo_info, 'github', error_msg)
            return []
    
    def fetch_releases(self, repo, item_name):
        """Fetch releases from a GitHub repository."""
        try:
            logger.info(f"Fetching releases from GitHub: {repo.full_name}")
            
            releases = []
            for release in repo.get_releases():
                release_data = {
                    'item_name': item_name,
                    'date': release.published_at.isoformat() if release.published_at else "",
                    'message': f"RELEASE: {release.title or release.tag_name}",
                    'sha': release.target_commitish,
                    'author': release.author.login if release.author else "Unknown",
                    'year': release.published_at.year if release.published_at else ""
                }
                releases.append(release_data)
                time.sleep(5)
                
            logger.info(f"Retrieved {len(releases)} releases from GitHub repo {repo.full_name}")
            return releases
            
        except Exception as e:
            logger.error(f"Error fetching GitHub releases: {str(e)}")
            return []