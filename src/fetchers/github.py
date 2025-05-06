"""
GitHub commit fetcher.
"""

import logging

logger = logging.getLogger(__name__)

class GitHubFetcher:
    """Fetch commit histories from GitHub repositories."""
    
    def __init__(self, client):
        self.client = client
    
    def fetch_commits(self, repo_info, max_commits=None):
        """Fetch commits from a GitHub repository."""
        try:
            # Parse repository information
            repo_full_name = repo_info.get('github', '')
            if not repo_full_name:
                logger.warning(f"No GitHub repository for {repo_info.get('item_name')}")
                return []
                
            logger.info(f"Fetching commits from GitHub: {repo_full_name}")
            
            # Get repository
            repo = self.client.get_repo(repo_full_name)
            
            # Get commits
            commits = []
            for commit in repo.get_commits():
                commit_data = {
                    'item_name': repo_info.get('item_name'),
                    'date': commit.commit.author.date.isoformat() if commit.commit.author and commit.commit.author.date else "",
                    'message': commit.commit.message.split('\n')[0] if commit.commit.message else "",
                    'sha': commit.sha,
                    'author': commit.commit.author.name if commit.commit.author else "Unknown"
                }
                commits.append(commit_data)
                
                # Break if we've reached the maximum number of commits
                if max_commits and len(commits) >= max_commits:
                    break
                    
            logger.info(f"Retrieved {len(commits)} commits from GitHub repo {repo_full_name}")
            
            # Get releases and tags
            releases = self.fetch_releases(repo, repo_info.get('item_name'))
            
            return commits + releases
            
        except Exception as e:
            logger.error(f"Error fetching GitHub commits: {str(e)}")
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
                    'author': release.author.login if release.author else "Unknown"
                }
                releases.append(release_data)
                
            logger.info(f"Retrieved {len(releases)} releases from GitHub repo {repo.full_name}")
            return releases
            
        except Exception as e:
            logger.error(f"Error fetching GitHub releases: {str(e)}")
            return []