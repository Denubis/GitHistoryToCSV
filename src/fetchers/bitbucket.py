"""
Bitbucket commit fetcher.
"""

import logging

logger = logging.getLogger(__name__)

class BitbucketFetcher:
    """Fetch commit histories from Bitbucket repositories."""
    
    def __init__(self, client):
        self.client = client
    
    def fetch_commits(self, repo_info, max_commits=None):
        """Fetch commits from a Bitbucket repository."""
        try:
            # Parse repository information
            repo_full_name = repo_info.get('bitbucket', '')
            if not repo_full_name or not isinstance(repo_full_name, str):
                logger.warning(f"No valid Bitbucket repository for {repo_info.get('item_name')}")
                return []
                
            logger.info(f"Fetching commits from Bitbucket: {repo_full_name}")
            
            # Parse workspace and repo slug
            parts = repo_full_name.split('/')
            if len(parts) != 2:
                logger.error(f"Invalid Bitbucket repository format: {repo_full_name}")
                return []
                
            workspace, repo_slug = parts
            
            # Get commits
            commits = []
            
            # Use the Bitbucket API client to get commits
            try:
                raw_commits = self.client.get_commits(workspace, repo_slug, limit=max_commits or 100)
            except Exception as e:
                logger.error(f"Error getting Bitbucket commits: {str(e)}")
                return []
            
            for commit in raw_commits:
                commit_data = {
                    'item_name': repo_info.get('item_name'),
                    'date': commit.get('date', ""),
                    'message': commit.get('message', "").split('\n')[0] if commit.get('message') else "",
                    'sha': commit.get('hash', ""),
                    'author': commit.get('author', {}).get('raw', "Unknown") if isinstance(commit.get('author'), dict) else "Unknown"
                }
                commits.append(commit_data)
                    
            logger.info(f"Retrieved {len(commits)} commits from Bitbucket repo {repo_full_name}")
            
            # Get tags
            tags = self.fetch_tags(workspace, repo_slug, repo_info.get('item_name'))
            
            return commits + tags
            
        except Exception as e:
            logger.error(f"Error fetching Bitbucket commits: {str(e)}")
            return []
    
    def fetch_tags(self, workspace, repo_slug, item_name):
        """Fetch tags from a Bitbucket repository."""
        try:
            logger.info(f"Fetching tags from Bitbucket: {workspace}/{repo_slug}")
            
            # Get tags from Bitbucket
            try:
                raw_tags = self.client.get_tags(workspace, repo_slug)
            except Exception as e:
                logger.error(f"Error getting Bitbucket tags: {str(e)}")
                return []
            
            tags = []
            if not raw_tags:
                return []
                
            for tag_name, tag_info in raw_tags.items():
                if not isinstance(tag_info, dict):
                    continue
                    
                tag_data = {
                    'item_name': item_name,
                    'date': "",  # Bitbucket API doesn't provide tag date directly
                    'message': f"TAG: {tag_name}",
                    'sha': tag_info.get('target', {}).get('hash', "") if isinstance(tag_info.get('target'), dict) else "",
                    'author': "Unknown"  # Bitbucket doesn't provide tagger info in the same way
                }
                tags.append(tag_data)
                
            logger.info(f"Retrieved {len(tags)} tags from Bitbucket repo {workspace}/{repo_slug}")
            return tags
            
        except Exception as e:
            logger.error(f"Error fetching Bitbucket tags: {str(e)}")
            return []