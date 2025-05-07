"""
GitLab commit fetcher.
"""

import logging
import requests

logger = logging.getLogger(__name__)

class GitLabFetcher:
    """Fetch commit histories from GitLab repositories."""
    
    def __init__(self, client):
        self.client = client
        self.gitlab_token = None
        
        # Extract token if client is initialized
        if client:
            try:
                self.gitlab_token = client.private_token
            except (AttributeError, TypeError):
                logger.warning("Could not extract GitLab token from client")
    
    def get_project_with_redirect(self, repo_full_name):
        """Get GitLab project with redirect handling."""
        try:
            # First try directly with python-gitlab client
            try:
                project = self.client.projects.get(repo_full_name)
                logger.info(f"Successfully retrieved GitLab project: {repo_full_name}")
                return project
            except Exception as e:
                logger.warning(f"Failed to get project via python-gitlab client: {str(e)}")
            
            # If direct access fails, check for redirects
            if not repo_full_name.startswith('http'):
                check_url = f"https://gitlab.com/{repo_full_name}"
            else:
                check_url = repo_full_name
                
            try:
                # Make a request to check for redirects
                response = requests.head(check_url, allow_redirects=False)
                
                # If we get a redirect, follow it to find the real repository location
                if response.status_code in (301, 302, 307, 308):
                    redirect_url = response.headers.get('Location')
                    if redirect_url:
                        logger.info(f"GitLab repository {repo_full_name} redirects to {redirect_url}")
                        
                        # Extract the project path from the redirect URL
                        if "gitlab.com/" in redirect_url:
                            # Extract the part after gitlab.com/
                            gitlab_path = redirect_url.split("gitlab.com/")[1]
                            # Remove any trailing parts (like /-/tree/main)
                            if "/-/" in gitlab_path:
                                gitlab_path = gitlab_path.split("/-/")[0]
                            
                            # Try to get the project with the new path
                            try:
                                project = self.client.projects.get(gitlab_path)
                                logger.info(f"Successfully retrieved GitLab project after redirect: {gitlab_path}")
                                return project
                            except Exception as e:
                                logger.error(f"Failed to get project after redirect: {str(e)}")
            except Exception as e:
                logger.warning(f"Failed to check GitLab redirects: {str(e)}")
            
            # If we got here, we couldn't find the project
            logger.error(f"Project not found or not accessible: {repo_full_name}")
            return None
            
        except Exception as e:
            logger.error(f"Error handling GitLab project redirect: {str(e)}")
            return None
    
    def fetch_commits(self, repo_info, max_commits=None):
        """Fetch commits from a GitLab repository."""
        try:
            # Parse repository information
            repo_full_name = repo_info.get('gitlab', '')
            if not repo_full_name:
                logger.warning(f"No GitLab repository for {repo_info.get('item_name')}")
                return []
                
            logger.info(f"Fetching commits from GitLab: {repo_full_name}")
            
            # Get project with redirect handling
            project = self.get_project_with_redirect(repo_full_name)
            
            if not project:
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Project not found or not accessible (could be moved, renamed, or private)"
                log_error_to_csv('output/errors/error_log.csv', repo_info, 'gitlab', error_msg)
                return []
            
            # Get commits
            commits = []
            try:
                for commit in project.commits.list(all=True):
                    commit_data = {
                        'item_name': repo_info.get('item_name'),
                        'date': commit.created_at if hasattr(commit, 'created_at') else "",
                        'message': commit.message.split('\n')[0] if hasattr(commit, 'message') and commit.message else "",
                        'sha': commit.id if hasattr(commit, 'id') else "",
                        'author': commit.author_name if hasattr(commit, 'author_name') else "Unknown",
                        'year': commit.created_at.split('-')[0] if hasattr(commit, 'created_at') and isinstance(commit.created_at, str) else ""
                    }
                    commits.append(commit_data)
                    
                    # Break if we've reached the maximum number of commits
                    if max_commits and len(commits) >= max_commits:
                        break
                        
                logger.info(f"Retrieved {len(commits)} commits from GitLab repo {project.name}")
            except Exception as e:
                logger.error(f"Error fetching commits: {str(e)}")
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Error fetching commits: {str(e)}"
                log_error_to_csv('output/errors/error_log.csv', repo_info, 'gitlab', error_msg)
            
            # Get tags
            tags = []
            try:
                tags = self.fetch_tags(project, repo_info.get('item_name'))
            except Exception as e:
                logger.error(f"Error fetching tags: {str(e)}")
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Error fetching tags: {str(e)}"
                log_error_to_csv('output/errors/error_log.csv', repo_info, 'gitlab', error_msg)
            
            return commits + tags
            
        except Exception as e:
            logger.error(f"Error fetching GitLab commits: {str(e)}")
            # Log error to CSV
            from src.utils import log_error_to_csv
            error_msg = f"Error fetching GitLab commits: {str(e)}"
            log_error_to_csv('output/errors/error_log.csv', repo_info, 'gitlab', error_msg)
            return []
    
    def fetch_tags(self, project, item_name):
        """Fetch tags from a GitLab repository."""
        try:
            logger.info(f"Fetching tags from GitLab: {project.name}")
            
            tags = []
            for tag in project.tags.list(all=True):
                tag_data = {
                    'item_name': item_name,
                    'date': tag.commit.get('created_at', "") if hasattr(tag, 'commit') and isinstance(tag.commit, dict) else "",
                    'message': f"TAG: {tag.name}" if hasattr(tag, 'name') else "TAG: Unknown",
                    'sha': tag.commit.get('id', "") if hasattr(tag, 'commit') and isinstance(tag.commit, dict) else "",
                    'author': tag.commit.get('author_name', "Unknown") if hasattr(tag, 'commit') and isinstance(tag.commit, dict) else "Unknown",
                    'year': tag.commit.get('created_at', "").split('-')[0] if hasattr(tag, 'commit') and isinstance(tag.commit, dict) and isinstance(tag.commit.get('created_at', ""), str) else ""
                }
                tags.append(tag_data)
                
            logger.info(f"Retrieved {len(tags)} tags from GitLab repo {project.name}")
            return tags
            
        except Exception as e:
            logger.error(f"Error fetching GitLab tags: {str(e)}")
            return []