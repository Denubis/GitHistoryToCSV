"""
GitLab monthly commit fetcher.
Retrieves one commit per month using the REST API.
"""

import logging
import requests
import calendar
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class GitLabMonthlyFetcher:
    """Fetch one commit per month from GitLab repositories."""
    
    def __init__(self, client):
        self.client = client
        self.gitlab_token = None
        
        # Extract token if client is initialized
        if client:
            try:
                self.gitlab_token = client.private_token
            except (AttributeError, TypeError):
                logger.warning("Could not extract GitLab token from client")
    
    def fetch_commits(self, repo_info, start_date=None, end_date=None):
        """Fetch one commit per month for the repository's history."""
        try:
            # Parse repository information
            repo_full_name = repo_info.get('gitlab', '')
            if not repo_full_name:
                logger.warning(f"No GitLab repository for {repo_info.get('item_name')}")
                return []
                
            logger.info(f"Fetching one commit per month from GitLab: {repo_full_name}")
            
            # Check for redirects first
            original_url = repo_full_name
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
                            repo_full_name = gitlab_path
                            logger.info(f"Using redirected GitLab repository path: {repo_full_name}")
                elif response.status_code == 404:
                    # Log error to CSV
                    from src.utils import log_error_to_csv
                    error_msg = f"Repository not found (404)"
                    log_error_to_csv('output/errors/gitlab_errors.csv', repo_info, 
                                    'gitlab', error_msg, status_code=404)
                    return []
            except Exception as e:
                logger.warning(f"Failed to check GitLab redirects: {str(e)}")
            
            # Get project
            project = self.client.projects.get(repo_full_name)
            
            # Get repository creation date
            created_at = project.created_at
            if isinstance(created_at, str):
                created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            else:
                created_date = created_at
            
            # Ensure created_date is timezone-aware
            if created_date.tzinfo is None:
                created_date = created_date.replace(tzinfo=timezone.utc)
            
            # Determine date range
            if not start_date:
                start_date = created_date
            else:
                start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
                # Make start_date timezone-aware
                start_date = start_date.replace(tzinfo=timezone.utc)
                
            if not end_date:
                # Make sure end_date is timezone-aware to avoid comparison issues
                end_date = datetime.now(timezone.utc)
            else:
                end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
                # Make end_date timezone-aware
                end_date = end_date.replace(tzinfo=timezone.utc)
            
            commits = []
            
            # Create a list of year-month pairs
            current_date = start_date.replace(day=1)  # Start at first day of month
            end_date_month = end_date.replace(day=1)  # First day of end month
            
            # For each month, get the first commit
            while current_date <= end_date_month:
                year = current_date.year
                month = current_date.month
                
                # Date range for the month
                month_start = current_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                
                # Calculate last day of month
                _, last_day = calendar.monthrange(year, month)
                month_end = current_date.replace(day=last_day, hour=23, minute=59, second=59).strftime("%Y-%m-%dT%H:%M:%SZ")
                
                try:
                    # Get commits for this month using GitLab API - explicitly state get_all=False to suppress warning
                    monthly_commits = project.commits.list(since=month_start, until=month_end, per_page=1, get_all=False)
                    
                    if monthly_commits and len(monthly_commits) > 0:
                        commit = monthly_commits[0]
                        commit_info = {
                            'item_name': repo_info.get('item_name'),
                            'date': commit.created_at,
                            'message': commit.message.splitlines()[0] if commit.message else "",  # First line only
                            'sha': commit.id,
                            'author': commit.author_name,
                            'year': str(year),
                            'month': str(month).zfill(2)  # Zero-padded month
                        }
                        commits.append(commit_info)
                        logger.info(f"Found commit for {year}-{month:02d} on {commit_info['date']}")
                    else:
                        logger.info(f"No commits found for {year}-{month:02d}")
                        
                except Exception as e:
                    logger.error(f"Error retrieving commits for {year}-{month:02d}: {str(e)}")
                
                # Move to next month
                if month == 12:
                    current_date = current_date.replace(year=year+1, month=1)
                else:
                    current_date = current_date.replace(month=month+1)
            
            return commits
            
        except Exception as e:
            logger.error(f"Error fetching GitLab monthly commits: {str(e)}")
            # Log error to CSV
            from src.utils import log_error_to_csv
            error_msg = f"Error fetching GitLab monthly commits: {str(e)}"
            log_error_to_csv('output/errors/gitlab_errors.csv', repo_info, 'gitlab', error_msg)
            return []