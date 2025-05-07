"""
GitHub monthly commit fetcher.
Retrieves one commit per month using the REST API.
"""

import logging
import requests
from datetime import datetime, timedelta
import calendar

logger = logging.getLogger(__name__)

class GitHubMonthlyFetcher:
    """Fetch one commit per month from GitHub repositories."""
    
    def __init__(self, client):
        self.client = client
        self.github_token = None
        
        # Extract token if client is initialized
        if client:
            try:
                self.github_token = client._Github__requester._Requester__auth.token
            except (AttributeError, TypeError):
                logger.warning("Could not extract GitHub token from client")
    
    def fetch_commits(self, repo_info, start_date=None, end_date=None):
        """Fetch one commit per month for the repository's history."""
        try:
            # Parse repository information
            repo_full_name = repo_info.get('github', '')
            if not repo_full_name:
                logger.warning(f"No GitHub repository for {repo_info.get('item_name')}")
                return []
                
            logger.info(f"Fetching one commit per month from GitHub: {repo_full_name}")
            
            # Set up the headers for API requests
            headers = {"Authorization": f"Bearer {self.github_token}"}
            
            # Check for GitHub redirects
            try:
                check_url = f"https://api.github.com/repos/{repo_full_name}"
                response = requests.get(check_url, headers=headers, allow_redirects=False)
                
                if response.status_code in (301, 302, 307, 308):
                    redirect_location = response.headers.get('Location')
                    if redirect_location and '/repos/' in redirect_location:
                        new_repo_full_name = redirect_location.split('/repos/')[1]
                        logger.info(f"GitHub repository {repo_full_name} redirects to {new_repo_full_name}")
                        repo_full_name = new_repo_full_name
                elif response.status_code == 404:
                    # Log error to CSV
                    from src.utils import log_error_to_csv
                    error_msg = f"Repository not found or not accessible (404 response)"
                    log_error_to_csv('output/errors/error_log.csv', repo_info, 'github', error_msg)
                    return []
                elif response.status_code != 200:
                    # Log error to CSV
                    from src.utils import log_error_to_csv
                    error_msg = f"Unexpected response when checking repository: {response.status_code}"
                    log_error_to_csv('output/errors/error_log.csv', repo_info, 'github', error_msg)
                    return []
            except Exception as e:
                logger.warning(f"Failed to check GitHub redirects: {str(e)}")
            
            # Get the commit date range
            first_commit_date, latest_commit_date = self.get_commit_date_range(repo_full_name)
            
            # Determine date range
            if not start_date:
                start_date = first_commit_date
            else:
                start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
                # Make sure we don't go earlier than the first commit
                if start_date < first_commit_date:
                    start_date = first_commit_date
                    
            if not end_date:
                end_date = latest_commit_date
            else:
                end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
                # Make sure we don't go later than the latest commit
                if end_date > latest_commit_date:
                    end_date = latest_commit_date
            
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
                
                # Get commits for this month
                commits_url = f"https://api.github.com/repos/{repo_full_name}/commits"
                params = {
                    'since': month_start,
                    'until': month_end,
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
            logger.error(f"Error fetching GitHub monthly commits: {str(e)}")
            # Log error to CSV
            from src.utils import log_error_to_csv
            error_msg = f"Error fetching GitHub monthly commits: {str(e)}"
            log_error_to_csv('output/errors/error_log.csv', repo_info, 'github', error_msg)
            return []
            
    def get_commit_date_range(self, repo_full_name):
        """Get the date range of commits in a repository."""
        try:
            logger.info(f"Getting commit date range for GitHub repository: {repo_full_name}")
            
            # Set up the headers for API requests
            headers = {"Authorization": f"Bearer {self.github_token}"}
            
            # Get repository creation date from REST API
            repo_url = f"https://api.github.com/repos/{repo_full_name}"
            response = requests.get(repo_url, headers=headers)
            response.raise_for_status()
            
            repo_data = response.json()
            created_at = repo_data.get("created_at")
            created_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
            
            # Get the latest commit (newest)
            latest_commit_url = f"https://api.github.com/repos/{repo_full_name}/commits?per_page=1"
            response = requests.get(latest_commit_url, headers=headers)
            response.raise_for_status()
            
            latest_commits = response.json()
            if not latest_commits:
                logger.warning(f"No commits found for repository {repo_full_name}")
                return created_date, datetime.now()
                
            latest_commit_date = datetime.strptime(
                latest_commits[0]["commit"]["author"]["date"], 
                "%Y-%m-%dT%H:%M:%SZ"
            )
            
            # Get the first commit (oldest) - use repository creation date as a boundary
            # This is more efficient than fetching all commits and finding the oldest one
            first_commit_url = f"https://api.github.com/repos/{repo_full_name}/commits?until={created_at}&per_page=1"
            response = requests.get(first_commit_url, headers=headers)
            response.raise_for_status()
            
            first_commits = response.json()
            if not first_commits:
                logger.warning(f"Could not determine first commit date for {repo_full_name}")
                first_commit_date = created_date
            else:
                first_commit_date = datetime.strptime(
                    first_commits[0]["commit"]["author"]["date"], 
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            
            logger.info(f"Repository {repo_full_name} commit range: {first_commit_date} to {latest_commit_date}")
            return first_commit_date, latest_commit_date
            
        except Exception as e:
            logger.error(f"Error getting commit date range: {str(e)}")
            # If we can't determine the date range, return a default range
            now = datetime.now()
            return created_date if 'created_date' in locals() else now.replace(year=now.year - 1), now        