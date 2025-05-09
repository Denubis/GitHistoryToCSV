"""
Repository processor for commit history tracking with resumable capability.
"""

import logging
import csv
import pandas as pd
from pathlib import Path
from tqdm import tqdm

from src.auth import GitAuthManager
from src.utils import read_repository_csv, write_commits_to_csv, RateLimitHandler
from src.fetchers.github import GitHubFetcher
from src.fetchers.github_yearly import GitHubYearlyFetcher
from src.fetchers.github_monthly import GitHubMonthlyFetcher
from src.fetchers.gitlab import GitLabFetcher
from src.fetchers.gitlab_monthly import GitLabMonthlyFetcher
from src.fetchers.bitbucket import BitbucketFetcher
from src.fetchers.gist import GistFetcher

logger = logging.getLogger(__name__)

def check_existing_file(file_path, item_name):
    """Check if a file exists and return processed year-month pairs if it does.
    
    Returns:
        bool: Whether file exists
        set: Set of (year, month) tuples that have already been processed
    """
    if not Path(file_path).exists():
        return False, set()
        
    try:
        df = pd.read_csv(file_path)
        # Get the year-month pairs that have already been processed
        processed_pairs = set()
        
        # Check if needed columns exist
        if 'year' in df.columns and 'month' in df.columns:
            for _, row in df.iterrows():
                # Only add if both year and month exist
                if pd.notna(row.get('year')) and pd.notna(row.get('month')):
                    processed_pairs.add((str(row['year']), str(row['month']).zfill(2)))
            
            logger.info(f"Found existing file {file_path} with {len(processed_pairs)} processed year-month pairs")
        else:
            logger.info(f"Found existing file {file_path} but no year-month columns")
            
        return True, processed_pairs
    except Exception as e:
        logger.error(f"Error reading existing file {file_path}: {str(e)}")
        return True, set()  # File exists but couldn't be read properly

def process_repositories(csv_file, output_dir="output", yearly_mode=True, monthly_mode=False, large_repo_threshold=1000, resume=True):
    """Process repositories from CSV and output commit histories.
    
    Args:
        csv_file: Path to CSV file with repository information
        output_dir: Directory to write output files
        yearly_mode: If True, only fetch one commit per year (default: True)
        monthly_mode: If True, fetch one commit per month (default: False)
        large_repo_threshold: Repositories with more commits than this will use yearly_mode
        resume: If True, skip repositories with existing output files
    """
    # Read repository information
    repos = read_repository_csv(csv_file)
    if not repos:
        logger.error("No repositories found or error reading CSV file")
        return
        
    # Initialize authentication
    auth_manager = GitAuthManager()
    
    # Initialize clients
    github_client = auth_manager.get_github_client()
    gitlab_client = auth_manager.get_gitlab_client()
    bitbucket_client = auth_manager.get_bitbucket_client()
    
    # Initialize fetchers
    github_fetcher = GitHubFetcher(github_client) if github_client else None
    github_yearly_fetcher = GitHubYearlyFetcher(github_client) if github_client else None
    github_monthly_fetcher = GitHubMonthlyFetcher(github_client) if github_client else None
    gitlab_fetcher = GitLabFetcher(gitlab_client) if gitlab_client else None
    gitlab_monthly_fetcher = GitLabMonthlyFetcher(gitlab_client) if gitlab_client else None
    bitbucket_fetcher = BitbucketFetcher(bitbucket_client) if bitbucket_client else None
    gist_fetcher = GistFetcher(github_client) if github_client else None
    
    # Initialize rate limit handler
    rate_handler = RateLimitHandler()
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    # Create monthly output directory if it doesn't exist
    Path(f"{output_dir}/monthly").mkdir(parents=True, exist_ok=True)
    # Create error directory if it doesn't exist - single location for all error logs
    Path(f"{output_dir}/errors").mkdir(parents=True, exist_ok=True)
    
    # Set up progress bar
    progress_bar = tqdm(repos, desc="Processing repositories", unit="repo")
    
    # Process each repository
    for repo in progress_bar:
        item_name = repo.get('item_name')
        if not item_name:
            logger.warning("Repository missing item_name, skipping")
            continue
            
        progress_bar.set_description(f"Processing {item_name}")
        logger.info(f"Processing repository: {item_name}")
        
        # GitHub repository
        github_repo = repo.get('github', '')
        if github_repo and github_client:
            try:
                # First check if this is a large repository that should use the yearly mode
                use_yearly_mode = yearly_mode
                use_monthly_mode = monthly_mode
                
                if not use_yearly_mode and not use_monthly_mode and github_repo:
                    try:
                        # Get repo information to check commit count
                        github_repo_obj = github_client.get_repo(github_repo)
                        if github_repo_obj:
                            commit_count = github_repo_obj.get_commits().totalCount
                            if commit_count > large_repo_threshold:
                                logger.info(f"Large repository detected ({commit_count} commits). Using yearly mode.")
                                use_yearly_mode = True
                    except Exception as e:
                        logger.warning(f"Could not determine repository size: {str(e)}")
                
                if use_yearly_mode and github_yearly_fetcher:
                    # Use the REST API yearly fetcher
                    output_file = Path(output_dir) / f"{item_name}_yearly.csv"
                    
                    # Check if file already exists and we're in resume mode
                    if resume and Path(output_file).exists():
                        logger.info(f"Skipping GitHub yearly for {item_name} - output file already exists")
                        continue
                    
                    github_commits = rate_handler.with_exponential_backoff(
                        github_yearly_fetcher.fetch_commits, repo)
                    
                    if github_commits:
                        write_commits_to_csv(github_commits, output_file)
                
                elif use_monthly_mode and github_monthly_fetcher:
                    # Use the REST API monthly fetcher
                    monthly_output_dir = Path(output_dir) / "monthly"
                    monthly_output_dir.mkdir(parents=True, exist_ok=True)
                    output_file = monthly_output_dir / f"{item_name}_github_monthly.csv"
                    
                    # Check if file already exists and we're in resume mode
                    file_exists, processed_pairs = check_existing_file(output_file, item_name)
                    if resume and file_exists and processed_pairs:
                        logger.info(f"Skipping GitHub monthly for {item_name} - already processed {len(processed_pairs)} year-month pairs")
                        continue
                    
                    github_commits = rate_handler.with_exponential_backoff(
                        github_monthly_fetcher.fetch_commits, repo)
                    
                    if github_commits:
                        write_commits_to_csv(github_commits, output_file)
                
                else:
                    # Use the standard fetcher for smaller repositories
                    output_file = Path(output_dir) / f"{item_name}.csv"
                    
                    # Check if file already exists and we're in resume mode
                    if resume and Path(output_file).exists():
                        logger.info(f"Skipping GitHub for {item_name} - output file already exists")
                        continue
                    
                    github_commits = rate_handler.with_exponential_backoff(
                        github_fetcher.fetch_commits, repo)
                    
                    if github_commits:
                        write_commits_to_csv(github_commits, output_file)
            
            except Exception as e:
                logger.error(f"Error processing GitHub repository {github_repo}: {str(e)}")
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Error processing repository: {str(e)}"
                log_error_to_csv(f'{output_dir}/errors/github_errors.csv', repo, 'github', error_msg)
        
        # GitHub Gist repository
        gist_repo = repo.get('gist', '')
        if gist_repo and gist_fetcher:
            try:
                output_file = Path(output_dir) / f"{item_name}_gist.csv"
                
                # Check if file already exists and we're in resume mode
                if resume and Path(output_file).exists():
                    logger.info(f"Skipping Gist for {item_name} - output file already exists")
                    continue
                
                gist_commits = rate_handler.with_exponential_backoff(
                    gist_fetcher.fetch_commits, repo)
                    
                if gist_commits:
                    write_commits_to_csv(gist_commits, output_file)
            except Exception as e:
                logger.error(f"Error processing Gist {gist_repo}: {str(e)}")
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Error processing Gist: {str(e)}"
                log_error_to_csv(f'{output_dir}/errors/gist_errors.csv', repo, 'gist', error_msg)
        
        # GitLab repository
        gitlab_repo = repo.get('gitlab', '')
        if gitlab_repo and gitlab_client:
            try:
                if monthly_mode and gitlab_monthly_fetcher:
                    # Use monthly GitLab fetcher
                    monthly_output_dir = Path(output_dir) / "monthly"
                    monthly_output_dir.mkdir(parents=True, exist_ok=True)
                    output_file = monthly_output_dir / f"{item_name}_gitlab_monthly.csv"
                    
                    # Check if file already exists and we're in resume mode
                    file_exists, processed_pairs = check_existing_file(output_file, item_name)
                    if resume and file_exists and processed_pairs:
                        logger.info(f"Skipping GitLab monthly for {item_name} - already processed {len(processed_pairs)} year-month pairs")
                        continue
                    
                    gitlab_commits = rate_handler.with_exponential_backoff(
                        gitlab_monthly_fetcher.fetch_commits, repo)
                    
                    if gitlab_commits:
                        write_commits_to_csv(gitlab_commits, output_file)
                        logger.info(f"Successfully saved GitLab monthly commits to {output_file}")
                else:
                    # Use standard GitLab fetcher
                    output_file = Path(output_dir) / f"{item_name}_gitlab.csv"
                    
                    # Check if file already exists and we're in resume mode
                    if resume and Path(output_file).exists():
                        logger.info(f"Skipping GitLab for {item_name} - output file already exists")
                        continue
                    
                    gitlab_commits = rate_handler.with_exponential_backoff(
                        gitlab_fetcher.fetch_commits, repo)
                        
                    if gitlab_commits:
                        write_commits_to_csv(gitlab_commits, output_file)
            except Exception as e:
                logger.error(f"Error processing GitLab repository {gitlab_repo}: {str(e)}")
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Error processing repository: {str(e)}"
                log_error_to_csv(f'{output_dir}/errors/gitlab_errors.csv', repo, 'gitlab', error_msg)
                
        # Bitbucket repository
        bitbucket_repo = repo.get('bitbucket', '')
        if bitbucket_repo and bitbucket_fetcher:
            try:
                output_file = Path(output_dir) / f"{item_name}_bitbucket.csv"
                
                # Check if file already exists and we're in resume mode
                if resume and Path(output_file).exists():
                    logger.info(f"Skipping Bitbucket for {item_name} - output file already exists")
                    continue
                
                bitbucket_commits = rate_handler.with_exponential_backoff(
                    bitbucket_fetcher.fetch_commits, repo)
                    
                if bitbucket_commits:
                    write_commits_to_csv(bitbucket_commits, output_file)
            except Exception as e:
                logger.error(f"Error processing Bitbucket repository {bitbucket_repo}: {str(e)}")
                # Log error to CSV
                from src.utils import log_error_to_csv
                error_msg = f"Error processing repository: {str(e)}"
                log_error_to_csv(f'{output_dir}/errors/bitbucket_errors.csv', repo, 'bitbucket', error_msg)
                
    logger.info("Finished processing all repositories")