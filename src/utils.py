"""
Utility functions for commit history tracker.
"""

import csv
import time
import random
import logging
import math
import pandas as pd
from pathlib import Path
from tqdm import tqdm

logger = logging.getLogger(__name__)

def read_repository_csv(file_path):
    """Read repository information from CSV file."""
    try:
        df = pd.read_csv(file_path)
        
        # Clean NaN values
        for col in ['github', 'gist', 'gitlab', 'bitbucket']:
            df[col] = df[col].fillna('')
            
        # Clean GitHub URLs (extract username/repo format)
        df['github'] = df['github'].apply(lambda x: clean_github_url(x) if x else '')
        
        logger.info(f"Successfully read {len(df)} repositories from {file_path}")
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        return []

def clean_github_url(url):
    """Clean GitHub URL to extract username/repo format."""
    if not url or not isinstance(url, str):
        return ''
    
    # Handle URLs with github.com
    if 'github.com' in url:
        parts = url.split('github.com/')
        if len(parts) > 1:
            return parts[1].strip('/')
    return url

def write_commits_to_csv(commits, output_filename):
    """Write commit history to a CSV file."""
    if not commits:
        logger.warning(f"No commits to write to {output_filename}")
        return
        
    # Create output directory if it doesn't exist
    output_dir = Path(output_filename).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['item_name', 'date', 'message', 'sha', 'author', 'year']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for commit in commits:
                # Filter out any extra fields
                filtered_commit = {k: v for k, v in commit.items() if k in fieldnames}
                writer.writerow(filtered_commit)
                
        logger.info(f"Successfully wrote {len(commits)} commits to {output_filename}")
        
    except Exception as e:
        logger.error(f"Error writing to CSV file {output_filename}: {str(e)}")

def log_error_to_csv(error_filename, repo_info, platform, error_message):
    """Log repository processing errors to a CSV file."""
    # Create error log directory if it doesn't exist
    error_dir = Path(error_filename).parent
    error_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if file exists to decide if header is needed
    file_exists = Path(error_filename).exists()
    
    try:
        with open(error_filename, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['item_name', 'platform', 'repository', 'error']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            # Get the repository URL based on platform
            repo_url = repo_info.get(platform, "")
            
            # Write error row
            writer.writerow({
                'item_name': repo_info.get('item_name', "Unknown"),
                'platform': platform,
                'repository': repo_url,
                'error': error_message
            })
            
        logger.info(f"Error logged to {error_filename}")
        
    except Exception as e:
        logger.error(f"Failed to log error to CSV: {str(e)}")

class RateLimitHandler:
    """Handle rate limiting for Git APIs."""
    
    def __init__(self, initial_delay=1, max_delay=60, max_retries=5):
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
    
    def with_exponential_backoff(self, func, *args, **kwargs):
        """Execute a function with exponential backoff for rate limits."""
        retries = 0
        delay = self.initial_delay
        
        while retries <= self.max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Check if this is a rate limit error
                is_rate_limit = False
                
                if "rate limit" in str(e).lower() or "429" in str(e):
                    is_rate_limit = True
                
                if not is_rate_limit or retries >= self.max_retries:
                    # Not a rate limit error or max retries reached, re-raise
                    raise
                
                # Calculate delay with exponential backoff and jitter
                delay = min(delay * 2, self.max_delay)
                jitter = random.uniform(0, 0.1 * delay)
                sleep_time = delay + jitter
                
                logger.warning(f"Rate limit hit. Retrying in {sleep_time:.2f} seconds (attempt {retries+1}/{self.max_retries})")
                time.sleep(sleep_time)
                
                retries += 1
        
        # If we get here, we've exhausted our retries
        raise Exception(f"Failed after {self.max_retries} retries due to rate limiting")