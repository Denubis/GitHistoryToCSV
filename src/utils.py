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
            if col in df.columns:
                df[col] = df[col].fillna('')
            else:
                df[col] = ''
            
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
            # Remove trailing slashes and other URL parts
            repo_path = parts[1].strip('/')
            
            # Handle potential fragments or query parameters
            if '#' in repo_path:
                repo_path = repo_path.split('#')[0]
            if '?' in repo_path:
                repo_path = repo_path.split('?')[0]
                
            # Remove trailing .git if present
            if repo_path.endswith('.git'):
                repo_path = repo_path[:-4]
                
            return repo_path
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
        # Determine all possible field names from the commits
        # Start with required fields plus common optional fields
        all_fields = set()
        
        # Add any fields present in the commits
        for commit in commits:
            all_fields.update(commit.keys())
            
        # Ensure primary fields come first in a specific order
        primary_fields = ['item_name', 'date']
        # Remove primary fields from all_fields if they exist
        remaining_fields = sorted(list(all_fields - set(primary_fields)))
        # Combine to create the final ordered fieldnames
        fieldnames = primary_fields + remaining_fields
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for commit in commits:
                # Ensure all fields are present with empty strings as defaults
                row = {field: commit.get(field, '') for field in fieldnames}
                writer.writerow(row)
                
        logger.info(f"Successfully wrote {len(commits)} commits to {output_filename}")
        
    except Exception as e:
        logger.error(f"Error writing to CSV file {output_filename}: {str(e)}")

def log_error_to_csv(error_filename, repo_info, platform, error_message, status_code=None):
    """Log repository processing errors to a CSV file."""
    # Create error log directory if it doesn't exist
    error_dir = Path(error_filename).parent
    error_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if file exists to decide if header is needed
    file_exists = Path(error_filename).exists()
    
    try:
        with open(error_filename, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['item_name', 'platform', 'repository', 'error', 'timestamp', 
                         'error_type', 'status_code', 'redirected']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            # Get the repository URL based on platform
            repo_url = repo_info.get(platform, "")
            
            # Determine error type and redirected status
            error_type = "Unknown"
            redirected = "No"
            
            if status_code:
                if status_code in (301, 302, 307, 308):
                    error_type = "Redirect"
                    redirected = "Yes"
                elif status_code == 404:
                    error_type = "Not Found"
                elif status_code == 429:
                    error_type = "Rate Limit"
                else:
                    error_type = f"HTTP {status_code}"
            elif "redirect" in error_message.lower() or "301" in error_message or "302" in error_message:
                error_type = "Redirect"
                redirected = "Yes"
            elif "404" in error_message or "not found" in error_message.lower():
                error_type = "Not Found"
            elif "rate limit" in error_message.lower():
                error_type = "Rate Limit"
            
            # Write error row
            writer.writerow({
                'item_name': repo_info.get('item_name', "Unknown"),
                'platform': platform,
                'repository': repo_url,
                'error': error_message,
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'error_type': error_type,
                'status_code': status_code if status_code else "",
                'redirected': redirected
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
                
                if any(rate_term in str(e).lower() for rate_term in 
                      ["rate limit", "rate_limit", "ratelimit", "429", "too many requests"]):
                    is_rate_limit = True
                
                if not is_rate_limit or retries >= self.max_retries:
                    # Not a rate limit error or max retries reached, re-raise
                    raise
                
                # Check for Retry-After header in the exception
                retry_after = None
                if hasattr(e, 'headers') and e.headers:
                    retry_after = e.headers.get('Retry-After') or e.headers.get('retry-after')
                
                # Calculate delay with exponential backoff and jitter
                if retry_after and retry_after.isdigit():
                    delay = int(retry_after)
                else:
                    delay = min(delay * 2, self.max_delay)
                
                jitter = random.uniform(0, 0.1 * delay)
                sleep_time = delay + jitter
                
                logger.warning(f"Rate limit hit. Retrying in {sleep_time:.2f} seconds (attempt {retries+1}/{self.max_retries})")
                time.sleep(sleep_time)
                
                retries += 1
        
        # If we get here, we've exhausted our retries
        raise Exception(f"Failed after {self.max_retries} retries due to rate limiting")