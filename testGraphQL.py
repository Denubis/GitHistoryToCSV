#!/usr/bin/env python3
"""
A minimal script that gets one commit per year from a GitHub repository
and outputs it to a CSV file.
"""

import os
import requests
import csv
from datetime import datetime

# Set your GitHub token here or in environment variable
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")

def get_commits_by_year(owner, repo, output_file):
    """Get one commit per year and write to CSV."""
    # Get repository creation date
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    
    repo_url = f"https://api.github.com/repos/{owner}/{repo}"
    response = requests.get(repo_url, headers=headers)
    response.raise_for_status()
    
    repo_data = response.json()
    created_at = repo_data.get("created_at")
    created_year = int(created_at.split("-")[0])
    
    # Current year
    current_year = datetime.now().year
    
    # Prepare CSV file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Year', 'Date', 'SHA', 'Author', 'Message'])
        
        print(f"Repository: {owner}/{repo}")
        print(f"Created: {created_at}")
        print(f"Finding one commit per year from {created_year} to {current_year}...")
        
        # For each year, get the first commit
        for year in range(created_year, current_year + 1):
            # Date range for the year
            start_date = f"{year}-01-01T00:00:00Z"
            end_date = f"{year}-12-31T23:59:59Z"
            
            # Get commits for this year
            commits_url = f"https://api.github.com/repos/{owner}/{repo}/commits"
            params = {
                'since': start_date,
                'until': end_date,
                'per_page': 1,  # We only need the first commit
            }
            
            try:
                response = requests.get(commits_url, headers=headers, params=params)
                response.raise_for_status()
                
                commits = response.json()
                if commits:
                    commit = commits[0]
                    sha = commit["sha"]
                    date = commit["commit"]["author"]["date"]
                    author = commit["commit"]["author"]["name"]
                    message = commit["commit"]["message"].splitlines()[0]  # First line only
                    
                    writer.writerow([year, date, sha, author, message])
                    print(f"{year}: Found commit from {date} by {author}")
                else:
                    writer.writerow([year, "No commits", "", "", ""])
                    print(f"{year}: No commits found")
                    
            except Exception as e:
                print(f"Error processing year {year}: {str(e)}")
                writer.writerow([year, f"Error: {str(e)}", "", "", ""])

if __name__ == "__main__":
    # You can modify these values or pass them as arguments
    owner = "SPAAM-community"
    repo = "AncientMetagenomeDir"
    output_file = f"{repo}_yearly_commits.csv"
    
    if not GITHUB_TOKEN:
        print("Please set your GitHub token in the GITHUB_TOKEN environment variable")
        exit(1)
        
    get_commits_by_year(owner, repo, output_file)
    print(f"Results saved to {output_file}")