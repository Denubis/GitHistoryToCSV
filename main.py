#!/usr/bin/env python3
"""
Repository Commit History Analyzer

Main entry point for the application.
"""

import logging
import sys
import argparse
from pathlib import Path
from src.processor import process_repositories


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("commit_tracker.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def main():
    """Main entry point of the script."""
    logger = setup_logging()
    logger.info("Starting repository commit history analyzer")
    
    # Set up command-line arguments
    parser = argparse.ArgumentParser(description='Analyze commit history of repositories.')
    parser.add_argument('csv_file', nargs='?', default="repositories.csv", 
                        help='CSV file containing repository information (default: repositories.csv)')
    parser.add_argument('--output', '-o', default="output", 
                        help='Output directory for CSV files (default: output)')
    parser.add_argument('--yearly', '-y', action='store_true', default=False,
                        help='Only fetch one commit per year (default: True)')
    parser.add_argument('--monthly', '-m', action='store_true', default=True,
                        help='Fetch one commit per month instead of yearly')
    parser.add_argument('--full', '-f', action='store_true',
                        help='Fetch all commits instead of yearly/monthly mode')
    parser.add_argument('--threshold', '-t', type=int, default=1000,
                        help='Commit count threshold for using yearly mode (default: 1000)')
    parser.add_argument('--resume', '-r', action='store_true', default=True,
                        help='Resume processing by skipping repositories with existing output files (default: True)')
    parser.add_argument('--no-resume', action='store_false', dest='resume',
                        help='Process all repositories even if output files exist')
    
    args = parser.parse_args()
    
    # If --full is specified, override yearly and monthly mode
    yearly_mode = False if args.full else args.yearly
    monthly_mode = False if args.full else args.monthly
    
    # If both yearly and monthly are True, prioritize monthly
    if yearly_mode and monthly_mode:
        yearly_mode = False
    
    # Create output directory if it doesn't exist
    Path(args.output).mkdir(parents=True, exist_ok=True)
    
    # Process repositories
    process_repositories(
        args.csv_file, 
        args.output,
        yearly_mode=yearly_mode,
        monthly_mode=monthly_mode,
        large_repo_threshold=args.threshold,
        resume=args.resume
    )
    
    logger.info("Repository commit history analyzer completed")


if __name__ == "__main__":
    main()