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
    parser.add_argument('--yearly', '-y', action='store_true', default=True,
                        help='Only fetch one commit per year (default: True)')
    parser.add_argument('--full', '-f', action='store_true',
                        help='Fetch all commits instead of yearly mode')
    parser.add_argument('--threshold', '-t', type=int, default=1000,
                        help='Commit count threshold for using yearly mode (default: 1000)')
    
    args = parser.parse_args()
    
    # If --full is specified, override yearly mode
    yearly_mode = not args.full if args.full else args.yearly
    
    # Create output directory if it doesn't exist
    Path(args.output).mkdir(parents=True, exist_ok=True)
    
    # Process repositories
    process_repositories(
        args.csv_file, 
        args.output,
        yearly_mode=yearly_mode,
        large_repo_threshold=args.threshold
    )
    
    logger.info("Repository commit history analyzer completed")


if __name__ == "__main__":
    main()