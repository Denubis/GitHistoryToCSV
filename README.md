# Git Commit History Tracker

A Python tool to fetch commit histories from various Git platforms (GitHub, GitLab, and Bitbucket) for archaeological software research.

## Features

- Extract commit histories from GitHub, GitHub Gist, GitLab, and Bitbucket repositories
- Special handling for releases and tags
- Rate limit handling with exponential backoff
- Secure API authentication using environment variables
- CSV output for further analysis
- Comprehensive logging

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/git-commit-tracker.git
   cd git-commit-tracker
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up your `.env` file with API keys:
   ```
   GITHUB_TOKEN=your_github_personal_access_token
   GITLAB_TOKEN=your_gitlab_token
   BITBUCKET_USERNAME=your_bitbucket_username
   BITBUCKET_APP_PASSWORD=your_bitbucket_app_password
   ```

## How to Use

1. Create a CSV file with repository information. The CSV should have the following headers:
   ```
   item_name,description,github,gist,gitlab,bitbucket,launchpad,twitter,youtube,blogpost,cran,pypi,codeberg,website,publication,DOI,author1_name,author2_name,author3_name,author4_name,author5_name,author6_name,category,platform,tag1,tag2,tag3,tag4,tag5,notes,internetarchive
   ```

   Example content:
   ```
   item_name,description,github,gist,gitlab,bitbucket
   repo1,Description 1,username/repo1,,,
   repo2,Description 2,,username/repo2,,
   repo3,Description 3,,,username/repo3,
   repo4,Description 4,,,,workspace/repo4
   ```

2. Run the script:
   ```bash
   python main.py path/to/your/repositories.csv
   ```

   If no CSV file is specified, it will default to `repositories.csv` in the current directory.

3. Check the output in the `output` directory. Each repository will have its commit history saved in a CSV file named after the `item_name`.

## Output Format

The output CSV files contain the following columns:
- `item_name`: The name of the repository/item
- `date`: The date of the commit
- `message`: The first line of the commit message
- `sha`: The commit hash
- `author`: The author of the commit

Releases and tags are included in the output with special prefixes in the `message` field (e.g., "RELEASE: v1.0.0" or "TAG: v1.0.0").

## Troubleshooting

Check the `commit_tracker.log` file for detailed logging information.

Common issues:
- **API Rate Limiting**: The script handles rate limiting with exponential backoff, but you may still encounter limits if processing many repositories
- **Authentication Issues**: Ensure your API tokens are correct in the `.env` file
- **Repository Format Issues**: For Bitbucket repositories, ensure they follow the `workspace/repo-slug` format

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.