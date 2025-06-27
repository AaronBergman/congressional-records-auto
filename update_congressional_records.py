#!/usr/bin/env python3
"""
Script to update Congressional Record downloads by fetching only new articles.
Stops processing once it encounters issues that have already been downloaded.
Downloads in reverse chronological order (newest to oldest).
"""

import requests
import os
import json
import time
from datetime import datetime, timezone  # Added timezone import
from pathlib import Path
from tqdm import tqdm
import sys
from itertools import cycle

class CongressionalRecordUpdater:
    def __init__(self, api_keys, timeout=5):
        self.api_keys = api_keys
        self.api_key_cycle = cycle(api_keys)
        self.current_api_key = next(self.api_key_cycle)
        self.timeout = timeout
        self.session = requests.Session()
        self.request_count = 0
        self.last_request_time = 0
        self.consecutive_existing_issues = 0
        self.stop_threshold = 3  # Stop after finding 3 consecutive fully-downloaded issues
        
    def get_next_api_key(self):
        """Rotate to the next API key."""
        self.current_api_key = next(self.api_key_cycle)
        return self.current_api_key
        
    def handle_429_error(self, attempt=1):
        """Handle 429 rate limit errors with exponential backoff."""
        # Try rotating to next API key first
        if len(self.api_keys) > 1:
            print(f"\n429 Rate limit hit. Rotating to next API key...")
            self.get_next_api_key()
            return
            
        # If only one key or all keys are rate limited, use backoff
        if attempt <= 5:
            # Exponential backoff for first 5 attempts
            wait_time = min(2 ** attempt, 60)
            print(f"\n429 Rate limit hit. Waiting {wait_time} seconds (attempt {attempt})...")
        else:
            # After 5 attempts, wait 1 minute between retries
            wait_time = 60
            print(f"\n429 Rate limit hit. Waiting 60 seconds (attempt {attempt})...")
        
        time.sleep(wait_time)
        
    def make_request(self, url, params=None, max_retries=float('inf')):
        """Make API request with timeout and 429 error handling."""
        attempt = 1
        
        while attempt <= max_retries:
            try:
                # Rate limiting: ensure at least 1 second between requests
                current_time = time.time()
                time_since_last_request = current_time - self.last_request_time
                if time_since_last_request < 1.0:
                    sleep_time = 1.0 - time_since_last_request
                    time.sleep(sleep_time)
                
                # Rotate API key for each request
                self.request_count += 1
                if self.request_count % 4 == 0:  # Rotate every 4 requests
                    self.get_next_api_key()
                
                # Add current API key to params
                if params is None:
                    params = {}
                params['api_key'] = self.current_api_key
                
                # Update last request time before making the request
                self.last_request_time = time.time()
                
                response = self.session.get(url, params=params, timeout=self.timeout)
                
                if response.status_code == 429:
                    self.handle_429_error(attempt)
                    attempt += 1
                    continue
                    
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout:
                print(f"\nTimeout error for {url} (attempt {attempt})")
                if attempt < 3:  # Retry timeouts up to 3 times
                    attempt += 1
                    time.sleep(1)
                    continue
                return None
                
            except requests.exceptions.RequestException as e:
                if "429" in str(e):  # Double-check for 429 errors
                    self.handle_429_error(attempt)
                    attempt += 1
                    continue
                print(f"\nError making request to {url}: {e}")
                return None
    
    def get_articles_for_issue(self, volume_number, issue_number):
        """Get articles for a specific Congressional Record issue."""
        url = f"https://api.congress.gov/v3/daily-congressional-record/{volume_number}/{issue_number}/articles"
        params = {
            'format': 'json',
            'limit': 250  # Maximum allowed
        }
        
        data = self.make_request(url, params)
        
        if not data or 'articles' not in data:
            return []
        
        return data['articles']
    
    def download_article_text(self, url, filename, max_retries=3):
        """Download the text content of an article with retry logic."""
        attempt = 1
        
        while attempt <= max_retries:
            try:
                # Rate limiting: ensure at least 1 second between requests
                current_time = time.time()
                time_since_last_request = current_time - self.last_request_time
                if time_since_last_request < 1.0:
                    sleep_time = 1.0 - time_since_last_request
                    time.sleep(sleep_time)
                
                # Rotate API key for downloads too
                self.request_count += 1
                if self.request_count % 10 == 0:
                    self.get_next_api_key()
                
                # Update last request time before making the request
                self.last_request_time = time.time()
                
                response = self.session.get(url, timeout=self.timeout)
                
                if response.status_code == 429:
                    self.handle_429_error(attempt)
                    attempt += 1
                    continue
                    
                response.raise_for_status()
                
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                return True
                
            except requests.exceptions.Timeout:
                if attempt < max_retries:
                    attempt += 1
                    time.sleep(1)
                    continue
                return False
                
            except Exception as e:
                if "429" in str(e):
                    self.handle_429_error(attempt)
                    attempt += 1
                    continue
                return False
    
    def safe_filename(self, text, max_length=100):
        """Create a safe filename from text."""
        safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_. "
        filename = ''.join(c if c in safe_chars else '_' for c in text)
        
        if len(filename) > max_length:
            filename = filename[:max_length]
        
        return filename.strip()
    
    def check_issue_completeness(self, issue, output_dir):
        """Check if an issue is already fully downloaded."""
        volume_number = issue.get('volumeNumber')
        issue_number = issue.get('issueNumber')
        congress = issue.get('congress')
        date = issue.get('issueDate', 'unknown_date')[:10]
        
        # Create subdirectory path
        congress_dir = output_dir / f"congress_{congress}"
        
        if not congress_dir.exists():
            return False, 0, 0
        
        # Get articles for this issue
        articles = self.get_articles_for_issue(volume_number, issue_number)
        
        total_articles = 0
        existing_articles = 0
        
        # Check each article
        for article in articles:
            section_articles = article.get('sectionArticles', [])
            
            for i, section_article in enumerate(section_articles):
                title = section_article.get('title', f'Section_{i}')
                text_urls = section_article.get('text', [])
                
                # Look for formatted text URL
                formatted_text_url = None
                for text_item in text_urls:
                    if text_item.get('type') == 'Formatted Text':
                        formatted_text_url = text_item.get('url')
                        break
                
                if formatted_text_url:
                    total_articles += 1
                    
                    # Check if file exists
                    safe_title = self.safe_filename(title)
                    filename = congress_dir / f"{date}_c{congress}_v{volume_number}_i{issue_number}_{safe_title}.html"
                    
                    if filename.exists():
                        existing_articles += 1
        
        # Consider issue complete if all articles exist
        is_complete = (total_articles > 0 and existing_articles == total_articles)
        return is_complete, existing_articles, total_articles
    
    def process_issue(self, issue, output_dir, progress_bar):
        """Process a single Congressional Record issue."""
        volume_number = issue.get('volumeNumber')
        issue_number = issue.get('issueNumber')
        congress = issue.get('congress')
        date = issue.get('issueDate', 'unknown_date')[:10]
        
        # First check if this issue is already complete
        is_complete, existing_count, total_count = self.check_issue_completeness(issue, output_dir)
        
        # Update progress bar description
        progress_bar.set_description(f"Congress {congress}, Vol {volume_number}, Issue {issue_number} ({date})")
        
        if is_complete:
            self.consecutive_existing_issues += 1
            progress_bar.write(f"  ✓ Issue already complete ({existing_count}/{total_count} articles exist)")
            
            if self.consecutive_existing_issues >= self.stop_threshold:
                progress_bar.write(f"\n🛑 Found {self.stop_threshold} consecutive complete issues. Stopping update.")
                return 0, True  # Return True to signal stop
        else:
            # Reset counter if we find an incomplete issue
            self.consecutive_existing_issues = 0
            
            if existing_count > 0:
                progress_bar.write(f"  ⚠️  Issue partially complete ({existing_count}/{total_count} articles exist)")
        
        # Create subdirectory for this congress
        congress_dir = output_dir / f"congress_{congress}"
        congress_dir.mkdir(exist_ok=True)
        
        # Get articles for this issue
        articles = self.get_articles_for_issue(volume_number, issue_number)
        
        downloaded_count = 0
        newly_downloaded = 0
        
        # Process each article
        for article in articles:
            article_name = article.get('name', 'Unknown Article')
            section_articles = article.get('sectionArticles', [])
            
            if not section_articles:
                continue
            
            # Process each section article
            for i, section_article in enumerate(section_articles):
                title = section_article.get('title', f'Section_{i}')
                text_urls = section_article.get('text', [])
                
                # Look for formatted text URL
                formatted_text_url = None
                for text_item in text_urls:
                    if text_item.get('type') == 'Formatted Text':
                        formatted_text_url = text_item.get('url')
                        break
                
                if formatted_text_url:
                    # Create filename
                    safe_title = self.safe_filename(title)
                    filename = congress_dir / f"{date}_c{congress}_v{volume_number}_i{issue_number}_{safe_title}.html"
                    
                    # Skip if already downloaded
                    if filename.exists():
                        downloaded_count += 1
                        continue
                    
                    # Download the text
                    if self.download_article_text(formatted_text_url, filename):
                        downloaded_count += 1
                        newly_downloaded += 1
                        
                        # Save metadata
                        metadata = {
                            'title': title,
                            'congress': congress,
                            'volume_number': volume_number,
                            'issue_number': issue_number,
                            'date_issued': date,
                            'article_name': article_name,
                            'start_page': section_article.get('startPage'),
                            'end_page': section_article.get('endPage'),
                            'source_url': formatted_text_url,
                            'downloaded_at': datetime.now().isoformat()
                        }
                        
                        metadata_filename = filename.with_suffix('.json')
                        with open(metadata_filename, 'w', encoding='utf-8') as f:
                            json.dump(metadata, f, indent=2)
        
        if newly_downloaded > 0:
            progress_bar.write(f"  ✨ Downloaded {newly_downloaded} new articles")
        
        return newly_downloaded, False  # Return False to continue processing

def load_api_keys(filename='congressional_api_keys.txt'):
    """Load API keys from a file, one per line."""
    try:
        with open(filename, 'r') as f:
            keys = [line.strip() for line in f if line.strip()]
        
        if not keys:
            print(f"Error: No API keys found in {filename}")
            sys.exit(1)
            
        print(f"Loaded {len(keys)} API key(s) from {filename}")
        return keys
        
    except FileNotFoundError:
        # Fall back to environment variable if file not found
        print(f"Warning: {filename} not found, checking environment variable...")
        api_key = os.getenv('CONGRESSIONAL_API_KEY')
        if not api_key:
            print("Error: No API keys found in file or environment variable")
            sys.exit(1)
        print("Using single API key from environment variable")
        return [api_key]

def fetch_recent_issues(downloader, start_date=None):
    """Fetch recent Congressional Record issues from the API."""
    print("Fetching recent Congressional Record issues...")
    
    all_issues = []
    offset = 0
    limit = 250  # Max allowed by API
    
    # If no start_date provided, use 2014 with timezone
    if start_date is None:
        start_date = datetime(2014, 1, 1, tzinfo=timezone.utc)  # Fixed: Added timezone
    
    while True:
        url = "https://api.congress.gov/v3/daily-congressional-record"
        params = {
            'format': 'json',
            'limit': limit,
            'offset': offset,
            'sort': 'issueDate desc'  # Newest first
        }
        
        data = downloader.make_request(url, params)
        
        if not data or 'dailyCongressionalRecord' not in data:
            break
        
        issues = data['dailyCongressionalRecord']
        
        if not issues:
            break
        
        # Filter and add issues
        for issue in issues:
            issue_date = datetime.fromisoformat(issue['issueDate'].replace('Z', '+00:00'))
            
            # Stop if we've gone too far back
            if issue_date < start_date:
                return all_issues
            
            all_issues.append(issue)
        
        # Check if there are more results
        pagination = data.get('pagination', {})
        if not pagination.get('next'):
            break
        
        offset += limit
        print(f"  Fetched {len(all_issues)} issues so far...")
    
    return all_issues

def update_issues_file(downloader, existing_issues_file='all_issues.json'):
    """Update the all_issues.json file with recent issues."""
    # Try to load existing issues
    existing_issues = []
    newest_existing_date = None
    
    try:
        with open(existing_issues_file, 'r') as f:
            existing_issues = json.load(f)
            
        if existing_issues:
            # Find the newest date in existing issues
            existing_issues.sort(key=lambda x: x['issueDate'], reverse=True)
            newest_existing_date = datetime.fromisoformat(
                existing_issues[0]['issueDate'].replace('Z', '+00:00')
            )
            print(f"Newest issue in existing file: {newest_existing_date.date()}")
    except FileNotFoundError:
        print(f"No existing {existing_issues_file} found. Will fetch all issues since 2014.")
    except json.JSONDecodeError:
        print(f"Warning: Invalid JSON in {existing_issues_file}. Will fetch all issues since 2014.")
    
    # Fetch recent issues
    recent_issues = fetch_recent_issues(downloader, start_date=newest_existing_date)
    
    if not recent_issues:
        print("No new issues found.")
        return existing_issues
    
    print(f"Found {len(recent_issues)} new issues")
    
    # Merge issues (recent_issues are already newest first)
    # Create a set of existing issue identifiers to avoid duplicates
    existing_ids = {(issue['volumeNumber'], issue['issueNumber']) 
                    for issue in existing_issues}
    
    new_issues_added = 0
    for issue in recent_issues:
        issue_id = (issue['volumeNumber'], issue['issueNumber'])
        if issue_id not in existing_ids:
            existing_issues.append(issue)
            new_issues_added += 1
    
    # Sort all issues by date (newest first)
    existing_issues.sort(key=lambda x: x['issueDate'], reverse=True)
    
    # Save updated issues file
    with open(existing_issues_file, 'w') as f:
        json.dump(existing_issues, f, indent=2)
    
    print(f"Updated {existing_issues_file} with {new_issues_added} new issues")
    
    return existing_issues

def main():
    """Main function to update Congressional Records downloads."""
    # Load API keys
    api_keys = load_api_keys()
    
    # Initialize updater/downloader
    updater = CongressionalRecordUpdater(api_keys, timeout=5)
    
    # Update the issues file first
    print("Step 1: Updating issues list...")
    all_issues = update_issues_file(updater)
    
    if not all_issues:
        print("Error: No issues found")
        sys.exit(1)
    
    print(f"\nTotal issues in database: {len(all_issues)}")
    
    # Filter for issues from 2014 onwards
    issues_2014_onwards = []
    for issue in all_issues:
        issue_date = datetime.fromisoformat(issue['issueDate'].replace('Z', '+00:00'))
        if issue_date.year >= 2014:
            issues_2014_onwards.append(issue)
    
    # Sort by date in REVERSE chronological order (newest first)
    issues_2014_onwards.sort(key=lambda x: x['issueDate'], reverse=True)
    
    print(f"\nStep 2: Checking for new articles to download...")
    print(f"Found {len(issues_2014_onwards)} issues from 2014 onwards")
    print(f"Will stop after finding {3} consecutive fully-downloaded issues")
    
    # Show date range
    if issues_2014_onwards:
        newest_date = issues_2014_onwards[0]['issueDate'][:10]
        oldest_date = issues_2014_onwards[-1]['issueDate'][:10]
        print(f"Date range: {newest_date} (newest) to {oldest_date} (oldest)")
    
    # Create output directory
    output_dir = Path("congressional_records")
    if not output_dir.exists():
        print(f"Warning: '{output_dir}' directory doesn't exist. Creating it...")
        output_dir.mkdir(exist_ok=True)
    
    # Initialize updater with multiple API keys
    updater = CongressionalRecordUpdater(api_keys, timeout=5)
    
    # Process each issue with progress bar
    total_new_downloads = 0
    issues_processed = 0
    
    with tqdm(total=len(issues_2014_onwards), unit="issue") as pbar:
        for issue in issues_2014_onwards:
            try:
                newly_downloaded, should_stop = updater.process_issue(issue, output_dir, pbar)
                total_new_downloads += newly_downloaded
                issues_processed += 1
                
                pbar.set_postfix({
                    "New Articles": total_new_downloads,
                    "Issues Checked": issues_processed,
                    "Current Key": f"#{api_keys.index(updater.current_api_key) + 1}"
                })
                
                if should_stop:
                    break
                    
            except KeyboardInterrupt:
                print("\n\nUpdate interrupted by user")
                break
            except Exception as e:
                print(f"\nUnexpected error processing issue: {e}")
                # Continue with next issue
            
            pbar.update(1)
    
    print(f"\n✅ Update completed!")
    print(f"📊 Summary:")
    print(f"   - Updated issues database with latest entries")
    print(f"   - Checked {issues_processed} issues")
    print(f"   - Downloaded {total_new_downloads} new articles")
    print(f"   - Total API requests: {updater.request_count}")
    
    if total_new_downloads == 0:
        print("\n💡 Your Congressional Records are already up to date!")
    else:
        print(f"\n💡 Successfully added {total_new_downloads} new articles to your collection")
    
    print("\n📁 Note: all_issues.json has been updated with the latest issues")

if __name__ == "__main__":
    main()