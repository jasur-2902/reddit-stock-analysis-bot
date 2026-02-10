#!/usr/bin/env python3
"""
Reddit Data Ingestion Script for AWS DynamoDB
Fetches top daily posts from specified subreddits and stores them in DynamoDB.

Supports multiple scraping backends:
1. Decodo/Smartproxy (ACTIVE) - Residential proxy (~$4/GB, ~$0.02/day)
2. Bright Data Web Unlocker - Premium proxy service (~$0.60/day)
3. Apify Reddit Scraper - Pre-built Reddit actor (pay-per-result)

Toggle SCRAPER_BACKEND to switch between them.
"""

import os
import time
import json
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal
from collections import defaultdict
from urllib.parse import quote
import concurrent.futures

import requests
import httpx
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
# Note: Using REST API for Gemini to avoid grpc binary compatibility issues in Lambda

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# SCRAPER BACKEND CONFIGURATION
# Options: 'decodo', 'webshare', 'brightdata', 'apify'
# ============================================================================
SCRAPER_BACKEND = 'decodo'


class ProxyRedditScraper:
    """
    Scrapes Reddit using residential proxies (Decodo/Smartproxy, Webshare, etc).
    Cost-effective: ~$4/GB with rotating residential IPs.
    """

    def __init__(self):
        """Initialize the proxy Reddit scraper."""
        # Target subreddits
        self.subreddits = ['stocks', 'investing', 'bitcoin', 'StockMarket', 'TheRaceTo10Million']

        # Reddit API endpoints
        self.base_url = 'https://www.reddit.com/r/{}/new.json'  # Changed from top to new

        # Proxy configuration (works with Decodo, Webshare, Smartproxy, etc.)
        self.proxy_host = os.getenv('PROXY_HOST', 'gate.decodo.com')
        self.proxy_port = os.getenv('PROXY_PORT', '7000')
        self.proxy_user = os.getenv('PROXY_USER')
        self.proxy_pass = os.getenv('PROXY_PASS')

        if not self.proxy_user or not self.proxy_pass:
            raise ValueError("PROXY_USER and PROXY_PASS environment variables are required")

        # Build proxy URL for httpx (different format than requests)
        # httpx uses http:// proxy URL for both HTTP and HTTPS destinations
        self.proxy_url = f"http://{self.proxy_user}:{self.proxy_pass}@{self.proxy_host}:{self.proxy_port}"
        logger.info(f"Using proxy: {self.proxy_host}:{self.proxy_port} (user: {self.proxy_user})")

        # Request configuration - realistic Chrome headers for better Cloudflare bypass
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-CH-UA': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        self.max_retries = 3  # Reduced to prevent timeout on failures
        self.retry_delay = 3  # Shorter delay since IP rotates anyway
        self.request_delay = 1  # Delay between requests

        # DynamoDB configuration
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.posts_table_name = 'RedditPosts'
        self.comments_table_name = 'RedditComments'

        # Comment fetching - DISABLED in main Lambda
        # Comments are handled by separate RedditCommentsFetcher Lambda
        self.max_comments_per_post = 25
        self.fetch_comments = False
        self.min_comments_for_fetch = 7
        self.max_parallel_comment_fetches = 50

        # Initialize DynamoDB
        self.dynamodb = None
        self.posts_table = None
        self.comments_table = None

    def connect_to_dynamodb(self) -> bool:
        """Establish connection to DynamoDB tables."""
        try:
            logger.info(f"Connecting to DynamoDB in region {self.region}")
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)

            self.posts_table = self.dynamodb.Table(self.posts_table_name)
            self.comments_table = self.dynamodb.Table(self.comments_table_name)

            # Verify tables exist
            _ = self.posts_table.table_status
            _ = self.comments_table.table_status

            logger.info("Connected to DynamoDB tables")
            return True

        except ClientError as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            return False

    def fetch_url(self, url: str) -> Optional[Dict]:
        """
        Fetch a URL using requests with residential proxy.
        Uses URL-safe encoding for special characters in credentials.

        Args:
            url: The URL to fetch.

        Returns:
            Parsed JSON response or None on failure.
        """
        for attempt in range(self.max_retries):
            try:
                print(f"Fetching: {url[:60]}... (attempt {attempt + 1})")  # Force log
                logger.info(f"Fetching {url} via proxy (attempt {attempt + 1})")

                # URL-encode credentials properly (including tilde which quote() doesn't encode)
                from urllib.parse import quote
                encoded_user = quote(self.proxy_user, safe='')
                # Force encode tilde since it's in our password
                encoded_pass = quote(self.proxy_pass, safe='').replace('~', '%7E')
                proxy_url = f"http://{encoded_user}:{encoded_pass}@{self.proxy_host}:{self.proxy_port}"

                proxies = {
                    'http': proxy_url,
                    'https': proxy_url
                }

                response = requests.get(
                    url,
                    headers=self.headers,
                    proxies=proxies,
                    timeout=30,
                    verify=True
                )

                if response.status_code == 200:
                    data = response.json()
                    # Reddit returns either:
                    # - {"kind": "Listing", "data": {...}} for subreddit pages
                    # - [{"kind": "Listing"...}, {"kind": "Listing"...}] for comment pages
                    if isinstance(data, dict) and 'data' in data:
                        return data
                    if isinstance(data, list) and len(data) > 0:
                        return data
                    logger.warning(f"Unexpected JSON format: {str(data)[:300]}")
                elif response.status_code == 429:
                    logger.warning(f"Rate limited, waiting {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                elif response.status_code == 403:
                    # Check if it's Reddit blocking us
                    if 'whoa there' in response.text.lower() or 'blocked' in response.text.lower():
                        logger.warning(f"Reddit blocked request, retrying with new IP...")
                    else:
                        logger.warning(f"Access denied (403), retrying...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text[:200]}")

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}")
            except requests.exceptions.ProxyError as e:
                logger.error(f"Proxy error: {e}")
                time.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Request error: {e}")

            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)

        return None

    def fetch_subreddit_data(self, subreddit: str) -> Optional[List[Dict]]:
        """
        Fetch top posts from a subreddit.

        Args:
            subreddit: Name of the subreddit.

        Returns:
            List of post dictionaries or None.
        """
        url = f"{self.base_url.format(subreddit)}?limit=100"
        logger.info(f"Fetching r/{subreddit}/new via residential proxy")

        data = self.fetch_url(url)
        if data:
            posts = data.get('data', {}).get('children', [])
            logger.info(f"Fetched {len(posts)} posts from r/{subreddit}")
            return posts

        logger.error(f"Failed to fetch r/{subreddit}")
        return None

    def fetch_post_comments(self, subreddit: str, post_id: str) -> Optional[List[Dict]]:
        """
        Fetch comments for a specific post.

        Args:
            subreddit: The subreddit name.
            post_id: The Reddit post ID (e.g., 't3_xyz').

        Returns:
            List of comment dictionaries or None.
        """
        short_id = post_id.replace('t3_', '')
        url = f"https://www.reddit.com/r/{subreddit}/comments/{short_id}.json?limit={self.max_comments_per_post}"

        time.sleep(self.request_delay)  # Rate limiting
        data = self.fetch_url(url)

        if data and isinstance(data, list) and len(data) >= 2:
            comments = data[1].get('data', {}).get('children', [])
            return comments

        return None

    def extract_comments_recursive(self, comments: List[Dict], post_id: str, depth: int = 0) -> List[Dict]:
        """Recursively extract comments and their replies."""
        extracted = []

        for comment in comments:
            if comment.get('kind') != 't1':
                continue

            comment_data = comment.get('data', {})
            comment_id = comment_data.get('name', '')

            if not comment_id:
                continue

            extracted.append({
                'comment_id': comment_id,
                'post_id': post_id,
                'parent_id': comment_data.get('parent_id', ''),
                'author': comment_data.get('author', '[deleted]'),
                'body': comment_data.get('body', ''),
                'score': comment_data.get('score', 0),
                'created_utc': comment_data.get('created_utc', 0),
                'depth': depth,
                'is_submitter': comment_data.get('is_submitter', False),
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })

            # Process replies
            replies = comment_data.get('replies')
            if replies and isinstance(replies, dict):
                reply_children = replies.get('data', {}).get('children', [])
                if reply_children:
                    extracted.extend(self.extract_comments_recursive(reply_children, post_id, depth + 1))

        return extracted

    def upsert_post(self, post: Dict) -> bool:
        """Upsert a post into DynamoDB."""
        try:
            post_data = post.get('data', {})
            post_id = post_data.get('name', '')
            if not post_id:
                return False

            permalink = post_data.get('permalink', '')
            post_url = f"https://www.reddit.com{permalink}" if permalink else ''

            self.posts_table.update_item(
                Key={'post_id': post_id},
                UpdateExpression="""
                    SET title = :title,
                        selftext = :selftext,
                        score = :score,
                        num_comments = :num_comments,
                        #url = :url,
                        updated_at = :updated_at,
                        created_utc = if_not_exists(created_utc, :created_utc),
                        #processed = if_not_exists(#processed, :processed)
                """,
                ExpressionAttributeNames={
                    '#url': 'url',
                    '#processed': 'processed'
                },
                ExpressionAttributeValues={
                    ':title': post_data.get('title', ''),
                    ':selftext': post_data.get('selftext', ''),
                    ':score': Decimal(str(post_data.get('score', 0))),
                    ':num_comments': Decimal(str(post_data.get('num_comments', 0))),
                    ':url': post_url,
                    ':updated_at': datetime.now(timezone.utc).isoformat(),
                    ':created_utc': Decimal(str(post_data.get('created_utc', 0))),
                    ':processed': False
                }
            )
            return True

        except ClientError as e:
            logger.error(f"DynamoDB error: {e}")
            return False

    def upsert_comment(self, comment: Dict) -> bool:
        """Upsert a comment into DynamoDB."""
        try:
            self.comments_table.update_item(
                Key={'comment_id': comment['comment_id']},
                UpdateExpression="""
                    SET post_id = :post_id,
                        parent_id = :parent_id,
                        author = :author,
                        body = :body,
                        score = :score,
                        created_utc = if_not_exists(created_utc, :created_utc),
                        #depth = :depth,
                        is_submitter = :is_submitter,
                        scraped_at = :scraped_at
                """,
                ExpressionAttributeNames={'#depth': 'depth'},
                ExpressionAttributeValues={
                    ':post_id': comment['post_id'],
                    ':parent_id': comment.get('parent_id', ''),
                    ':author': comment.get('author', '[deleted]'),
                    ':body': comment.get('body', ''),
                    ':score': Decimal(str(comment.get('score', 0))),
                    ':created_utc': Decimal(str(comment.get('created_utc', 0))),
                    ':depth': Decimal(str(comment.get('depth', 0))),
                    ':is_submitter': comment.get('is_submitter', False),
                    ':scraped_at': comment.get('scraped_at', datetime.now(timezone.utc).isoformat())
                }
            )
            return True

        except ClientError as e:
            logger.error(f"DynamoDB error: {e}")
            return False

    def process_subreddit(self, subreddit: str) -> Dict[str, int]:
        """Process all posts from a subreddit with parallel comment fetching."""
        stats = {
            'posts_fetched': 0,
            'posts_processed': 0,
            'posts_errors': 0,
            'comments_fetched': 0,
            'comments_processed': 0,
            'comments_errors': 0
        }

        posts = self.fetch_subreddit_data(subreddit)
        if not posts:
            return stats

        stats['posts_fetched'] = len(posts)

        # First pass: upsert all posts and collect those needing comments
        posts_needing_comments = []

        for post in posts:
            post_data = post.get('data', {})
            post_id = post_data.get('name', '')

            if self.upsert_post(post):
                stats['posts_processed'] += 1

                # Check if this post needs comment fetching
                num_comments = int(post_data.get('num_comments', 0))
                if self.fetch_comments and post_id and num_comments >= self.min_comments_for_fetch:
                    posts_needing_comments.append(post_id)
            else:
                stats['posts_errors'] += 1

        # Second pass: fetch comments in parallel batches
        if posts_needing_comments:
            logger.info(f"Fetching comments for {len(posts_needing_comments)} posts in parallel...")

            def fetch_comments_for_post(post_id):
                """Fetch and extract comments for a single post."""
                try:
                    comments = self.fetch_post_comments(subreddit, post_id)
                    if comments:
                        return self.extract_comments_recursive(comments, post_id)
                except Exception as e:
                    logger.error(f"Error fetching comments for {post_id}: {e}")
                return []

            # Process in batches to avoid overwhelming the proxy
            batch_size = self.max_parallel_comment_fetches
            all_comments = []

            for i in range(0, len(posts_needing_comments), batch_size):
                batch = posts_needing_comments[i:i + batch_size]
                logger.info(f"  Fetching comment batch {i//batch_size + 1} ({len(batch)} posts)...")

                with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
                    batch_results = list(executor.map(fetch_comments_for_post, batch))

                for comments in batch_results:
                    all_comments.extend(comments)

                # Small delay between batches
                if i + batch_size < len(posts_needing_comments):
                    time.sleep(1)

            # Batch write all comments (much faster than individual updates)
            stats['comments_fetched'] = len(all_comments)
            if all_comments:
                logger.info(f"  Batch writing {len(all_comments)} comments to DynamoDB...")
                try:
                    with self.comments_table.batch_writer() as batch:
                        for comment in all_comments:
                            item = {
                                'comment_id': comment['comment_id'],
                                'post_id': comment['post_id'],
                                'parent_id': comment.get('parent_id', ''),
                                'author': comment.get('author', '[deleted]'),
                                'body': comment.get('body', ''),
                                'score': Decimal(str(comment.get('score', 0))),
                                'created_utc': Decimal(str(comment.get('created_utc', 0))),
                                'depth': Decimal(str(comment.get('depth', 0))),
                                'is_submitter': comment.get('is_submitter', False),
                                'scraped_at': comment.get('scraped_at', datetime.now(timezone.utc).isoformat())
                            }
                            batch.put_item(Item=item)
                            stats['comments_processed'] += 1
                except Exception as e:
                    logger.error(f"Error batch writing comments: {e}")
                    stats['comments_errors'] = len(all_comments) - stats['comments_processed']

        logger.info(f"r/{subreddit} - Posts: {stats['posts_processed']}/{stats['posts_fetched']}, "
                   f"Comments: {stats['comments_processed']}/{stats['comments_fetched']}")

        return stats

    def run(self) -> Dict[str, Any]:
        """Main execution method."""
        print("ProxyRedditScraper.run() called")  # Force log
        logger.info("=" * 50)
        logger.info("Starting Reddit ingestion via residential proxy")
        logger.info(f"Proxy: {self.proxy_host}:{self.proxy_port}")
        logger.info(f"Subreddits: {self.subreddits}")
        logger.info("=" * 50)

        print("Connecting to DynamoDB...")  # Force log
        if not self.connect_to_dynamodb():
            return {'success': False, 'error': 'DynamoDB connection failed'}
        print("DynamoDB connected")  # Force log

        total_stats = {
            'posts_fetched': 0,
            'posts_processed': 0,
            'posts_errors': 0,
            'comments_fetched': 0,
            'comments_processed': 0,
            'comments_errors': 0
        }

        try:
            for subreddit in self.subreddits:
                logger.info(f"Processing r/{subreddit}...")

                if subreddit != self.subreddits[0]:
                    time.sleep(2)  # Delay between subreddits

                stats = self.process_subreddit(subreddit)

                total_stats['posts_fetched'] += stats['posts_fetched']
                total_stats['posts_processed'] += stats['posts_processed']
                total_stats['posts_errors'] += stats['posts_errors']
                total_stats['comments_fetched'] += stats['comments_fetched']
                total_stats['comments_processed'] += stats['comments_processed']
                total_stats['comments_errors'] += stats['comments_errors']

            logger.info("=" * 50)
            logger.info("Proxy Reddit ingestion completed!")
            logger.info(f"Posts - Fetched: {total_stats['posts_fetched']}, "
                       f"Processed: {total_stats['posts_processed']}, "
                       f"Errors: {total_stats['posts_errors']}")
            logger.info(f"Comments - Fetched: {total_stats['comments_fetched']}, "
                       f"Processed: {total_stats['comments_processed']}, "
                       f"Errors: {total_stats['comments_errors']}")

            # Fail if no posts scraped
            if total_stats['posts_processed'] == 0:
                return {
                    'success': False,
                    'error': 'No posts scraped - proxy may be blocked',
                    'stats': total_stats,
                    'backend': 'proxy'
                }

            return {
                'success': True,
                'stats': total_stats,
                'backend': 'proxy'
            }

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {'success': False, 'error': str(e)}


class ApifyRedditScraper:
    """
    Scrapes Reddit using Apify's Reddit Scraper actor (trudax/reddit-scraper).
    Cost-effective alternative to Bright Data (~$0.10-0.20/day vs $0.60/day).
    """

    def __init__(self):
        """Initialize the Apify Reddit scraper."""
        # Target subreddits
        self.subreddits = ['stocks', 'investing', 'bitcoin', 'StockMarket', 'TheRaceTo10Million']

        # Apify configuration
        self.apify_token = os.getenv('APIFY_API_TOKEN')
        # Using free Reddit API Scraper (pay-per-result, ~$0.005/item)
        self.actor_id = 'trudax~reddit-scraper-lite'
        self.api_base = 'https://api.apify.com/v2'

        if not self.apify_token:
            raise ValueError("APIFY_API_TOKEN environment variable is required")

        # DynamoDB configuration
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.posts_table_name = 'RedditPosts'
        self.comments_table_name = 'RedditComments'

        # Processing limits
        self.max_posts_per_subreddit = 100
        self.max_comments_per_post = 50
        self.actor_timeout_secs = 300  # 5 minutes max per run

        # Initialize DynamoDB
        self.dynamodb = None
        self.posts_table = None
        self.comments_table = None

    def connect_to_dynamodb(self) -> bool:
        """Establish connection to DynamoDB tables."""
        try:
            logger.info(f"Connecting to DynamoDB in region {self.region}")
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)

            self.posts_table = self.dynamodb.Table(self.posts_table_name)
            self.comments_table = self.dynamodb.Table(self.comments_table_name)

            # Verify tables exist
            _ = self.posts_table.table_status
            _ = self.comments_table.table_status

            logger.info("Connected to DynamoDB tables")
            return True

        except ClientError as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            return False

    def run_actor(self, subreddit: str) -> Optional[str]:
        """
        Start an Apify actor run for a subreddit.

        Args:
            subreddit: The subreddit name to scrape.

        Returns:
            Run ID if started successfully, None otherwise.
        """
        url = f"{self.api_base}/acts/{self.actor_id}/runs"

        # Actor input configuration
        actor_input = {
            "startUrls": [
                {"url": f"https://www.reddit.com/r/{subreddit}/top/?t=day"}
            ],
            "maxItems": self.max_posts_per_subreddit,
            "maxPostCount": self.max_posts_per_subreddit,
            "maxComments": self.max_comments_per_post,
            "maxCommunitiesCount": 1,
            "scrollTimeout": 40,
            "searchComments": True,
            "searchCommunities": False,
            "searchPosts": True,
            "searchUsers": False,
            "skipComments": False,
            "proxy": {
                "useApifyProxy": True
            }
        }

        try:
            logger.info(f"Starting Apify actor for r/{subreddit}")

            response = requests.post(
                url,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {self.apify_token}'
                },
                json=actor_input,
                timeout=30
            )

            if response.status_code == 201:
                run_data = response.json().get('data', {})
                run_id = run_data.get('id')
                logger.info(f"Actor started for r/{subreddit}, run ID: {run_id}")
                return run_id
            else:
                logger.error(f"Failed to start actor: {response.status_code} - {response.text[:200]}")
                return None

        except Exception as e:
            logger.error(f"Error starting Apify actor: {e}")
            return None

    def wait_for_run(self, run_id: str, timeout: int = 300) -> bool:
        """
        Wait for an actor run to complete.

        Args:
            run_id: The Apify run ID.
            timeout: Maximum wait time in seconds.

        Returns:
            True if run completed successfully.
        """
        url = f"{self.api_base}/actor-runs/{run_id}"
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(
                    url,
                    headers={'Authorization': f'Bearer {self.apify_token}'},
                    timeout=30
                )

                if response.status_code == 200:
                    run_data = response.json().get('data', {})
                    status = run_data.get('status')

                    if status == 'SUCCEEDED':
                        logger.info(f"Run {run_id} completed successfully")
                        return True
                    elif status in ['FAILED', 'ABORTED', 'TIMED-OUT']:
                        logger.error(f"Run {run_id} failed with status: {status}")
                        return False
                    else:
                        # Still running
                        logger.debug(f"Run {run_id} status: {status}")
                        time.sleep(10)
                else:
                    logger.error(f"Error checking run status: {response.status_code}")
                    time.sleep(10)

            except Exception as e:
                logger.error(f"Error polling run status: {e}")
                time.sleep(10)

        logger.error(f"Run {run_id} timed out after {timeout} seconds")
        return False

    def get_run_results(self, run_id: str) -> List[Dict]:
        """
        Get results from a completed actor run.

        Args:
            run_id: The Apify run ID.

        Returns:
            List of scraped items.
        """
        url = f"{self.api_base}/actor-runs/{run_id}/dataset/items"

        try:
            response = requests.get(
                url,
                headers={'Authorization': f'Bearer {self.apify_token}'},
                timeout=60
            )

            if response.status_code == 200:
                items = response.json()
                logger.info(f"Retrieved {len(items)} items from run {run_id}")
                return items
            else:
                logger.error(f"Error getting results: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error fetching run results: {e}")
            return []

    def process_apify_post(self, item: Dict, subreddit: str) -> Optional[Dict]:
        """
        Convert Apify result to our post format.

        Args:
            item: Raw item from Apify.
            subreddit: The subreddit name.

        Returns:
            Processed post dict or None.
        """
        try:
            # Apify returns different structures - handle both
            post_id = item.get('id') or item.get('postId')
            if not post_id:
                return None

            # Ensure post_id has t3_ prefix
            if not post_id.startswith('t3_'):
                post_id = f"t3_{post_id}"

            return {
                'post_id': post_id,
                'title': item.get('title', ''),
                'selftext': item.get('body', '') or item.get('selftext', ''),
                'score': int(item.get('upVotes', 0) or item.get('score', 0)),
                'num_comments': int(item.get('numberOfComments', 0) or item.get('numComments', 0)),
                'url': item.get('url', ''),
                'subreddit': subreddit,
                'created_utc': item.get('createdAt', ''),
                'author': item.get('username', '') or item.get('author', '')
            }
        except Exception as e:
            logger.error(f"Error processing Apify post: {e}")
            return None

    def process_apify_comments(self, item: Dict, post_id: str) -> List[Dict]:
        """
        Extract comments from Apify result.

        Args:
            item: Raw item from Apify containing comments.
            post_id: The parent post ID.

        Returns:
            List of processed comment dicts.
        """
        comments = []
        raw_comments = item.get('comments', [])

        for comment in raw_comments[:self.max_comments_per_post]:
            try:
                comment_id = comment.get('id', '')
                if not comment_id:
                    continue

                if not comment_id.startswith('t1_'):
                    comment_id = f"t1_{comment_id}"

                comments.append({
                    'comment_id': comment_id,
                    'post_id': post_id,
                    'parent_id': comment.get('parentId', post_id),
                    'author': comment.get('username', '[deleted]'),
                    'body': comment.get('body', ''),
                    'score': int(comment.get('upVotes', 0)),
                    'created_utc': comment.get('createdAt', ''),
                    'depth': int(comment.get('depth', 0)),
                    'is_submitter': comment.get('isSubmitter', False),
                    'scraped_at': datetime.now(timezone.utc).isoformat()
                })
            except Exception as e:
                logger.debug(f"Error processing comment: {e}")
                continue

        return comments

    def upsert_post(self, post: Dict) -> bool:
        """Upsert a post into DynamoDB."""
        try:
            post_id = post['post_id']

            self.posts_table.update_item(
                Key={'post_id': post_id},
                UpdateExpression="""
                    SET title = :title,
                        selftext = :selftext,
                        score = :score,
                        num_comments = :num_comments,
                        #url = :url,
                        updated_at = :updated_at,
                        subreddit = :subreddit,
                        author = :author,
                        #processed = if_not_exists(#processed, :processed)
                """,
                ExpressionAttributeNames={
                    '#url': 'url',
                    '#processed': 'processed'
                },
                ExpressionAttributeValues={
                    ':title': post.get('title', ''),
                    ':selftext': post.get('selftext', ''),
                    ':score': Decimal(str(post.get('score', 0))),
                    ':num_comments': Decimal(str(post.get('num_comments', 0))),
                    ':url': post.get('url', ''),
                    ':updated_at': datetime.now(timezone.utc).isoformat(),
                    ':subreddit': post.get('subreddit', ''),
                    ':author': post.get('author', ''),
                    ':processed': False
                }
            )
            return True

        except ClientError as e:
            logger.error(f"DynamoDB error for post: {e}")
            return False

    def upsert_comment(self, comment: Dict) -> bool:
        """Upsert a comment into DynamoDB."""
        try:
            self.comments_table.update_item(
                Key={'comment_id': comment['comment_id']},
                UpdateExpression="""
                    SET post_id = :post_id,
                        parent_id = :parent_id,
                        author = :author,
                        body = :body,
                        score = :score,
                        #depth = :depth,
                        is_submitter = :is_submitter,
                        scraped_at = :scraped_at
                """,
                ExpressionAttributeNames={
                    '#depth': 'depth'
                },
                ExpressionAttributeValues={
                    ':post_id': comment['post_id'],
                    ':parent_id': comment.get('parent_id', ''),
                    ':author': comment.get('author', '[deleted]'),
                    ':body': comment.get('body', ''),
                    ':score': Decimal(str(comment.get('score', 0))),
                    ':depth': Decimal(str(comment.get('depth', 0))),
                    ':is_submitter': comment.get('is_submitter', False),
                    ':scraped_at': comment.get('scraped_at', datetime.now(timezone.utc).isoformat())
                }
            )
            return True

        except ClientError as e:
            logger.error(f"DynamoDB error for comment: {e}")
            return False

    def process_subreddit(self, subreddit: str) -> Dict[str, int]:
        """
        Process a single subreddit via Apify.

        Args:
            subreddit: The subreddit name.

        Returns:
            Dictionary with processing statistics.
        """
        stats = {
            'posts_fetched': 0,
            'posts_processed': 0,
            'posts_errors': 0,
            'comments_fetched': 0,
            'comments_processed': 0,
            'comments_errors': 0
        }

        # Start actor run
        run_id = self.run_actor(subreddit)
        if not run_id:
            return stats

        # Wait for completion
        if not self.wait_for_run(run_id, timeout=self.actor_timeout_secs):
            return stats

        # Get results
        items = self.get_run_results(run_id)
        stats['posts_fetched'] = len(items)

        # Process each item
        for item in items:
            post = self.process_apify_post(item, subreddit)
            if not post:
                stats['posts_errors'] += 1
                continue

            if self.upsert_post(post):
                stats['posts_processed'] += 1

                # Process comments
                comments = self.process_apify_comments(item, post['post_id'])
                stats['comments_fetched'] += len(comments)

                for comment in comments:
                    if self.upsert_comment(comment):
                        stats['comments_processed'] += 1
                    else:
                        stats['comments_errors'] += 1
            else:
                stats['posts_errors'] += 1

        logger.info(f"r/{subreddit} - Posts: {stats['posts_processed']}/{stats['posts_fetched']}, "
                   f"Comments: {stats['comments_processed']}/{stats['comments_fetched']}")

        return stats

    def run(self) -> Dict[str, Any]:
        """
        Main execution method - scrapes all configured subreddits.

        Returns:
            Dictionary with execution statistics.
        """
        logger.info("Starting Reddit data ingestion via Apify...")
        logger.info(f"Actor: {self.actor_id}")
        logger.info(f"Subreddits: {self.subreddits}")

        if not self.connect_to_dynamodb():
            return {'success': False, 'error': 'DynamoDB connection failed'}

        total_stats = {
            'posts_fetched': 0,
            'posts_processed': 0,
            'posts_errors': 0,
            'comments_fetched': 0,
            'comments_processed': 0,
            'comments_errors': 0
        }

        try:
            for subreddit in self.subreddits:
                logger.info(f"Processing r/{subreddit}...")

                stats = self.process_subreddit(subreddit)

                total_stats['posts_fetched'] += stats['posts_fetched']
                total_stats['posts_processed'] += stats['posts_processed']
                total_stats['posts_errors'] += stats['posts_errors']
                total_stats['comments_fetched'] += stats['comments_fetched']
                total_stats['comments_processed'] += stats['comments_processed']
                total_stats['comments_errors'] += stats['comments_errors']

                # Small delay between subreddits
                time.sleep(2)

            logger.info("=" * 50)
            logger.info("Apify Reddit ingestion completed!")
            logger.info(f"Posts - Fetched: {total_stats['posts_fetched']}, "
                       f"Processed: {total_stats['posts_processed']}, "
                       f"Errors: {total_stats['posts_errors']}")
            logger.info(f"Comments - Fetched: {total_stats['comments_fetched']}, "
                       f"Processed: {total_stats['comments_processed']}, "
                       f"Errors: {total_stats['comments_errors']}")

            # Fail if no posts were scraped (indicates API/quota issues)
            if total_stats['posts_processed'] == 0:
                return {
                    'success': False,
                    'error': 'No posts scraped - Apify quota may be exhausted',
                    'stats': total_stats,
                    'backend': 'apify'
                }

            return {
                'success': True,
                'stats': total_stats,
                'backend': 'apify'
            }

        except Exception as e:
            logger.error(f"Unexpected error during ingestion: {e}")
            return {'success': False, 'error': str(e)}


class RedditDynamoIngester:
    """
    A class to scrape Reddit posts using Bright Data's Web Unlocker API
    and store them in DynamoDB with deduplication.

    Bright Data handles proxy rotation and bypasses Reddit's IP blocking.
    """

    def __init__(self):
        """Initialize the Reddit ingester with configuration."""
        # Target subreddits to scrape
        self.subreddits = ['stocks', 'investing', 'bitcoin', 'StockMarket', 'TheRaceTo10Million']

        # Reddit JSON endpoint configuration
        self.base_url = 'https://www.reddit.com/r/{}/top.json'
        self.time_filter = 'day'

        # Bright Data Web Unlocker API configuration
        # API endpoint for Web Unlocker
        self.brightdata_api_url = 'https://api.brightdata.com/request'

        # Load Bright Data credentials from environment variables
        # BRIGHTDATA_API_TOKEN: Your API token from Bright Data dashboard
        # BRIGHTDATA_ZONE: Your Web Unlocker zone name
        self.brightdata_api_token = os.getenv('BRIGHTDATA_API_TOKEN')
        self.brightdata_zone = os.getenv('BRIGHTDATA_ZONE', 'web_unlocker1')

        # Validate Bright Data configuration
        if not self.brightdata_api_token:
            logger.warning("BRIGHTDATA_API_TOKEN not set - will attempt direct requests (may be blocked)")

        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 10  # Reduced delay since Bright Data handles rate limiting

        # DynamoDB configuration
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.posts_table_name = 'RedditPosts'
        self.comments_table_name = 'RedditComments'

        # Comment fetching configuration
        self.max_comments_per_post = 50  # Limit comments to fetch per post
        self.fetch_comments = False  # Disabled - Cloudflare blocks comment fetching  # Enable comment fetching

        # Initialize DynamoDB
        self.dynamodb = None
        self.posts_table = None
        self.comments_table = None

    def connect_to_dynamodb(self) -> bool:
        """
        Establish connection to DynamoDB tables (posts and comments).

        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to DynamoDB in region {self.region}")
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)

            # Connect to posts table
            self.posts_table = self.dynamodb.Table(self.posts_table_name)
            posts_status = self.posts_table.table_status
            logger.info(f"Connected to posts table: {self.posts_table_name} (status: {posts_status})")

            # Connect to comments table
            self.comments_table = self.dynamodb.Table(self.comments_table_name)
            comments_status = self.comments_table.table_status
            logger.info(f"Connected to comments table: {self.comments_table_name} (status: {comments_status})")

            return True

        except ClientError as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to DynamoDB: {e}")
            return False

    def fetch_subreddit_data(self, subreddit: str) -> Optional[List[Dict]]:
        """
        Fetch top posts from a specific subreddit using Bright Data's Web Unlocker API.

        Bright Data routes the request through residential proxies to bypass
        Reddit's blocking of AWS/datacenter IP addresses.

        Args:
            subreddit: Name of the subreddit to fetch data from.

        Returns:
            List of post dictionaries if successful, None otherwise.
        """
        # Build the target Reddit URL with query parameters
        target_url = f"{self.base_url.format(subreddit)}?t={self.time_filter}&limit=100"

        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching data from r/{subreddit} via Bright Data (attempt {attempt + 1}/{self.max_retries})")

                # Use Bright Data Web Unlocker API
                if self.brightdata_api_token:
                    response = self._fetch_via_brightdata(target_url)
                else:
                    # Fallback to direct request (likely to be blocked on AWS)
                    logger.warning("Bright Data not configured, using direct request")
                    response = self._fetch_direct(target_url)

                if response is None:
                    if attempt < self.max_retries - 1:
                        logger.info(f"Retrying in {self.retry_delay} seconds...")
                        time.sleep(self.retry_delay)
                        continue
                    else:
                        logger.error(f"Failed to fetch data from r/{subreddit} after {self.max_retries} attempts")
                        return None

                # Parse the response
                posts = response.get('data', {}).get('children', [])
                logger.info(f"Successfully fetched {len(posts)} posts from r/{subreddit}")
                return posts

            except Exception as e:
                logger.error(f"Error fetching r/{subreddit}: {e}")
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to fetch data from r/{subreddit} after {self.max_retries} attempts")

        return None

    def _fetch_via_brightdata(self, target_url: str) -> Optional[Dict]:
        """
        Fetch a URL using Bright Data's Web Unlocker API.

        The Web Unlocker API handles:
        - Proxy rotation with residential IPs
        - CAPTCHA solving
        - Rate limit handling
        - Browser fingerprinting

        Args:
            target_url: The URL to fetch.

        Returns:
            Parsed JSON response or None on failure.
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.brightdata_api_token}'
        }

        # Bright Data API request payload
        payload = {
            'zone': self.brightdata_zone,
            'url': target_url,
            'format': 'raw'  # Return raw response content
        }

        try:
            logger.info(f"Bright Data request to: {target_url}")
            response = requests.post(
                self.brightdata_api_url,
                headers=headers,
                json=payload,
                timeout=60  # Longer timeout for proxy routing
            )

            # Log response status for debugging
            logger.info(f"Bright Data response status: {response.status_code}")

            # Check for Bright Data API errors
            if response.status_code == 401:
                logger.error("Bright Data authentication failed - check BRIGHTDATA_API_TOKEN")
                return None
            elif response.status_code == 403:
                logger.error("Bright Data access denied - check zone permissions")
                return None
            elif response.status_code == 429:
                logger.warning("Bright Data rate limited - waiting before retry")
                time.sleep(self.retry_delay)
                return None
            elif response.status_code != 200:
                logger.error(f"Bright Data API error {response.status_code}: {response.text[:500]}")
                return None

            # Check if response is empty or an error message
            response_text = response.text
            if not response_text:
                logger.error("Bright Data returned empty response")
                return None

            # Check for Bright Data error messages (200 status but error in body)
            if response_text.startswith('Request Failed') or response_text.startswith('Error'):
                logger.error(f"Bright Data error: {response_text[:500]}")
                return None

            # Parse the JSON response from Reddit
            return response.json()

        except requests.exceptions.Timeout:
            logger.error("Bright Data request timed out")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Bright Data request error: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON (response: {response.text[:200]}): {e}")
            return None

    def _fetch_direct(self, target_url: str) -> Optional[Dict]:
        """
        Fallback: Fetch URL directly without Bright Data (may be blocked on AWS).

        Args:
            target_url: The URL to fetch.

        Returns:
            Parsed JSON response or None on failure.
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        }

        try:
            response = requests.get(target_url, headers=headers, timeout=30)

            if response.status_code == 401:
                logger.error("Reddit returned 401 - AWS IP likely blocked")
                return None
            elif response.status_code == 429:
                logger.warning("Reddit rate limited")
                return None

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Direct request error: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            return None

    def upsert_post(self, post: Dict) -> bool:
        """
        Upsert a post into DynamoDB using update_item.
        - Always updates: title, selftext, score, num_comments, url, updated_at
        - Sets only if new: created_utc, processed, post_id

        Args:
            post: Raw Reddit post data from the API.

        Returns:
            bool: True if operation successful, False otherwise.
        """
        try:
            post_data = post.get('data', {})

            # Extract post_id (Reddit's "name" field, e.g., "t3_xyz")
            post_id = post_data.get('name', '')
            if not post_id:
                logger.warning("Skipping post with no ID")
                return False

            # Build the post URL
            permalink = post_data.get('permalink', '')
            post_url = f"https://www.reddit.com{permalink}" if permalink else ''

            # Current timestamp for updated_at
            updated_at = datetime.now(timezone.utc).isoformat()

            # DynamoDB requires Decimal for numbers
            score = Decimal(str(post_data.get('score', 0)))
            num_comments = Decimal(str(post_data.get('num_comments', 0)))
            created_utc = Decimal(str(post_data.get('created_utc', 0)))

            # Perform upsert using update_item
            # Note: post_id is the partition key, so it's set automatically
            self.posts_table.update_item(
                Key={'post_id': post_id},
                UpdateExpression="""
                    SET title = :title,
                        selftext = :selftext,
                        score = :score,
                        num_comments = :num_comments,
                        #url = :url,
                        updated_at = :updated_at,
                        created_utc = if_not_exists(created_utc, :created_utc),
                        #processed = if_not_exists(#processed, :processed)
                """,
                ExpressionAttributeNames={
                    '#url': 'url',
                    '#processed': 'processed'
                },
                ExpressionAttributeValues={
                    ':title': post_data.get('title', ''),
                    ':selftext': post_data.get('selftext', ''),
                    ':score': score,
                    ':num_comments': num_comments,
                    ':url': post_url,
                    ':updated_at': updated_at,
                    ':created_utc': created_utc,
                    ':processed': False
                }
            )

            logger.debug(f"Upserted post: {post_id}")
            return True

        except ClientError as e:
            logger.error(f"DynamoDB error for post {post_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error upserting post: {e}")
            return False

    def fetch_post_comments(self, subreddit: str, post_id: str) -> Optional[List[Dict]]:
        """
        Fetch comments for a specific post using Bright Data.

        Args:
            subreddit: The subreddit name.
            post_id: The Reddit post ID (e.g., 't3_xyz').

        Returns:
            List of comment dictionaries or None on failure.
        """
        # Extract the short ID from full Reddit ID (t3_xyz -> xyz)
        short_id = post_id.replace('t3_', '')

        # Reddit comments endpoint
        comments_url = f"https://www.reddit.com/r/{subreddit}/comments/{short_id}.json?limit={self.max_comments_per_post}"

        try:
            if self.brightdata_api_token:
                response = self._fetch_via_brightdata(comments_url)
            else:
                response = self._fetch_direct(comments_url)

            if response is None:
                return None

            # Reddit returns an array: [post_data, comments_data]
            if isinstance(response, list) and len(response) >= 2:
                comments_listing = response[1]
                comments = comments_listing.get('data', {}).get('children', [])
                return comments

            return None

        except Exception as e:
            logger.error(f"Error fetching comments for {post_id}: {e}")
            return None

    def extract_comments_recursive(self, comments: List[Dict], post_id: str, depth: int = 0) -> List[Dict]:
        """
        Recursively extract comments and their replies.

        Args:
            comments: List of comment objects from Reddit API.
            post_id: Parent post ID for linking.
            depth: Current depth in comment tree.

        Returns:
            Flat list of extracted comment dictionaries.
        """
        extracted = []

        for comment in comments:
            if comment.get('kind') != 't1':  # Skip non-comment items (e.g., 'more' links)
                continue

            comment_data = comment.get('data', {})
            comment_id = comment_data.get('name', '')  # e.g., t1_abc123

            if not comment_id:
                continue

            # Extract comment fields
            extracted_comment = {
                'comment_id': comment_id,
                'post_id': post_id,
                'parent_id': comment_data.get('parent_id', ''),  # Parent comment or post ID
                'author': comment_data.get('author', '[deleted]'),
                'body': comment_data.get('body', ''),
                'score': comment_data.get('score', 0),
                'created_utc': comment_data.get('created_utc', 0),
                'depth': depth,
                'is_submitter': comment_data.get('is_submitter', False),  # True if comment by OP
                'scraped_at': datetime.now(timezone.utc).isoformat()
            }
            extracted.append(extracted_comment)

            # Recursively process replies
            replies = comment_data.get('replies')
            if replies and isinstance(replies, dict):
                reply_children = replies.get('data', {}).get('children', [])
                if reply_children:
                    extracted.extend(
                        self.extract_comments_recursive(reply_children, post_id, depth + 1)
                    )

        return extracted

    def upsert_comment(self, comment: Dict) -> bool:
        """
        Upsert a comment into DynamoDB.

        Args:
            comment: Extracted comment data.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            comment_id = comment['comment_id']

            # Convert numeric fields to Decimal for DynamoDB
            score = Decimal(str(comment.get('score', 0)))
            created_utc = Decimal(str(comment.get('created_utc', 0)))
            depth = Decimal(str(comment.get('depth', 0)))

            self.comments_table.update_item(
                Key={'comment_id': comment_id},
                UpdateExpression="""
                    SET post_id = :post_id,
                        parent_id = :parent_id,
                        author = :author,
                        body = :body,
                        score = :score,
                        created_utc = if_not_exists(created_utc, :created_utc),
                        #depth = :depth,
                        is_submitter = :is_submitter,
                        scraped_at = :scraped_at
                """,
                ExpressionAttributeNames={
                    '#depth': 'depth'  # 'depth' is a reserved keyword in DynamoDB
                },
                ExpressionAttributeValues={
                    ':post_id': comment['post_id'],
                    ':parent_id': comment.get('parent_id', ''),
                    ':author': comment.get('author', '[deleted]'),
                    ':body': comment.get('body', ''),
                    ':score': score,
                    ':created_utc': created_utc,
                    ':depth': depth,
                    ':is_submitter': comment.get('is_submitter', False),
                    ':scraped_at': comment.get('scraped_at', datetime.now(timezone.utc).isoformat())
                }
            )

            logger.debug(f"Upserted comment: {comment_id}")
            return True

        except ClientError as e:
            logger.error(f"DynamoDB error for comment {comment.get('comment_id')}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error upserting comment: {e}")
            return False

    def process_post_comments(self, subreddit: str, post_id: str) -> Dict[str, int]:
        """
        Fetch and store all comments for a post.

        Args:
            subreddit: The subreddit name.
            post_id: The Reddit post ID.

        Returns:
            Dictionary with comment processing statistics.
        """
        stats = {'fetched': 0, 'processed': 0, 'errors': 0}

        # Fetch comments from Reddit
        comments = self.fetch_post_comments(subreddit, post_id)

        if not comments:
            logger.debug(f"No comments fetched for post {post_id}")
            return stats

        # Extract all comments recursively (including replies)
        extracted_comments = self.extract_comments_recursive(comments, post_id)
        stats['fetched'] = len(extracted_comments)

        # Upsert each comment
        for comment in extracted_comments:
            if self.upsert_comment(comment):
                stats['processed'] += 1
            else:
                stats['errors'] += 1

        logger.debug(f"Post {post_id}: {stats['processed']} comments processed")
        return stats

    def process_subreddit(self, subreddit: str) -> Dict[str, int]:
        """
        Process all posts and their comments from a single subreddit.

        Args:
            subreddit: Name of the subreddit to process.

        Returns:
            Dictionary with processing statistics.
        """
        stats = {
            'posts_fetched': 0,
            'posts_processed': 0,
            'posts_errors': 0,
            'comments_fetched': 0,
            'comments_processed': 0,
            'comments_errors': 0
        }

        posts = self.fetch_subreddit_data(subreddit)

        if not posts:
            logger.warning(f"No posts fetched from r/{subreddit}")
            return stats

        stats['posts_fetched'] = len(posts)

        for post in posts:
            post_data = post.get('data', {})
            post_id = post_data.get('name', '')

            # Upsert the post
            if self.upsert_post(post):
                stats['posts_processed'] += 1

                # Fetch and store comments for this post
                if self.fetch_comments and post_id:
                    # Small delay between comment fetches to be respectful
                    time.sleep(0.5)

                    comment_stats = self.process_post_comments(subreddit, post_id)
                    stats['comments_fetched'] += comment_stats['fetched']
                    stats['comments_processed'] += comment_stats['processed']
                    stats['comments_errors'] += comment_stats['errors']
            else:
                stats['posts_errors'] += 1

        logger.info(f"r/{subreddit} - Posts: {stats['posts_processed']}/{stats['posts_fetched']}, "
                    f"Comments: {stats['comments_processed']}/{stats['comments_fetched']}")

        return stats

    def run(self) -> Dict[str, Any]:
        """
        Main execution method - scrapes all configured subreddits.

        Returns:
            Dictionary with execution statistics.
        """
        # Log configuration status
        if self.brightdata_api_token:
            logger.info("Starting Reddit data ingestion via Bright Data Web Unlocker...")
            logger.info(f"Bright Data zone: {self.brightdata_zone}")
        else:
            logger.warning("Starting Reddit data ingestion (Bright Data NOT configured - may be blocked)")

        if not self.connect_to_dynamodb():
            logger.error("Failed to connect to DynamoDB. Exiting.")
            return {'success': False, 'error': 'DynamoDB connection failed'}

        total_stats = {
            'posts_fetched': 0,
            'posts_processed': 0,
            'posts_errors': 0,
            'comments_fetched': 0,
            'comments_processed': 0,
            'comments_errors': 0
        }

        try:
            for subreddit in self.subreddits:
                logger.info(f"Processing r/{subreddit}...")

                # Small delay between subreddits to be respectful
                if subreddit != self.subreddits[0]:
                    time.sleep(2)

                stats = self.process_subreddit(subreddit)

                total_stats['posts_fetched'] += stats['posts_fetched']
                total_stats['posts_processed'] += stats['posts_processed']
                total_stats['posts_errors'] += stats['posts_errors']
                total_stats['comments_fetched'] += stats['comments_fetched']
                total_stats['comments_processed'] += stats['comments_processed']
                total_stats['comments_errors'] += stats['comments_errors']

            logger.info("=" * 50)
            logger.info("Reddit data ingestion completed!")
            logger.info(f"Posts - Fetched: {total_stats['posts_fetched']}, "
                       f"Processed: {total_stats['posts_processed']}, "
                       f"Errors: {total_stats['posts_errors']}")
            logger.info(f"Comments - Fetched: {total_stats['comments_fetched']}, "
                       f"Processed: {total_stats['comments_processed']}, "
                       f"Errors: {total_stats['comments_errors']}")

            return {
                'success': True,
                'stats': total_stats,
                'brightdata_enabled': bool(self.brightdata_api_token)
            }

        except KeyboardInterrupt:
            logger.info("Ingestion interrupted by user.")
            return {'success': False, 'error': 'Interrupted'}

        except Exception as e:
            logger.error(f"Unexpected error during ingestion: {e}")
            return {'success': False, 'error': str(e)}


class PostCleaner:
    """
    Cleans Reddit posts by filtering low-quality content using
    heuristics and AI-based semantic analysis with Gemini.
    """

    def __init__(self, posts_table):
        """Initialize the post cleaner."""
        self.table = posts_table

        # Heuristic thresholds
        self.min_comments_threshold = 5

        # Gemini configuration (using REST API to avoid grpc issues in Lambda)
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        self.gemini_model = 'gemini-2.0-flash-lite'
        self.gemini_api_url = f'https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent'
        self.gemini_enabled = bool(self.gemini_api_key)

        if self.gemini_enabled:
            logger.info("Gemini API configured for post cleaning (REST API)")
        else:
            logger.warning("GEMINI_API_KEY not set - skipping semantic filtering")

        # Processing configuration
        self.batch_size = 500  # Process all daily posts
        self.max_text_length = 500
        self.gemini_delay = 0.5

        # Statistics
        self.stats = {
            'total_scanned': 0,
            'skipped_low_engagement': 0,
            'skipped_irrelevant': 0,
            'verified_clean': 0,
            'errors': 0
        }

    def scan_unprocessed_posts(self) -> List[Dict]:
        """Scan for posts that need cleaning."""
        posts = []
        try:
            filter_expression = (
                (Attr('processed').eq(False) | Attr('processed').not_exists()) &
                Attr('cleaning_status').not_exists()
            )

            response = self.table.scan(
                FilterExpression=filter_expression,
                Limit=self.batch_size
            )
            posts.extend(response.get('Items', []))

            while 'LastEvaluatedKey' in response and len(posts) < self.batch_size:
                response = self.table.scan(
                    FilterExpression=filter_expression,
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    Limit=self.batch_size - len(posts)
                )
                posts.extend(response.get('Items', []))

            logger.info(f"Found {len(posts)} posts to clean")
            return posts[:self.batch_size]

        except ClientError as e:
            logger.error(f"Error scanning for posts to clean: {e}")
            return []

    def stage1_heuristic_filter(self, post: Dict) -> bool:
        """Stage 1: Filter based on engagement (num_comments < 5)."""
        num_comments = int(post.get('num_comments', 0))
        return num_comments >= self.min_comments_threshold

    def stage2_semantic_filter(self, post: Dict) -> Optional[bool]:
        """Stage 2: Semantic filter using Gemini REST API."""
        if not self.gemini_enabled:
            return True  # Skip semantic filtering if Gemini not available

        post_id = post.get('post_id', 'unknown')
        title = str(post.get('title', ''))
        selftext = str(post.get('selftext', ''))

        prompt = f"""You are a content moderator for a financial data feed. Analyze this Reddit post.

Answer strictly 'RELEVANT' if it discusses stocks, crypto, economics, investing, trading strategies, market analysis, or financial news.

Answer 'NOISE' if it is a meme, shitpost, purely political (not economic policy), personal rant without financial content, or spam.

Post Title: {title}

Post Content: {selftext[:self.max_text_length] if selftext else '[No content]'}

Your answer (RELEVANT or NOISE):"""

        try:
            time.sleep(self.gemini_delay)

            # Use Gemini REST API
            response = requests.post(
                f"{self.gemini_api_url}?key={self.gemini_api_key}",
                headers={'Content-Type': 'application/json'},
                json={'contents': [{'parts': [{'text': prompt}]}]},
                timeout=30
            )

            if response.status_code == 429:
                logger.warning(f"Gemini rate limited for post {post_id}")
                return None
            elif response.status_code != 200:
                logger.error(f"Gemini API error {response.status_code} for post {post_id}")
                return None

            data = response.json()
            result = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '').strip().upper()

            if 'RELEVANT' in result:
                return True
            elif 'NOISE' in result:
                return False
            else:
                return True  # Default to relevant on ambiguous response

        except Exception as e:
            logger.error(f"Gemini API error for post {post_id}: {e}")
            return None

    def update_post_skipped(self, post_id: str, reason: str) -> bool:
        """Mark a post as skipped/discarded."""
        try:
            self.table.update_item(
                Key={'post_id': post_id},
                UpdateExpression='SET #processed = :processed, cleaning_status = :status, cleaned_at = :cleaned_at',
                ExpressionAttributeNames={'#processed': 'processed'},
                ExpressionAttributeValues={
                    ':processed': True,
                    ':status': reason,
                    ':cleaned_at': datetime.now(timezone.utc).isoformat()
                }
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to update post {post_id} as skipped: {e}")
            return False

    def update_post_verified(self, post_id: str) -> bool:
        """Mark a post as clean and ready for analysis."""
        try:
            self.table.update_item(
                Key={'post_id': post_id},
                UpdateExpression='SET is_clean = :is_clean, cleaning_status = :status, cleaned_at = :cleaned_at',
                ExpressionAttributeValues={
                    ':is_clean': True,
                    ':status': 'verified',
                    ':cleaned_at': datetime.now(timezone.utc).isoformat()
                }
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to update post {post_id} as verified: {e}")
            return False

    def process_post(self, post: Dict) -> None:
        """Process a single post through filtering stages."""
        post_id = post.get('post_id', 'unknown')
        title = str(post.get('title', ''))[:50]

        self.stats['total_scanned'] += 1

        # Stage 1: Heuristic filter
        if not self.stage1_heuristic_filter(post):
            if self.update_post_skipped(post_id, 'skipped_low_engagement'):
                self.stats['skipped_low_engagement'] += 1
                logger.info(f"[SKIP] {post_id}: Low engagement - '{title}...'")
            else:
                self.stats['errors'] += 1
            return

        # Stage 2: Semantic filter
        is_relevant = self.stage2_semantic_filter(post)

        if is_relevant is None:
            self.stats['errors'] += 1
            return

        if not is_relevant:
            if self.update_post_skipped(post_id, 'skipped_irrelevant'):
                self.stats['skipped_irrelevant'] += 1
                logger.info(f"[NOISE] {post_id}: Irrelevant - '{title}...'")
            else:
                self.stats['errors'] += 1
            return

        # Post passed both filters
        if self.update_post_verified(post_id):
            self.stats['verified_clean'] += 1
            logger.info(f"[CLEAN] {post_id}: Verified - '{title}...'")
        else:
            self.stats['errors'] += 1

    def run(self) -> Dict[str, Any]:
        """Run the cleaning job."""
        logger.info("=" * 50)
        logger.info("Starting Post Cleaning Job")
        logger.info("=" * 50)

        try:
            batch_num = 0
            while True:
                batch_num += 1
                posts = self.scan_unprocessed_posts()

                if not posts:
                    logger.info("No more posts to clean.")
                    break

                for post in posts:
                    self.process_post(post)

                logger.info(f"Batch {batch_num} complete. Clean={self.stats['verified_clean']}, "
                           f"Skipped={self.stats['skipped_low_engagement'] + self.stats['skipped_irrelevant']}")

                time.sleep(1)

        except Exception as e:
            logger.error(f"Error during cleaning: {e}")

        logger.info("=" * 50)
        logger.info("CLEANING JOB COMPLETE")
        logger.info(f"Total scanned: {self.stats['total_scanned']}")
        logger.info(f"Verified clean: {self.stats['verified_clean']}")
        logger.info(f"Skipped (low engagement): {self.stats['skipped_low_engagement']}")
        logger.info(f"Skipped (irrelevant): {self.stats['skipped_irrelevant']}")
        logger.info(f"Errors: {self.stats['errors']}")

        return self.stats


class SentimentAnalyzer:
    """
    Analyzes Reddit posts for market sentiment using Gemini AI.
    Extracts trading signals and stores them in DynamoDB.
    """

    def __init__(self, posts_table, comments_table):
        """Initialize the sentiment analyzer."""
        self.posts_table = posts_table
        self.comments_table = comments_table

        # DynamoDB configuration
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.results_table_name = 'SentimentResults'

        # Gemini configuration (REST API)
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        self.gemini_model = 'gemini-2.0-flash'
        self.gemini_api_url = f'https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent'

        # Processing configuration
        self.batch_size = 500  # Analyze all daily posts
        self.max_comments = 3
        self.gemini_delay = 1.0

        # Statistics
        self.stats = {
            'posts_analyzed': 0,
            'signals_extracted': 0,
            'errors': 0
        }

        # Initialize results table
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
        self.results_table = self.dynamodb.Table(self.results_table_name)

    def scan_clean_posts(self) -> List[Dict]:
        """Scan for clean, unprocessed posts with pagination."""
        posts = []
        try:
            filter_expression = (
                Attr('is_clean').eq(True) &
                (Attr('processed').eq(False) | Attr('processed').not_exists())
            )

            # Paginate through all results until we have enough
            last_key = None
            while len(posts) < self.batch_size:
                scan_kwargs = {'FilterExpression': filter_expression}
                if last_key:
                    scan_kwargs['ExclusiveStartKey'] = last_key

                response = self.posts_table.scan(**scan_kwargs)
                posts.extend(response.get('Items', []))

                last_key = response.get('LastEvaluatedKey')
                if not last_key:
                    break  # No more items

            # Trim to batch_size
            posts = posts[:self.batch_size]
            logger.info(f"Found {len(posts)} clean posts to analyze")
            return posts

        except ClientError as e:
            logger.error(f"Error scanning posts: {e}")
            return []

    def fetch_top_comments(self, post_id: str) -> List[str]:
        """Fetch top comments for a post."""
        try:
            response = self.comments_table.query(
                IndexName='post_id-index',
                KeyConditionExpression='post_id = :pid',
                ExpressionAttributeValues={':pid': post_id},
                Limit=self.max_comments * 2
            )

            comments = response.get('Items', [])
            sorted_comments = sorted(
                comments,
                key=lambda x: int(x.get('score', 0)),
                reverse=True
            )

            top_texts = []
            for comment in sorted_comments[:self.max_comments]:
                body = comment.get('body', '')
                if body and body != '[deleted]' and body != '[removed]':
                    top_texts.append(body[:500])

            return top_texts

        except ClientError as e:
            logger.error(f"Error fetching comments for {post_id}: {e}")
            return []

    def build_analysis_prompt(self, post: Dict, comments: List[str]) -> str:
        """Build the prompt for Gemini analysis."""
        title = str(post.get('title', ''))
        selftext = str(post.get('selftext', ''))[:1000]

        context_parts = [
            f"Title: {title}",
            f"Body: {selftext if selftext else '[No body text]'}"
        ]

        if comments:
            comments_text = "\n".join([f"- {c}" for c in comments])
            context_parts.append(f"Top Comments:\n{comments_text}")

        context = "\n\n".join(context_parts)

        prompt = f"""You are a Senior Hedge Fund Analyst. Analyze this Reddit thread for market sentiment.

IMPORTANT RULES:
1. Focus on CONSENSUS - what does the majority think?
2. Ignore the original poster if comments heavily disagree (the 'ratio' effect)
3. Only extract signals for specific stocks, crypto, or ETFs mentioned
4. Be conservative - only flag clear BUY/SELL/HOLD signals with high confidence
5. If no clear trading signals, return an empty array []

THREAD CONTENT:
{context}

Respond with a JSON array of trading signals. Each signal must have:
- "ticker": Stock/crypto symbol (e.g., "NVDA", "BTC", "SPY")
- "action": One of "BUY", "SELL", or "HOLD"
- "sentiment_score": Float from -1.0 (very bearish) to 1.0 (very bullish)
- "reason": Brief explanation (1-2 sentences)

Example response:
[
  {{"ticker": "NVDA", "action": "BUY", "sentiment_score": 0.8, "reason": "Comments cite strong data center demand despite OP's bearish take."}}
]

If no clear signals, respond with: []

JSON Response:"""

        return prompt

    def analyze_with_gemini(self, prompt: str) -> Optional[List[Dict]]:
        """Call Gemini API to analyze the post."""
        try:
            time.sleep(self.gemini_delay)

            response = requests.post(
                f"{self.gemini_api_url}?key={self.gemini_api_key}",
                headers={'Content-Type': 'application/json'},
                json={
                    'contents': [{'parts': [{'text': prompt}]}],
                    'generationConfig': {
                        'responseMimeType': 'application/json'
                    }
                },
                timeout=60
            )

            if response.status_code == 429:
                logger.warning("Gemini rate limited")
                return None
            elif response.status_code != 200:
                logger.error(f"Gemini API error {response.status_code}: {response.text[:200]}")
                return None

            data = response.json()
            result_text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '[]')

            signals = json.loads(result_text)

            if not isinstance(signals, list):
                logger.warning(f"Invalid response format: {result_text[:100]}")
                return []

            return signals

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Gemini JSON response: {e}")
            return None
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            return None

    def convert_floats_to_decimal(self, obj: Any) -> Any:
        """Recursively convert floats to Decimal for DynamoDB compatibility."""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self.convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_floats_to_decimal(item) for item in obj]
        return obj

    def save_result(self, post_id: str, signals: List[Dict]) -> Optional[str]:
        """Save analysis result to SentimentResults table."""
        try:
            result_id = str(uuid.uuid4())
            signals_decimal = self.convert_floats_to_decimal(signals)

            item = {
                'result_id': result_id,
                'original_post_id': post_id,
                'analyzed_at_utc': datetime.now(timezone.utc).isoformat(),
                'signals': signals_decimal,
                'signal_count': len(signals)
            }

            self.results_table.put_item(Item=item)
            logger.info(f"Saved result {result_id} with {len(signals)} signals")

            return result_id

        except ClientError as e:
            logger.error(f"Failed to save result for {post_id}: {e}")
            return None

    def mark_post_processed(self, post_id: str) -> bool:
        """Mark a post as processed in RedditPosts table."""
        try:
            self.posts_table.update_item(
                Key={'post_id': post_id},
                UpdateExpression='SET #processed = :processed, analyzed_at = :analyzed_at',
                ExpressionAttributeNames={'#processed': 'processed'},
                ExpressionAttributeValues={
                    ':processed': True,
                    ':analyzed_at': datetime.now(timezone.utc).isoformat()
                }
            )
            return True

        except ClientError as e:
            logger.error(f"Failed to mark post {post_id} as processed: {e}")
            return False

    def analyze_post(self, post: Dict) -> bool:
        """Analyze a single post end-to-end."""
        post_id = post.get('post_id', 'unknown')
        title = str(post.get('title', ''))[:50]

        logger.info(f"Analyzing post {post_id}: '{title}...'")

        comments = self.fetch_top_comments(post_id)
        prompt = self.build_analysis_prompt(post, comments)
        signals = self.analyze_with_gemini(prompt)

        if signals is None:
            self.stats['errors'] += 1
            return False

        result_id = self.save_result(post_id, signals)
        if not result_id:
            self.stats['errors'] += 1
            return False

        if not self.mark_post_processed(post_id):
            self.stats['errors'] += 1
            return False

        self.stats['posts_analyzed'] += 1
        self.stats['signals_extracted'] += len(signals)

        if signals:
            for signal in signals:
                logger.info(f"  Signal: {signal.get('ticker')} -> {signal.get('action')} "
                           f"(score: {signal.get('sentiment_score')})")
        else:
            logger.info(f"  No trading signals extracted")

        return True

    def run(self) -> Dict[str, Any]:
        """Main execution method - analyzes a batch of clean posts."""
        logger.info("=" * 50)
        logger.info("Starting Sentiment Analysis")
        logger.info("=" * 50)

        try:
            posts = self.scan_clean_posts()

            if not posts:
                logger.info("No clean posts to analyze")
                return self.stats

            for post in posts:
                self.analyze_post(post)

            logger.info(f"Sentiment analysis complete - Analyzed: {self.stats['posts_analyzed']}, "
                       f"Signals: {self.stats['signals_extracted']}, Errors: {self.stats['errors']}")

            return self.stats

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return self.stats


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class StrategyGenerator:
    """
    Generates daily trading strategy by aggregating sentiment signals
    and comparing with historical data.
    """

    def __init__(self):
        """Initialize the strategy generator."""
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.results_table_name = 'SentimentResults'
        self.strategy_table_name = 'StrategyReports'

        # Gemini configuration
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        self.gemini_model = 'gemini-2.0-flash'
        self.gemini_api_url = f'https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent'

        # Initialize DynamoDB
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
        self.results_table = self.dynamodb.Table(self.results_table_name)
        self.strategy_table = self.dynamodb.Table(self.strategy_table_name)

    def get_today_date(self) -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')

    def get_yesterday_date(self) -> str:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d')

    def fetch_todays_signals(self) -> List[Dict]:
        """Fetch all sentiment results from the last 24 hours."""
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            cutoff_iso = cutoff.isoformat()

            response = self.results_table.scan(
                FilterExpression=Attr('analyzed_at_utc').gte(cutoff_iso)
            )

            items = response.get('Items', [])

            while 'LastEvaluatedKey' in response:
                response = self.results_table.scan(
                    FilterExpression=Attr('analyzed_at_utc').gte(cutoff_iso),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response.get('Items', []))

            logger.info(f"Fetched {len(items)} sentiment results from last 24 hours")
            return items

        except ClientError as e:
            logger.error(f"Error fetching today's signals: {e}")
            return []

    def fetch_yesterdays_strategy(self) -> Optional[Dict]:
        """Fetch yesterday's strategy report for comparison."""
        try:
            yesterday = self.get_yesterday_date()
            response = self.strategy_table.get_item(Key={'report_date': yesterday})
            item = response.get('Item')
            if item:
                logger.info(f"Found yesterday's strategy report ({yesterday})")
            else:
                logger.info(f"No strategy report found for {yesterday}")
            return item

        except ClientError as e:
            logger.error(f"Error fetching yesterday's strategy: {e}")
            return None

    def aggregate_signals(self, results: List[Dict]) -> Dict[str, Dict]:
        """Aggregate signals by ticker, computing consensus metrics."""
        ticker_data = defaultdict(lambda: {
            'signals': [],
            'buy_count': 0,
            'sell_count': 0,
            'hold_count': 0,
            'sentiment_scores': [],
            'reasons': []
        })

        for result in results:
            signals = result.get('signals', [])
            for signal in signals:
                ticker = signal.get('ticker', '').upper()
                if not ticker:
                    continue

                action = signal.get('action', '').upper()
                score = float(signal.get('sentiment_score', 0))
                reason = signal.get('reason', '')

                data = ticker_data[ticker]
                data['signals'].append(signal)
                data['sentiment_scores'].append(score)

                if reason:
                    data['reasons'].append(reason)

                if action == 'BUY':
                    data['buy_count'] += 1
                elif action == 'SELL':
                    data['sell_count'] += 1
                elif action == 'HOLD':
                    data['hold_count'] += 1

        aggregated = {}
        for ticker, data in ticker_data.items():
            total_signals = len(data['signals'])
            if total_signals == 0:
                continue

            avg_sentiment = sum(data['sentiment_scores']) / len(data['sentiment_scores'])

            if data['buy_count'] > data['sell_count'] and data['buy_count'] > data['hold_count']:
                consensus = 'BUY'
            elif data['sell_count'] > data['buy_count'] and data['sell_count'] > data['hold_count']:
                consensus = 'SELL'
            else:
                consensus = 'HOLD'

            aggregated[ticker] = {
                'ticker': ticker,
                'total_mentions': total_signals,
                'buy_count': data['buy_count'],
                'sell_count': data['sell_count'],
                'hold_count': data['hold_count'],
                'avg_sentiment': round(avg_sentiment, 3),
                'consensus': consensus,
                'sample_reasons': data['reasons'][:3]
            }

        logger.info(f"Aggregated signals for {len(aggregated)} tickers")
        return aggregated

    def build_strategy_prompt(self, aggregated: Dict[str, Dict], yesterday_strategy: Optional[Dict]) -> str:
        """Build prompt for Gemini to generate strategic advice."""
        today = self.get_today_date()

        if aggregated:
            today_summary = json.dumps(aggregated, indent=2, cls=DecimalEncoder)
        else:
            today_summary = "No signals collected today."

        if yesterday_strategy:
            yesterday_summary = json.dumps({
                'date': yesterday_strategy.get('report_date'),
                'market_mood': yesterday_strategy.get('market_mood'),
                'short_term_plays': yesterday_strategy.get('short_term_plays', []),
                'long_term_plays': yesterday_strategy.get('long_term_plays', [])
            }, indent=2, cls=DecimalEncoder)
        else:
            yesterday_summary = "No previous strategy report available."

        prompt = f"""You are a Senior Hedge Fund Strategist. Generate a daily trading strategy based on aggregated Reddit sentiment.

TODAY'S DATE: {today}

TODAY'S AGGREGATED SIGNALS:
{today_summary}

YESTERDAY'S STRATEGY (for comparison):
{yesterday_summary}

INSTRUCTIONS:
1. Determine the overall MARKET MOOD based on today's signals:
   - "Risk-On" = Majority bullish, growth/tech favored
   - "Risk-Off" = Majority bearish, defensive posture
   - "Mixed" = No clear direction
   - "Rotation" = Sector rotation happening

2. Generate SHORT-TERM PLAYS (1-5 days horizon):
   - Focus on tickers with strong consensus and high mention count
   - Actions: BUY, SELL, TAKE_PROFIT, ADD_TO_POSITION
   - Consider if sentiment has changed since yesterday

3. Generate LONG-TERM PLAYS (weeks to months):
   - Focus on tickers with consistent sentiment over time
   - Actions: ACCUMULATE, REDUCE, HOLD, AVOID
   - Consider fundamental trends mentioned in reasons

4. Each play must have:
   - "ticker": The symbol
   - "action": The recommended action
   - "reason": 1-2 sentence justification referencing the data

Respond with a JSON object in this exact format:
{{
    "market_mood": "Risk-On|Risk-Off|Mixed|Rotation",
    "market_summary": "1-2 sentence summary of today's sentiment landscape",
    "short_term_plays": [
        {{"ticker": "SYMBOL", "action": "ACTION", "reason": "..."}}
    ],
    "long_term_plays": [
        {{"ticker": "SYMBOL", "action": "ACTION", "reason": "..."}}
    ]
}}

If there's insufficient data, return empty arrays for plays but still assess market_mood as "Mixed".

JSON Response:"""

        return prompt

    def generate_with_gemini(self, prompt: str) -> Optional[Dict]:
        """Call Gemini API to generate strategy."""
        try:
            time.sleep(1.0)

            response = requests.post(
                f"{self.gemini_api_url}?key={self.gemini_api_key}",
                headers={'Content-Type': 'application/json'},
                json={
                    'contents': [{'parts': [{'text': prompt}]}],
                    'generationConfig': {
                        'responseMimeType': 'application/json'
                    }
                },
                timeout=90
            )

            if response.status_code == 429:
                logger.warning("Gemini rate limited")
                return None
            elif response.status_code != 200:
                logger.error(f"Gemini API error {response.status_code}: {response.text[:200]}")
                return None

            data = response.json()
            result_text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '{}')

            strategy = json.loads(result_text)

            required_keys = ['market_mood', 'short_term_plays', 'long_term_plays']
            if not all(k in strategy for k in required_keys):
                logger.warning(f"Invalid strategy format: {result_text[:200]}")
                return None

            return strategy

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Gemini JSON response: {e}")
            return None
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            return None

    def convert_floats_to_decimal(self, obj: Any) -> Any:
        """Recursively convert floats to Decimal for DynamoDB compatibility."""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self.convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_floats_to_decimal(item) for item in obj]
        return obj

    def save_strategy(self, strategy: Dict) -> bool:
        """Save strategy report to DynamoDB."""
        try:
            today = self.get_today_date()
            strategy_decimal = self.convert_floats_to_decimal(strategy)

            item = {
                'report_date': today,
                'generated_at_utc': datetime.now(timezone.utc).isoformat(),
                'market_mood': strategy_decimal.get('market_mood', 'Mixed'),
                'market_summary': strategy_decimal.get('market_summary', ''),
                'short_term_plays': strategy_decimal.get('short_term_plays', []),
                'long_term_plays': strategy_decimal.get('long_term_plays', [])
            }

            self.strategy_table.put_item(Item=item)
            logger.info(f"Saved strategy report for {today}")

            return True

        except ClientError as e:
            logger.error(f"Failed to save strategy: {e}")
            return False

    def run(self) -> Dict[str, Any]:
        """Main execution method - generates daily strategy."""
        logger.info("=" * 50)
        logger.info("Starting Strategy Generation")
        logger.info("=" * 50)

        try:
            results = self.fetch_todays_signals()

            if not results:
                logger.warning("No sentiment results found for today")
                aggregated = {}
            else:
                aggregated = self.aggregate_signals(results)

            yesterday_strategy = self.fetch_yesterdays_strategy()

            prompt = self.build_strategy_prompt(aggregated, yesterday_strategy)
            strategy = self.generate_with_gemini(prompt)

            if not strategy:
                return {'success': False, 'error': 'Failed to generate strategy with Gemini'}

            if not self.save_strategy(strategy):
                return {'success': False, 'error': 'Failed to save strategy'}

            logger.info(f"Strategy generation complete - Mood: {strategy.get('market_mood')}, "
                       f"Short-term: {len(strategy.get('short_term_plays', []))}, "
                       f"Long-term: {len(strategy.get('long_term_plays', []))}")

            return {
                'success': True,
                'strategy': strategy,
                'aggregated_signals': aggregated,
                'aggregated_tickers': len(aggregated),
                'total_results': len(results)
            }

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {'success': False, 'error': str(e)}


class EmailSender:
    """
    Sends daily strategy reports via AWS SES.
    """

    def __init__(self):
        """Initialize the email sender."""
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.recipient = os.getenv('REPORT_EMAIL', 'jasur.shukurov29@gmail.com')
        self.sender = self.recipient  # Same email for sender in sandbox mode
        self.ses_client = boto3.client('ses', region_name=self.region)

    def format_strategy_email(self, strategy: Dict, stats: Dict) -> tuple:
        """
        Format the strategy report as an HTML email.

        Returns:
            Tuple of (subject, html_body, text_body)
        """
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        market_mood = strategy.get('market_mood', 'Unknown')
        market_summary = strategy.get('market_summary', 'No summary available.')
        short_term = strategy.get('short_term_plays', [])
        long_term = strategy.get('long_term_plays', [])

        # Email subject with mood emoji
        mood_emoji = {
            'Risk-On': '🟢',
            'Risk-Off': '🔴',
            'Mixed': '🟡',
            'Rotation': '🔄'
        }.get(market_mood, '📊')

        subject = f"{mood_emoji} Daily Strategy Report - {today} | {market_mood}"

        # Build HTML body
        short_term_html = ""
        if short_term:
            short_term_html = "<ul>"
            for play in short_term:
                action_color = {'BUY': '#28a745', 'SELL': '#dc3545', 'TAKE_PROFIT': '#ffc107', 'ADD_TO_POSITION': '#17a2b8'}.get(play.get('action', ''), '#6c757d')
                short_term_html += f"""
                <li style="margin-bottom: 10px;">
                    <strong style="color: {action_color};">{play.get('ticker', 'N/A')}</strong> → {play.get('action', 'N/A')}
                    <br><span style="color: #666; font-size: 14px;">{play.get('reason', '')}</span>
                </li>"""
            short_term_html += "</ul>"
        else:
            short_term_html = "<p style='color: #666;'>No short-term plays identified today.</p>"

        long_term_html = ""
        if long_term:
            long_term_html = "<ul>"
            for play in long_term:
                action_color = {'ACCUMULATE': '#28a745', 'REDUCE': '#dc3545', 'HOLD': '#ffc107', 'AVOID': '#6c757d'}.get(play.get('action', ''), '#6c757d')
                long_term_html += f"""
                <li style="margin-bottom: 10px;">
                    <strong style="color: {action_color};">{play.get('ticker', 'N/A')}</strong> → {play.get('action', 'N/A')}
                    <br><span style="color: #666; font-size: 14px;">{play.get('reason', '')}</span>
                </li>"""
            long_term_html += "</ul>"
        else:
            long_term_html = "<p style='color: #666;'>No long-term plays identified today.</p>"

        # Stats section
        posts_analyzed = stats.get('sentiment_stats', {}).get('posts_analyzed', 0)
        signals_extracted = stats.get('sentiment_stats', {}).get('signals_extracted', 0)
        posts_fetched = stats.get('ingestion_stats', {}).get('posts_fetched', 0)
        aggregated_signals = stats.get('aggregated_signals', {})

        # Build signals table HTML
        signals_table_html = ""
        if aggregated_signals:
            # Sort by mention count descending
            sorted_signals = sorted(
                aggregated_signals.items(),
                key=lambda x: x[1].get('total_mentions', 0),
                reverse=True
            )

            signals_table_html = """
            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                <thead>
                    <tr style="background: #f8f9fa; text-align: left;">
                        <th style="padding: 8px; border-bottom: 2px solid #667eea;">Ticker</th>
                        <th style="padding: 8px; border-bottom: 2px solid #667eea;">Mentions</th>
                        <th style="padding: 8px; border-bottom: 2px solid #667eea;">Consensus</th>
                        <th style="padding: 8px; border-bottom: 2px solid #667eea;">Sentiment</th>
                    </tr>
                </thead>
                <tbody>
            """
            for ticker, data in sorted_signals[:15]:  # Top 15 signals
                mentions = data.get('total_mentions', 0)
                consensus = data.get('consensus', 'HOLD')
                avg_sentiment = float(data.get('avg_sentiment', 0))

                # Color coding
                if consensus == 'BUY':
                    consensus_color = '#28a745'
                elif consensus == 'SELL':
                    consensus_color = '#dc3545'
                else:
                    consensus_color = '#ffc107'

                # Sentiment bar
                sentiment_pct = int((avg_sentiment + 1) * 50)  # Convert -1 to 1 -> 0 to 100
                sentiment_color = '#28a745' if avg_sentiment > 0.3 else '#dc3545' if avg_sentiment < -0.3 else '#ffc107'

                signals_table_html += f"""
                    <tr style="border-bottom: 1px solid #eee;">
                        <td style="padding: 8px; font-weight: bold;">{ticker}</td>
                        <td style="padding: 8px;">{mentions}x</td>
                        <td style="padding: 8px; color: {consensus_color}; font-weight: bold;">{consensus}</td>
                        <td style="padding: 8px;">
                            <div style="background: #eee; border-radius: 4px; height: 8px; width: 60px; display: inline-block;">
                                <div style="background: {sentiment_color}; height: 8px; width: {sentiment_pct}%; border-radius: 4px;"></div>
                            </div>
                            <span style="font-size: 12px; color: #666;"> {avg_sentiment:+.2f}</span>
                        </td>
                    </tr>
                """
            signals_table_html += "</tbody></table>"
        else:
            signals_table_html = "<p style='color: #666;'>No signals extracted today.</p>"

        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; }}
                .mood {{ font-size: 24px; font-weight: bold; }}
                .summary {{ background: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #667eea; }}
                .section {{ margin-bottom: 25px; }}
                .section-title {{ font-size: 18px; font-weight: bold; color: #333; border-bottom: 2px solid #667eea; padding-bottom: 5px; margin-bottom: 10px; }}
                .stats {{ background: #e9ecef; padding: 10px 15px; border-radius: 8px; font-size: 14px; color: #666; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #999; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="header">
                <div class="mood">{mood_emoji} Market Mood: {market_mood}</div>
                <div style="font-size: 14px; margin-top: 5px;">{today}</div>
            </div>

            <div class="summary">
                <strong>Summary:</strong> {market_summary}
            </div>

            <div class="section">
                <div class="section-title">📊 Key Signals Found ({len(aggregated_signals)} tickers)</div>
                {signals_table_html}
            </div>

            <div class="section">
                <div class="section-title">🎯 Short-Term Plays (1-5 days)</div>
                {short_term_html}
            </div>

            <div class="section">
                <div class="section-title">📈 Long-Term Plays (weeks-months)</div>
                {long_term_html}
            </div>

            <div class="stats">
                <strong>Pipeline Stats:</strong> {posts_fetched} posts scraped → {posts_analyzed} analyzed → {signals_extracted} signals extracted
            </div>

            <div class="footer">
                Generated by Stonks AI Pipeline<br>
                Data sourced from r/stocks, r/investing, r/bitcoin, r/StockMarket, r/TheRaceTo10Million
            </div>
        </body>
        </html>
        """

        # Plain text version
        text_body = f"""
DAILY STRATEGY REPORT - {today}
{'=' * 40}

MARKET MOOD: {market_mood}

SUMMARY:
{market_summary}

KEY SIGNALS FOUND ({len(aggregated_signals)} tickers):
"""
        if aggregated_signals:
            sorted_signals = sorted(
                aggregated_signals.items(),
                key=lambda x: x[1].get('total_mentions', 0),
                reverse=True
            )
            for ticker, data in sorted_signals[:15]:
                mentions = data.get('total_mentions', 0)
                consensus = data.get('consensus', 'HOLD')
                avg_sentiment = float(data.get('avg_sentiment', 0))
                text_body += f"  {ticker}: {mentions}x mentions | {consensus} | sentiment: {avg_sentiment:+.2f}\n"
        else:
            text_body += "  No signals extracted.\n"

        text_body += "\nSHORT-TERM PLAYS (1-5 days):\n"
        if short_term:
            for play in short_term:
                text_body += f"  • {play.get('ticker')} → {play.get('action')}: {play.get('reason')}\n"
        else:
            text_body += "  No short-term plays identified.\n"

        text_body += "\nLONG-TERM PLAYS (weeks-months):\n"
        if long_term:
            for play in long_term:
                text_body += f"  • {play.get('ticker')} → {play.get('action')}: {play.get('reason')}\n"
        else:
            text_body += "  No long-term plays identified.\n"

        text_body += f"""
STATS: {posts_fetched} posts → {posts_analyzed} analyzed → {signals_extracted} signals

---
Generated by Stonks AI Pipeline
"""

        return subject, html_body, text_body

    def send_strategy_email(self, strategy: Dict, stats: Dict) -> bool:
        """
        Send the strategy report email.

        Args:
            strategy: The generated strategy dict.
            stats: Pipeline statistics.

        Returns:
            True if email sent successfully.
        """
        try:
            subject, html_body, text_body = self.format_strategy_email(strategy, stats)

            response = self.ses_client.send_email(
                Source=self.sender,
                Destination={
                    'ToAddresses': [self.recipient]
                },
                Message={
                    'Subject': {
                        'Data': subject,
                        'Charset': 'UTF-8'
                    },
                    'Body': {
                        'Text': {
                            'Data': text_body,
                            'Charset': 'UTF-8'
                        },
                        'Html': {
                            'Data': html_body,
                            'Charset': 'UTF-8'
                        }
                    }
                }
            )

            message_id = response.get('MessageId', 'unknown')
            logger.info(f"Email sent successfully. MessageId: {message_id}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"Failed to send email: {error_code} - {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending email: {e}")
            return False

    def send_failure_email(self, step: str, error: str, attempt: int) -> bool:
        """
        Send a failure notification email.

        Args:
            step: The step that failed.
            error: The error message.
            attempt: Which attempt this was (1 or 2).

        Returns:
            True if email sent successfully.
        """
        try:
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

            subject = f"🔴 Stonks Pipeline FAILED - {step}"

            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <div style="background: #dc3545; color: white; padding: 20px; border-radius: 10px;">
                    <h1 style="margin: 0;">⚠️ Pipeline Failure</h1>
                    <p style="margin: 5px 0 0 0;">{today}</p>
                </div>

                <div style="background: #f8d7da; padding: 15px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #dc3545;">
                    <strong>Failed Step:</strong> {step}<br>
                    <strong>Attempt:</strong> {attempt} of 2<br>
                    <strong>Error:</strong> {error}
                </div>

                <p style="color: #666;">
                    The daily Stonks pipeline failed after {attempt} attempt(s).
                    Please check the CloudWatch logs for more details.
                </p>

                <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #999; text-align: center;">
                    Stonks AI Pipeline - Failure Notification
                </div>
            </body>
            </html>
            """

            text_body = f"""
STONKS PIPELINE FAILURE
=======================

Time: {today}
Failed Step: {step}
Attempt: {attempt} of 2
Error: {error}

Please check CloudWatch logs for details.
"""

            response = self.ses_client.send_email(
                Source=self.sender,
                Destination={'ToAddresses': [self.recipient]},
                Message={
                    'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                    'Body': {
                        'Text': {'Data': text_body, 'Charset': 'UTF-8'},
                        'Html': {'Data': html_body, 'Charset': 'UTF-8'}
                    }
                }
            )

            logger.info(f"Failure notification sent. MessageId: {response.get('MessageId')}")
            return True

        except Exception as e:
            logger.error(f"Failed to send failure notification: {e}")
            return False


def run_pipeline_with_retry(action: str = "full", max_attempts: int = 2, retry_delay: int = 300) -> Dict[str, Any]:
    """
    Run the pipeline with retry logic.

    Args:
        action: Which steps to run:
            - "scrape": Steps 1-2 (scrape posts + clean)
            - "analyze": Steps 3-5 (sentiment + strategy + email)
            - "full": All steps (default)
        max_attempts: Maximum number of attempts (default 2).
        retry_delay: Delay between retries in seconds (default 300 = 5 minutes).

    Returns:
        Dictionary with pipeline results.
    """
    last_error = None
    last_failed_step = None

    # Initialize DynamoDB tables for analyze-only mode
    dynamodb = boto3.resource('dynamodb', region_name=os.getenv('AWS_REGION', 'us-east-1'))
    posts_table = dynamodb.Table('RedditPosts')
    comments_table = dynamodb.Table('RedditComments')

    for attempt in range(1, max_attempts + 1):
        print(f"PIPELINE ATTEMPT {attempt}/{max_attempts} (action={action})")  # Force log
        logger.info(f"{'=' * 60}")
        logger.info(f"PIPELINE ATTEMPT {attempt}/{max_attempts} (action={action})")
        logger.info(f"{'=' * 60}")

        try:
            ingest_result = {'stats': {}}
            clean_stats = {}
            sentiment_stats = {}
            strategy_result = {}

            # Steps 1-2: Scrape and Clean (only if action is "scrape" or "full")
            if action in ["scrape", "full"]:
                # Step 1: Scrape Reddit data
                print(f"STEP 1: Creating scraper (backend: {SCRAPER_BACKEND})")  # Force log
                logger.info(f"STEP 1: Reddit Data Ingestion (backend: {SCRAPER_BACKEND})")

                # Choose scraper based on configuration
                if SCRAPER_BACKEND in ['decodo', 'webshare', 'proxy']:
                    print("Creating ProxyRedditScraper...")  # Force log
                    ingester = ProxyRedditScraper()
                    print("ProxyRedditScraper created successfully")  # Force log
                elif SCRAPER_BACKEND == 'apify':
                    ingester = ApifyRedditScraper()
                else:
                    ingester = RedditDynamoIngester()

                ingest_result = ingester.run()
                posts_table = ingester.posts_table
                comments_table = ingester.comments_table

                if not ingest_result.get('success'):
                    last_failed_step = "Reddit Ingestion"
                    last_error = ingest_result.get('error', 'Unknown ingestion error')
                    raise Exception(last_error)

                # Step 2: Clean the data
                logger.info("STEP 2: Post Cleaning")
                cleaner = PostCleaner(posts_table)
                clean_stats = cleaner.run()

                # If action is "scrape" only, return here
                if action == "scrape":
                    logger.info(f"{'=' * 60}")
                    logger.info("SCRAPE PHASE COMPLETED")
                    logger.info(f"{'=' * 60}")
                    return {
                        'success': True,
                        'action': action,
                        'attempt': attempt,
                        'ingestion_stats': ingest_result.get('stats'),
                        'cleaning_stats': clean_stats
                    }

            # Steps 3-5: Analyze, Strategy, Email (only if action is "analyze" or "full")
            if action in ["analyze", "full"]:
                # Step 3: Analyze sentiment
                logger.info("STEP 3: Sentiment Analysis")
                analyzer = SentimentAnalyzer(posts_table, comments_table)
                sentiment_stats = analyzer.run()

                if sentiment_stats.get('errors', 0) > sentiment_stats.get('posts_analyzed', 0):
                    last_failed_step = "Sentiment Analysis"
                    last_error = f"Too many errors: {sentiment_stats.get('errors')} errors"
                    raise Exception(last_error)

                # Step 4: Generate strategy (if we have signals)
                if sentiment_stats.get('signals_extracted', 0) > 0:
                    logger.info("STEP 4: Strategy Generation")
                    generator = StrategyGenerator()
                    strategy_result = generator.run()

                    if not strategy_result.get('success'):
                        last_failed_step = "Strategy Generation"
                        last_error = strategy_result.get('error', 'Unknown strategy error')
                        raise Exception(last_error)

                    # Step 5: Send email
                    logger.info("STEP 5: Sending Email Report")
                    email_sender = EmailSender()
                    email_stats = {
                        'ingestion_stats': ingest_result.get('stats', {}),
                        'sentiment_stats': sentiment_stats,
                        'aggregated_signals': strategy_result.get('aggregated_signals', {})
                    }
                    email_sent = email_sender.send_strategy_email(
                        strategy_result.get('strategy', {}),
                        email_stats
                    )
                    if not email_sent:
                        logger.warning("Failed to send success email, but pipeline completed")
                else:
                    logger.info("No signals extracted - skipping strategy generation")

            # Success!
            logger.info(f"{'=' * 60}")
            logger.info(f"PIPELINE COMPLETED SUCCESSFULLY (action={action})")
            logger.info(f"{'=' * 60}")

            return {
                'success': True,
                'action': action,
                'attempt': attempt,
                'ingestion_stats': ingest_result.get('stats'),
                'cleaning_stats': clean_stats,
                'sentiment_stats': sentiment_stats,
                'strategy': strategy_result.get('strategy') if strategy_result.get('success') else None
            }

        except Exception as e:
            logger.error(f"Attempt {attempt} failed: {e}")
            last_error = str(e)

            if attempt < max_attempts:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                # Final attempt failed - send failure email
                logger.error("All attempts failed. Sending failure notification...")
                try:
                    email_sender = EmailSender()
                    email_sender.send_failure_email(last_failed_step or "Unknown", last_error, attempt)
                except Exception as email_error:
                    logger.error(f"Could not send failure email: {email_error}")

    return {
        'success': False,
        'action': action,
        'error': last_error,
        'failed_step': last_failed_step,
        'attempts': max_attempts
    }


def lambda_handler(event, context):
    """
    AWS Lambda entry point - runs the full pipeline with retry logic:
    Scrape → Clean → Analyze Sentiment → Generate Strategy → Email

    If any step fails, waits 5 minutes and retries once.
    If still fails, sends a failure notification email.

    Environment variables required:
        - APIFY_API_TOKEN: Your Apify API token (if using Apify backend)
        - BRIGHTDATA_API_TOKEN: Your Bright Data API token (if using Bright Data backend)
        - BRIGHTDATA_ZONE: Your Web Unlocker zone name (default: web_unlocker1)
        - GEMINI_API_KEY: Your Gemini API key for AI processing
        - AWS_REGION: AWS region for DynamoDB (default: us-east-1)
        - REPORT_EMAIL: Email address for reports (default: jasur.shukurov29@gmail.com)

    Configure SCRAPER_BACKEND at top of file to switch between 'apify' and 'brightdata'.

    Returns:
        Lambda response with execution statistics.
    """
    print("Lambda handler started")  # Force immediate log
    logger.info("Lambda handler invoked")

    # Get action from event (default: "full" for backwards compatibility)
    # Actions: "scrape" (steps 1-2), "analyze" (steps 3-5), "full" (all steps)
    action = event.get('action', 'full') if event else 'full'
    logger.info(f"Action: {action}")

    # Run pipeline with retry (2 attempts, 5 min delay)
    result = run_pipeline_with_retry(action=action, max_attempts=2, retry_delay=300)

    if result.get('success'):
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Pipeline completed successfully (action={action})',
                'action': action,
                'attempt': result.get('attempt'),
                'ingestion_stats': result.get('ingestion_stats'),
                'cleaning_stats': result.get('cleaning_stats'),
                'sentiment_stats': result.get('sentiment_stats'),
                'strategy': result.get('strategy')
            }, cls=DecimalEncoder)
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Pipeline failed after all retries (action={action})',
                'action': action,
                'error': result.get('error'),
                'failed_step': result.get('failed_step'),
                'attempts': result.get('attempts')
            })
        }


def main():
    """Main entry point for the script."""
    ingester = RedditDynamoIngester()
    ingester.run()


if __name__ == "__main__":
    main()
