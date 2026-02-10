#!/usr/bin/env python3
"""
Reddit Comments Fetcher Lambda
Fetches comments for posts that meet the criteria:
- Posts with > 7 comments
- Top 25 comments per post
- Ignores moderator comments
- Ignores comments < 10 characters
Uses 50 concurrent workers for fast fetching.
"""

import os
import time
import json
import logging
import requests
import concurrent.futures
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from urllib.parse import quote
from typing import Dict, List, Optional, Any

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MIN_COMMENTS_THRESHOLD = 7  # Only fetch for posts with > 7 comments
MAX_COMMENTS_PER_POST = 25  # Top 25 comments
MIN_COMMENT_LENGTH = 10  # Ignore comments < 10 chars
MAX_CONCURRENT_WORKERS = 50  # Parallel fetches
MODERATOR_AUTHORS = ['AutoModerator', 'moderator', 'mod']  # Authors to ignore


class RedditCommentsFetcher:
    """Fetches comments for Reddit posts using residential proxy."""

    def __init__(self):
        # Proxy configuration
        self.proxy_host = os.getenv('PROXY_HOST', 'gate.decodo.com')
        self.proxy_port = os.getenv('PROXY_PORT', '7000')
        self.proxy_user = os.getenv('PROXY_USER')
        self.proxy_pass = os.getenv('PROXY_PASS')

        if not self.proxy_user or not self.proxy_pass:
            raise ValueError("PROXY_USER and PROXY_PASS environment variables required")

        # Realistic Chrome headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-CH-UA': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
        }

        # DynamoDB
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
        self.posts_table = self.dynamodb.Table('RedditPosts')
        self.comments_table = self.dynamodb.Table('RedditComments')

        # Stats
        self.stats = {
            'posts_processed': 0,
            'comments_fetched': 0,
            'comments_saved': 0,
            'errors': 0
        }

    def get_proxy_url(self):
        """Build proxy URL with encoded credentials."""
        encoded_user = quote(self.proxy_user, safe='')
        encoded_pass = quote(self.proxy_pass, safe='')
        return f"http://{encoded_user}:{encoded_pass}@{self.proxy_host}:{self.proxy_port}"

    def fetch_url(self, url: str, timeout: int = 30) -> Optional[Dict]:
        """Fetch URL through proxy."""
        proxy_url = self.get_proxy_url()
        proxies = {'http': proxy_url, 'https': proxy_url}

        try:
            response = requests.get(url, headers=self.headers, proxies=proxies, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            logger.warning(f"Non-200 response: {response.status_code} for {url[:60]}")
        except Exception as e:
            logger.error(f"Fetch error for {url[:60]}: {str(e)[:50]}")
        return None

    def get_posts_needing_comments(self, hours_back: int = 48, limit: int = 300) -> List[Dict]:
        """Get recent posts that need comments fetched."""
        posts = []

        try:
            # Scan for posts with > MIN_COMMENTS_THRESHOLD comments
            # that haven't had comments fetched yet (attribute doesn't exist or is False)
            filter_expr = (
                Attr('num_comments').gt(MIN_COMMENTS_THRESHOLD) &
                (Attr('comments_fetched').not_exists() | Attr('comments_fetched').eq(False))
            )

            # Paginate through scan results until we have enough posts
            last_key = None
            while len(posts) < limit:
                scan_kwargs = {
                    'FilterExpression': filter_expr,
                    'ProjectionExpression': 'post_id, num_comments, title, #url_attr',
                    'ExpressionAttributeNames': {'#url_attr': 'url'}
                }
                if last_key:
                    scan_kwargs['ExclusiveStartKey'] = last_key

                response = self.posts_table.scan(**scan_kwargs)
                posts.extend(response.get('Items', []))

                last_key = response.get('LastEvaluatedKey')
                if not last_key:
                    break  # No more items to scan

            # Trim to limit
            posts = posts[:limit]
            logger.info(f"Found {len(posts)} posts needing comments (> {MIN_COMMENTS_THRESHOLD} comments)")

        except ClientError as e:
            logger.error(f"DynamoDB scan error: {e}")

        return posts

    def extract_comments(self, data: List, post_id: str) -> List[Dict]:
        """Extract and filter comments from Reddit API response."""
        if not data or len(data) < 2:
            return []

        comments = []
        comment_data = data[1].get('data', {}).get('children', [])

        for item in comment_data[:MAX_COMMENTS_PER_POST]:
            if item.get('kind') != 't1':
                continue

            comment = item.get('data', {})
            author = comment.get('author', '[deleted]')
            body = comment.get('body', '')

            # Skip moderator comments
            if author in MODERATOR_AUTHORS or author.lower().startswith('automoderator'):
                continue

            # Skip short comments
            if len(body.strip()) < MIN_COMMENT_LENGTH:
                continue

            # Skip deleted/removed
            if body in ['[deleted]', '[removed]', '']:
                continue

            comments.append({
                'comment_id': comment.get('name', f"t1_{comment.get('id', '')}"),
                'post_id': post_id,
                'parent_id': comment.get('parent_id', ''),
                'author': author,
                'body': body[:5000],  # Limit body size
                'score': comment.get('score', 0),
                'created_utc': comment.get('created_utc', 0),
                'depth': comment.get('depth', 0),
                'is_submitter': comment.get('is_submitter', False),
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })

        return comments

    def extract_subreddit_from_url(self, url: str) -> str:
        """Extract subreddit name from Reddit URL."""
        # URL format: https://www.reddit.com/r/stocks/comments/xxx/...
        import re
        match = re.search(r'/r/([^/]+)/', url)
        return match.group(1) if match else ''

    def fetch_comments_for_post(self, post: Dict) -> List[Dict]:
        """Fetch comments for a single post."""
        post_id = post.get('post_id', '')
        post_url = post.get('url', '')

        # Extract subreddit from URL
        subreddit = self.extract_subreddit_from_url(post_url)

        if not post_id or not subreddit:
            return []

        # Reddit post ID format: t3_xxxxx -> xxxxx
        short_id = post_id.replace('t3_', '')
        url = f"https://www.reddit.com/r/{subreddit}/comments/{short_id}.json?limit={MAX_COMMENTS_PER_POST}&sort=top"

        data = self.fetch_url(url)
        if not data:
            return []

        return self.extract_comments(data, post_id)

    def save_comments(self, comments: List[Dict]) -> int:
        """Batch save comments to DynamoDB."""
        if not comments:
            return 0

        saved = 0
        try:
            with self.comments_table.batch_writer() as batch:
                for comment in comments:
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
                    saved += 1
        except Exception as e:
            logger.error(f"Error saving comments: {e}")

        return saved

    def mark_post_comments_fetched(self, post_id: str):
        """Mark a post as having had its comments fetched."""
        try:
            self.posts_table.update_item(
                Key={'post_id': post_id},
                UpdateExpression='SET comments_fetched = :val',
                ExpressionAttributeValues={':val': True}
            )
        except Exception as e:
            logger.error(f"Error marking post {post_id}: {e}")

    def run(self) -> Dict[str, Any]:
        """Main execution - fetch comments for eligible posts."""
        logger.info("=" * 60)
        logger.info("REDDIT COMMENTS FETCHER")
        logger.info(f"Config: min_comments={MIN_COMMENTS_THRESHOLD}, max_per_post={MAX_COMMENTS_PER_POST}")
        logger.info(f"Workers: {MAX_CONCURRENT_WORKERS}")
        logger.info("=" * 60)

        # Get posts needing comments
        posts = self.get_posts_needing_comments()
        if not posts:
            logger.info("No posts need comments")
            return {'success': True, 'stats': self.stats}

        logger.info(f"Fetching comments for {len(posts)} posts...")

        # Fetch comments in parallel
        all_comments = []
        post_ids_processed = []

        def fetch_and_extract(post):
            try:
                comments = self.fetch_comments_for_post(post)
                return (post['post_id'], comments)
            except Exception as e:
                logger.error(f"Error processing {post.get('post_id')}: {e}")
                return (post.get('post_id'), [])

        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
            futures = {executor.submit(fetch_and_extract, post): post for post in posts}

            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                post_id, comments = future.result()
                if comments:
                    all_comments.extend(comments)
                    post_ids_processed.append(post_id)
                    self.stats['posts_processed'] += 1
                    self.stats['comments_fetched'] += len(comments)

                if (i + 1) % 20 == 0:
                    logger.info(f"  Progress: {i + 1}/{len(posts)} posts, {len(all_comments)} comments")

        fetch_time = time.time() - start_time
        logger.info(f"Fetched {len(all_comments)} comments in {fetch_time:.1f}s")

        # Save comments in batch
        if all_comments:
            logger.info(f"Saving {len(all_comments)} comments to DynamoDB...")
            self.stats['comments_saved'] = self.save_comments(all_comments)

            # Mark posts as processed
            for post_id in post_ids_processed:
                self.mark_post_comments_fetched(post_id)

        logger.info("=" * 60)
        logger.info("COMPLETE")
        logger.info(f"Posts: {self.stats['posts_processed']}")
        logger.info(f"Comments fetched: {self.stats['comments_fetched']}")
        logger.info(f"Comments saved: {self.stats['comments_saved']}")
        logger.info("=" * 60)

        return {
            'success': True,
            'stats': self.stats,
            'fetch_time': round(fetch_time, 2)
        }


def lambda_handler(event, context):
    """Lambda entry point."""
    logger.info("Comments Fetcher Lambda started")

    try:
        fetcher = RedditCommentsFetcher()
        result = fetcher.run()

        return {
            'statusCode': 200,
            'body': json.dumps(result, default=str)
        }
    except Exception as e:
        logger.error(f"Lambda error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


if __name__ == "__main__":
    # Local testing - set these environment variables before running:
    # PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS, AWS_REGION
    if not os.getenv('PROXY_USER') or not os.getenv('PROXY_PASS'):
        print("Error: Set PROXY_USER and PROXY_PASS environment variables")
        exit(1)

    result = lambda_handler({}, None)
    print(json.dumps(json.loads(result['body']), indent=2))
