#!/usr/bin/env python3
"""
Reddit Data Ingestion Script for AWS DynamoDB
Fetches top daily posts from specified subreddits using public JSON endpoints
and stores them in DynamoDB with deduplication via upsert.
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from decimal import Decimal

import requests
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedditDynamoIngester:
    """
    A class to scrape Reddit posts using public JSON endpoints
    and store them in DynamoDB with deduplication.
    """

    def __init__(self):
        """Initialize the Reddit ingester with configuration."""
        # Target subreddits to scrape
        self.subreddits = ['stocks', 'investing', 'bitcoin']

        # Reddit JSON endpoint configuration
        self.base_url = 'https://www.reddit.com/r/{}/top.json'
        self.time_filter = 'day'

        # User-Agent header to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        }

        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 60

        # DynamoDB configuration
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.table_name = 'RedditPosts'

        # Initialize DynamoDB
        self.dynamodb = None
        self.table = None

    def connect_to_dynamodb(self) -> bool:
        """
        Establish connection to DynamoDB.

        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to DynamoDB in region {self.region}")
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
            self.table = self.dynamodb.Table(self.table_name)

            # Verify table exists by checking its status
            status = self.table.table_status
            logger.info(f"Connected to DynamoDB table: {self.table_name} (status: {status})")
            return True

        except ClientError as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to DynamoDB: {e}")
            return False

    def fetch_subreddit_data(self, subreddit: str) -> Optional[List[Dict]]:
        """
        Fetch top posts from a specific subreddit using the public JSON endpoint.

        Args:
            subreddit: Name of the subreddit to fetch data from.

        Returns:
            List of post dictionaries if successful, None otherwise.
        """
        url = self.base_url.format(subreddit)
        params = {'t': self.time_filter, 'limit': 100}

        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching data from r/{subreddit} (attempt {attempt + 1}/{self.max_retries})")

                response = requests.get(url, headers=self.headers, params=params, timeout=30)

                if response.status_code == 429:
                    logger.warning(f"Rate limited (429) for r/{subreddit}. Waiting {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                    continue

                response.raise_for_status()

                data = response.json()
                posts = data.get('data', {}).get('children', [])

                logger.info(f"Successfully fetched {len(posts)} posts from r/{subreddit}")
                return posts

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for r/{subreddit}: {e}")
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to fetch data from r/{subreddit} after {self.max_retries} attempts")

            except ValueError as e:
                logger.error(f"JSON parsing error for r/{subreddit}: {e}")
                return None

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
            self.table.update_item(
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

    def process_subreddit(self, subreddit: str) -> Dict[str, int]:
        """
        Process all posts from a single subreddit.

        Args:
            subreddit: Name of the subreddit to process.

        Returns:
            Dictionary with processing statistics.
        """
        stats = {'fetched': 0, 'processed': 0, 'errors': 0}

        posts = self.fetch_subreddit_data(subreddit)

        if not posts:
            logger.warning(f"No posts fetched from r/{subreddit}")
            return stats

        stats['fetched'] = len(posts)

        for post in posts:
            if self.upsert_post(post):
                stats['processed'] += 1
            else:
                stats['errors'] += 1

        logger.info(f"r/{subreddit} - Fetched: {stats['fetched']}, "
                    f"Processed: {stats['processed']}, Errors: {stats['errors']}")

        return stats

    def run(self):
        """
        Main execution method - scrapes all configured subreddits.
        """
        logger.info("Starting Reddit data ingestion to DynamoDB...")

        if not self.connect_to_dynamodb():
            logger.error("Failed to connect to DynamoDB. Exiting.")
            return

        total_stats = {'fetched': 0, 'processed': 0, 'errors': 0}

        try:
            for subreddit in self.subreddits:
                logger.info(f"Processing r/{subreddit}...")

                if subreddit != self.subreddits[0]:
                    time.sleep(2)

                stats = self.process_subreddit(subreddit)

                total_stats['fetched'] += stats['fetched']
                total_stats['processed'] += stats['processed']
                total_stats['errors'] += stats['errors']

            logger.info("=" * 50)
            logger.info("Reddit data ingestion completed!")
            logger.info(f"Total posts fetched: {total_stats['fetched']}")
            logger.info(f"Total posts processed: {total_stats['processed']}")
            logger.info(f"Total errors: {total_stats['errors']}")

        except KeyboardInterrupt:
            logger.info("Ingestion interrupted by user.")

        except Exception as e:
            logger.error(f"Unexpected error during ingestion: {e}")


def lambda_handler(event, context):
    """AWS Lambda entry point."""
    ingester = RedditDynamoIngester()
    ingester.run()
    return {
        'statusCode': 200,
        'body': 'Reddit ingestion completed successfully'
    }


def main():
    """Main entry point for the script."""
    ingester = RedditDynamoIngester()
    ingester.run()


if __name__ == "__main__":
    main()
