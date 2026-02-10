#!/usr/bin/env python3
"""
Reddit Data Ingestion Script
Fetches top daily posts from specified subreddits using public JSON endpoints
and stores them in MongoDB with deduplication.
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import requests
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedditScraper:
    """
    A class to scrape Reddit posts using public JSON endpoints
    and store them in MongoDB with deduplication.
    """

    def __init__(self):
        """Initialize the Reddit scraper with configuration."""
        # Target subreddits to scrape
        self.subreddits = ['stocks', 'investing', 'bitcoin']

        # Reddit JSON endpoint configuration
        self.base_url = 'https://www.reddit.com/r/{}/top.json'
        self.time_filter = 'day'  # Top posts of the day

        # User-Agent header to mimic a real browser (prevents 429 errors)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        }

        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 60  # seconds to wait on 429 error

        # MongoDB configuration
        self.mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        self.db_name = 'reddit_digest'
        self.collection_name = 'raw_posts'

        # Initialize MongoDB connection
        self.mongo_client = None
        self.db = None
        self.collection = None

    def connect_to_mongodb(self) -> bool:
        """
        Establish connection to MongoDB.

        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to MongoDB at {self.mongo_uri}")
            self.mongo_client = MongoClient(self.mongo_uri)

            # Test the connection
            self.mongo_client.admin.command('ping')

            # Get database and collection
            self.db = self.mongo_client[self.db_name]
            self.collection = self.db[self.collection_name]

            logger.info(f"Successfully connected to MongoDB database: {self.db_name}")
            return True

        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {e}")
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
        params = {'t': self.time_filter, 'limit': 100}  # Fetch up to 100 posts

        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching data from r/{subreddit} (attempt {attempt + 1}/{self.max_retries})")

                # Make the request with custom headers
                response = requests.get(url, headers=self.headers, params=params, timeout=30)

                # Check for rate limiting
                if response.status_code == 429:
                    logger.warning(f"Rate limited (429) for r/{subreddit}. Waiting {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                    continue

                # Check for successful response
                response.raise_for_status()

                # Parse JSON response
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

    def extract_post_data(self, post: Dict) -> Dict[str, Any]:
        """
        Extract relevant fields from a Reddit post.

        Args:
            post: Raw Reddit post data from the API.

        Returns:
            Dictionary with extracted and formatted post data.
        """
        post_data = post.get('data', {})

        # Extract the Reddit ID (e.g., 't3_xyz')
        reddit_id = post_data.get('name', '')

        # Build the post URL
        permalink = post_data.get('permalink', '')
        post_url = f"https://www.reddit.com{permalink}" if permalink else ''

        # Extract and format the data according to schema
        extracted_data = {
            '_id': reddit_id,  # Use Reddit ID as MongoDB _id for uniqueness
            'title': post_data.get('title', ''),
            'selftext': post_data.get('selftext', ''),  # Body text (empty for link posts)
            'score': post_data.get('score', 0),  # Upvote count
            'num_comments': post_data.get('num_comments', 0),
            'url': post_url,
            'created_utc': post_data.get('created_utc', 0),  # Unix timestamp
            'processed': False,  # Default to unprocessed
            'scraped_at': datetime.now(timezone.utc),  # Current UTC timestamp
            'subreddit': post_data.get('subreddit', ''),  # Also store subreddit for reference
            'author': post_data.get('author', ''),  # Store author for reference
        }

        return extracted_data

    def upsert_post(self, post_data: Dict) -> bool:
        """
        Upsert a post into MongoDB (update if exists, insert if new).
        Implements deduplication strategy based on Reddit ID.

        Args:
            post_data: Extracted post data to upsert.

        Returns:
            bool: True if operation successful, False otherwise.
        """
        try:
            reddit_id = post_data['_id']

            # Check if post already exists
            existing_post = self.collection.find_one({'_id': reddit_id})

            if existing_post:
                # Post exists - check if content changed significantly
                content_changed = (
                    existing_post.get('title') != post_data['title'] or
                    existing_post.get('selftext') != post_data['selftext'] or
                    abs(existing_post.get('score', 0) - post_data['score']) > 100  # Significant score change
                )

                # Preserve the existing 'processed' value unless content changed
                if not content_changed and 'processed' in existing_post:
                    post_data['processed'] = existing_post['processed']
                else:
                    post_data['processed'] = False  # Reset if content changed

                # Update the scraped_at timestamp
                post_data['scraped_at'] = datetime.now(timezone.utc)

            # Perform upsert operation
            result = self.collection.update_one(
                {'_id': reddit_id},
                {'$set': post_data},
                upsert=True
            )

            if result.upserted_id:
                logger.debug(f"Inserted new post: {reddit_id}")
            elif result.modified_count > 0:
                logger.debug(f"Updated existing post: {reddit_id}")
            else:
                logger.debug(f"Post unchanged: {reddit_id}")

            return True

        except OperationFailure as e:
            logger.error(f"MongoDB operation failed for post {post_data.get('_id')}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error upserting post {post_data.get('_id')}: {e}")
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

        # Fetch posts from subreddit
        posts = self.fetch_subreddit_data(subreddit)

        if not posts:
            logger.warning(f"No posts fetched from r/{subreddit}")
            return stats

        stats['fetched'] = len(posts)

        # Process each post
        for post in posts:
            try:
                # Extract post data
                post_data = self.extract_post_data(post)

                # Upsert to MongoDB
                if self.upsert_post(post_data):
                    stats['processed'] += 1
                else:
                    stats['errors'] += 1

            except Exception as e:
                logger.error(f"Error processing post from r/{subreddit}: {e}")
                stats['errors'] += 1

        logger.info(f"r/{subreddit} - Fetched: {stats['fetched']}, "
                   f"Processed: {stats['processed']}, Errors: {stats['errors']}")

        return stats

    def run(self):
        """
        Main execution method - scrapes all configured subreddits.
        """
        logger.info("Starting Reddit data ingestion...")

        # Connect to MongoDB
        if not self.connect_to_mongodb():
            logger.error("Failed to connect to MongoDB. Exiting.")
            return

        total_stats = {'fetched': 0, 'processed': 0, 'errors': 0}

        try:
            # Process each subreddit
            for subreddit in self.subreddits:
                logger.info(f"Processing r/{subreddit}...")

                # Add delay between subreddits to be respectful
                if subreddit != self.subreddits[0]:
                    time.sleep(2)

                # Process the subreddit
                stats = self.process_subreddit(subreddit)

                # Update total statistics
                total_stats['fetched'] += stats['fetched']
                total_stats['processed'] += stats['processed']
                total_stats['errors'] += stats['errors']

            # Log summary
            logger.info("=" * 50)
            logger.info("Reddit data ingestion completed!")
            logger.info(f"Total posts fetched: {total_stats['fetched']}")
            logger.info(f"Total posts processed: {total_stats['processed']}")
            logger.info(f"Total errors: {total_stats['errors']}")

        except KeyboardInterrupt:
            logger.info("Ingestion interrupted by user.")

        except Exception as e:
            logger.error(f"Unexpected error during ingestion: {e}")

        finally:
            # Close MongoDB connection
            if self.mongo_client:
                self.mongo_client.close()
                logger.info("MongoDB connection closed.")


def main():
    """Main entry point for the script."""
    scraper = RedditScraper()
    scraper.run()


if __name__ == "__main__":
    main()