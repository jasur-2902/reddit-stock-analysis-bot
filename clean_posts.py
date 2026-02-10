#!/usr/bin/env python3
"""
Reddit Posts Data Cleaning Script

Filters noise from RedditPosts DynamoDB table using:
1. Heuristic filter (engagement threshold)
2. Semantic filter (Gemini Flash for relevance classification)

Marks posts as:
- is_clean=True, cleaning_status="verified" -> Ready for analysis
- processed=True, cleaning_status="skipped_*" -> Discarded
"""

import os
import time
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
import google.generativeai as genai

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostCleaner:
    """
    Cleans Reddit posts by filtering low-quality content using
    heuristics and AI-based semantic analysis.
    """

    def __init__(self):
        """Initialize the post cleaner with configuration."""
        # DynamoDB configuration
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.table_name = 'RedditPosts'

        # Heuristic thresholds
        self.min_comments_threshold = 5  # Minimum comments for engagement

        # Gemini configuration
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")

        # Initialize Gemini
        genai.configure(api_key=self.gemini_api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash-lite')  # Lite model for higher quota

        # Processing configuration
        self.batch_size = 10
        self.max_text_length = 500  # Truncate text for Gemini

        # Rate limiting for Gemini API
        self.gemini_delay = 0.5  # Seconds between Gemini calls

        # Statistics
        self.stats = {
            'total_scanned': 0,
            'skipped_low_engagement': 0,
            'skipped_irrelevant': 0,
            'verified_clean': 0,
            'errors': 0
        }

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
            logger.info(f"Connecting to DynamoDB table: {self.table_name}")
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
            self.table = self.dynamodb.Table(self.table_name)

            # Verify table exists
            status = self.table.table_status
            logger.info(f"Connected to DynamoDB table (status: {status})")
            return True

        except ClientError as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            return False

    def scan_unprocessed_posts(self) -> List[Dict]:
        """
        Scan for posts that need cleaning.

        Conditions:
        - processed is False (or doesn't exist)
        - cleaning_status does NOT exist

        Returns:
            List of post items to process.
        """
        posts = []

        try:
            # Build filter expression:
            # (processed = False OR attribute_not_exists(processed))
            # AND attribute_not_exists(cleaning_status)
            filter_expression = (
                (Attr('processed').eq(False) | Attr('processed').not_exists()) &
                Attr('cleaning_status').not_exists()
            )

            # Paginate through results
            response = self.table.scan(
                FilterExpression=filter_expression,
                Limit=self.batch_size
            )

            posts.extend(response.get('Items', []))

            # Handle pagination if needed (for larger datasets)
            while 'LastEvaluatedKey' in response and len(posts) < self.batch_size:
                response = self.table.scan(
                    FilterExpression=filter_expression,
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    Limit=self.batch_size - len(posts)
                )
                posts.extend(response.get('Items', []))

            logger.info(f"Found {len(posts)} posts to process")
            return posts[:self.batch_size]  # Ensure we don't exceed batch size

        except ClientError as e:
            logger.error(f"Error scanning DynamoDB: {e}")
            return []

    def stage1_heuristic_filter(self, post: Dict) -> bool:
        """
        Stage 1: Heuristic filter based on engagement.

        Args:
            post: Post item from DynamoDB.

        Returns:
            bool: True if post passes filter, False if discarded.
        """
        post_id = post.get('post_id', 'unknown')
        num_comments = int(post.get('num_comments', 0))

        if num_comments < self.min_comments_threshold:
            logger.debug(f"Post {post_id}: Low engagement ({num_comments} comments) - SKIPPED")
            return False

        logger.debug(f"Post {post_id}: Passed heuristic filter ({num_comments} comments)")
        return True

    def stage2_semantic_filter(self, post: Dict) -> Optional[bool]:
        """
        Stage 2: Semantic filter using Gemini Flash.

        Args:
            post: Post item from DynamoDB.

        Returns:
            True if relevant, False if noise, None on error.
        """
        post_id = post.get('post_id', 'unknown')
        title = post.get('title', '')
        selftext = post.get('selftext', '')

        # Truncate text to avoid token limits
        content = f"{title}\n\n{selftext}"[:self.max_text_length]

        # Build prompt for Gemini
        prompt = f"""You are a content moderator for a financial data feed. Analyze this Reddit post.

Answer strictly 'RELEVANT' if it discusses stocks, crypto, economics, investing, trading strategies, market analysis, or financial news.

Answer 'NOISE' if it is a meme, shitpost, purely political (not economic policy), personal rant without financial content, or spam.

Post Title: {title}

Post Content: {selftext[:self.max_text_length] if selftext else '[No content]'}

Your answer (RELEVANT or NOISE):"""

        try:
            # Rate limiting
            time.sleep(self.gemini_delay)

            # Call Gemini Flash
            response = self.model.generate_content(prompt)
            result = response.text.strip().upper()

            # Parse response
            if 'RELEVANT' in result:
                logger.debug(f"Post {post_id}: Gemini classified as RELEVANT")
                return True
            elif 'NOISE' in result:
                logger.debug(f"Post {post_id}: Gemini classified as NOISE")
                return False
            else:
                # Ambiguous response - default to relevant to avoid losing good data
                logger.warning(f"Post {post_id}: Ambiguous Gemini response '{result}' - defaulting to RELEVANT")
                return True

        except Exception as e:
            logger.error(f"Gemini API error for post {post_id}: {e}")
            return None

    def update_post_skipped(self, post_id: str, reason: str) -> bool:
        """
        Mark a post as skipped/discarded.

        Args:
            post_id: The post ID to update.
            reason: The skip reason (e.g., "skipped_low_engagement").

        Returns:
            bool: True if update successful.
        """
        try:
            self.table.update_item(
                Key={'post_id': post_id},
                UpdateExpression='SET #processed = :processed, cleaning_status = :status, cleaned_at = :cleaned_at',
                ExpressionAttributeNames={
                    '#processed': 'processed'  # 'processed' is a reserved keyword
                },
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
        """
        Mark a post as clean and ready for analysis.

        Note: processed remains False so analysis script can find it.

        Args:
            post_id: The post ID to update.

        Returns:
            bool: True if update successful.
        """
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
        """
        Process a single post through both filtering stages.

        Args:
            post: Post item from DynamoDB.
        """
        post_id = post.get('post_id', 'unknown')
        title = post.get('title', '')[:50]  # Truncate for logging

        self.stats['total_scanned'] += 1

        # Stage 1: Heuristic filter
        if not self.stage1_heuristic_filter(post):
            if self.update_post_skipped(post_id, 'skipped_low_engagement'):
                self.stats['skipped_low_engagement'] += 1
                logger.info(f"[SKIP] {post_id}: Low engagement - '{title}...'")
            else:
                self.stats['errors'] += 1
            return

        # Stage 2: Semantic filter (only if Stage 1 passed)
        is_relevant = self.stage2_semantic_filter(post)

        if is_relevant is None:
            # Gemini error - skip for now, will retry next run
            self.stats['errors'] += 1
            logger.warning(f"[ERROR] {post_id}: Gemini API error - will retry")
            return

        if not is_relevant:
            if self.update_post_skipped(post_id, 'skipped_irrelevant'):
                self.stats['skipped_irrelevant'] += 1
                logger.info(f"[NOISE] {post_id}: Irrelevant content - '{title}...'")
            else:
                self.stats['errors'] += 1
            return

        # Post passed both filters - mark as clean
        if self.update_post_verified(post_id):
            self.stats['verified_clean'] += 1
            logger.info(f"[CLEAN] {post_id}: Verified relevant - '{title}...'")
        else:
            self.stats['errors'] += 1

    def run(self) -> Dict[str, int]:
        """
        Main execution method - processes posts in batches.

        Returns:
            Dictionary with processing statistics.
        """
        logger.info("=" * 60)
        logger.info("Starting Reddit Posts Cleaning Job")
        logger.info("=" * 60)

        # Connect to DynamoDB
        if not self.connect_to_dynamodb():
            logger.error("Failed to connect to DynamoDB. Exiting.")
            return self.stats

        try:
            # Process posts in batches until none remain
            batch_num = 0

            while True:
                batch_num += 1
                logger.info(f"\n--- Processing Batch {batch_num} ---")

                # Fetch batch of unprocessed posts
                posts = self.scan_unprocessed_posts()

                if not posts:
                    logger.info("No more posts to process.")
                    break

                # Process each post in the batch
                for post in posts:
                    self.process_post(post)

                # Log batch progress
                logger.info(f"Batch {batch_num} complete. Running totals: "
                           f"Clean={self.stats['verified_clean']}, "
                           f"Skipped={self.stats['skipped_low_engagement'] + self.stats['skipped_irrelevant']}, "
                           f"Errors={self.stats['errors']}")

                # Small delay between batches
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("\nCleaning job interrupted by user.")

        except Exception as e:
            logger.error(f"Unexpected error during cleaning: {e}")

        # Final summary
        self._log_summary()
        return self.stats

    def _log_summary(self) -> None:
        """Log final cleaning summary."""
        logger.info("\n" + "=" * 60)
        logger.info("CLEANING JOB COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total posts scanned:      {self.stats['total_scanned']}")
        logger.info(f"Verified clean:           {self.stats['verified_clean']}")
        logger.info(f"Skipped (low engagement): {self.stats['skipped_low_engagement']}")
        logger.info(f"Skipped (irrelevant):     {self.stats['skipped_irrelevant']}")
        logger.info(f"Errors:                   {self.stats['errors']}")

        total_skipped = self.stats['skipped_low_engagement'] + self.stats['skipped_irrelevant']
        if self.stats['total_scanned'] > 0:
            clean_rate = (self.stats['verified_clean'] / self.stats['total_scanned']) * 100
            skip_rate = (total_skipped / self.stats['total_scanned']) * 100
            logger.info(f"Clean rate:               {clean_rate:.1f}%")
            logger.info(f"Skip rate:                {skip_rate:.1f}%")


def main():
    """Main entry point."""
    try:
        cleaner = PostCleaner()
        cleaner.run()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
