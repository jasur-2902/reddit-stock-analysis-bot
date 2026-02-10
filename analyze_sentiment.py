#!/usr/bin/env python3
"""
Sentiment Analysis Script for Reddit Posts

Analyzes cleaned Reddit posts using Gemini AI to extract trading signals.
Stores results in SentimentResults table and marks posts as processed.
"""

import os
import json
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from decimal import Decimal

import requests
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """
    Analyzes Reddit posts for market sentiment using Gemini AI.
    Extracts trading signals and stores them in DynamoDB.
    """

    def __init__(self):
        """Initialize the sentiment analyzer."""
        # DynamoDB configuration
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.posts_table_name = 'RedditPosts'
        self.comments_table_name = 'RedditComments'
        self.results_table_name = 'SentimentResults'

        # Gemini configuration (REST API to avoid grpc issues in Lambda)
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        self.gemini_model = 'gemini-2.0-flash'  # Using flash for speed/cost
        self.gemini_api_url = f'https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent'

        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")

        # Processing configuration
        self.batch_size = 5  # Limit to manage API costs
        self.max_comments = 3  # Top comments to include
        self.gemini_delay = 1.0  # Rate limiting

        # Statistics
        self.stats = {
            'posts_analyzed': 0,
            'signals_extracted': 0,
            'errors': 0
        }

        # Initialize DynamoDB
        self.dynamodb = None
        self.posts_table = None
        self.comments_table = None
        self.results_table = None

    def connect_to_dynamodb(self) -> bool:
        """Establish connection to DynamoDB tables."""
        try:
            logger.info(f"Connecting to DynamoDB in region {self.region}")
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)

            self.posts_table = self.dynamodb.Table(self.posts_table_name)
            self.comments_table = self.dynamodb.Table(self.comments_table_name)
            self.results_table = self.dynamodb.Table(self.results_table_name)

            # Verify tables exist
            for table in [self.posts_table, self.comments_table, self.results_table]:
                _ = table.table_status

            logger.info("Connected to all DynamoDB tables")
            return True

        except ClientError as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            return False

    def scan_clean_posts(self) -> List[Dict]:
        """
        Scan for clean, unprocessed posts.

        Returns:
            List of post items ready for analysis.
        """
        try:
            # Filter: is_clean=True AND processed=False
            filter_expression = (
                Attr('is_clean').eq(True) &
                (Attr('processed').eq(False) | Attr('processed').not_exists())
            )

            response = self.posts_table.scan(
                FilterExpression=filter_expression,
                Limit=self.batch_size
            )

            posts = response.get('Items', [])
            logger.info(f"Found {len(posts)} clean posts to analyze")
            return posts

        except ClientError as e:
            logger.error(f"Error scanning posts: {e}")
            return []

    def fetch_top_comments(self, post_id: str) -> List[str]:
        """
        Fetch top comments for a post from RedditComments table.

        Args:
            post_id: The Reddit post ID.

        Returns:
            List of top comment texts.
        """
        try:
            # Query using the GSI
            response = self.comments_table.query(
                IndexName='post_id-index',
                KeyConditionExpression='post_id = :pid',
                ExpressionAttributeValues={':pid': post_id},
                Limit=self.max_comments * 2  # Fetch extra to filter
            )

            comments = response.get('Items', [])

            # Sort by score (descending) and get top N
            sorted_comments = sorted(
                comments,
                key=lambda x: int(x.get('score', 0)),
                reverse=True
            )

            # Extract comment bodies
            top_texts = []
            for comment in sorted_comments[:self.max_comments]:
                body = comment.get('body', '')
                if body and body != '[deleted]' and body != '[removed]':
                    top_texts.append(body[:500])  # Truncate long comments

            return top_texts

        except ClientError as e:
            logger.error(f"Error fetching comments for {post_id}: {e}")
            return []

    def build_analysis_prompt(self, post: Dict, comments: List[str]) -> str:
        """
        Build the prompt for Gemini analysis.

        Args:
            post: Post data from DynamoDB.
            comments: List of top comment texts.

        Returns:
            Formatted prompt string.
        """
        title = str(post.get('title', ''))
        selftext = str(post.get('selftext', ''))[:1000]  # Truncate long posts

        # Build context
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
        """
        Call Gemini API to analyze the post.

        Args:
            prompt: The analysis prompt.

        Returns:
            List of trading signals or None on error.
        """
        try:
            time.sleep(self.gemini_delay)

            # Gemini REST API request with JSON response
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

            # Parse response
            data = response.json()
            result_text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '[]')

            # Parse the JSON response
            signals = json.loads(result_text)

            # Validate structure
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
        """
        Recursively convert floats to Decimal for DynamoDB compatibility.

        Args:
            obj: Object to convert.

        Returns:
            Converted object.
        """
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self.convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_floats_to_decimal(item) for item in obj]
        return obj

    def save_result(self, post_id: str, signals: List[Dict]) -> Optional[str]:
        """
        Save analysis result to SentimentResults table.

        Args:
            post_id: Original post ID.
            signals: List of trading signals.

        Returns:
            Result ID if successful, None otherwise.
        """
        try:
            result_id = str(uuid.uuid4())

            # Convert floats to Decimal
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
        """
        Mark a post as processed in RedditPosts table.

        Args:
            post_id: The post ID to update.

        Returns:
            True if successful.
        """
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
        """
        Analyze a single post end-to-end.

        Args:
            post: Post item from DynamoDB.

        Returns:
            True if analysis completed successfully.
        """
        post_id = post.get('post_id', 'unknown')
        title = str(post.get('title', ''))[:50]

        logger.info(f"Analyzing post {post_id}: '{title}...'")

        # Fetch top comments
        comments = self.fetch_top_comments(post_id)
        logger.debug(f"Fetched {len(comments)} comments for {post_id}")

        # Build prompt
        prompt = self.build_analysis_prompt(post, comments)

        # Analyze with Gemini
        signals = self.analyze_with_gemini(prompt)

        if signals is None:
            self.stats['errors'] += 1
            return False

        # Save result
        result_id = self.save_result(post_id, signals)
        if not result_id:
            self.stats['errors'] += 1
            return False

        # Mark post as processed
        if not self.mark_post_processed(post_id):
            self.stats['errors'] += 1
            return False

        # Update stats
        self.stats['posts_analyzed'] += 1
        self.stats['signals_extracted'] += len(signals)

        # Log signals found
        if signals:
            for signal in signals:
                logger.info(f"  Signal: {signal.get('ticker')} -> {signal.get('action')} "
                           f"(score: {signal.get('sentiment_score')})")
        else:
            logger.info(f"  No trading signals extracted")

        return True

    def run(self) -> Dict[str, Any]:
        """
        Main execution method - analyzes a batch of clean posts.

        Returns:
            Dictionary with execution statistics.
        """
        logger.info("=" * 60)
        logger.info("Starting Sentiment Analysis Job")
        logger.info("=" * 60)

        if not self.connect_to_dynamodb():
            return {'success': False, 'error': 'DynamoDB connection failed'}

        try:
            # Fetch clean posts
            posts = self.scan_clean_posts()

            if not posts:
                logger.info("No clean posts to analyze")
                return {'success': True, 'stats': self.stats}

            # Analyze each post
            for post in posts:
                self.analyze_post(post)

            # Final summary
            logger.info("=" * 60)
            logger.info("SENTIMENT ANALYSIS COMPLETE")
            logger.info(f"Posts analyzed: {self.stats['posts_analyzed']}")
            logger.info(f"Signals extracted: {self.stats['signals_extracted']}")
            logger.info(f"Errors: {self.stats['errors']}")

            return {'success': True, 'stats': self.stats}

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {'success': False, 'error': str(e)}


def lambda_handler(event, context):
    """AWS Lambda entry point."""
    analyzer = SentimentAnalyzer()
    result = analyzer.run()

    if result.get('success'):
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Sentiment analysis completed',
                'stats': result.get('stats')
            })
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Sentiment analysis failed',
                'error': result.get('error')
            })
        }


def main():
    """Main entry point for local execution."""
    analyzer = SentimentAnalyzer()
    analyzer.run()


if __name__ == "__main__":
    main()
