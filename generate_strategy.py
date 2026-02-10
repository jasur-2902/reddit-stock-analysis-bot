#!/usr/bin/env python3
"""
Strategy Generation Script for Daily Financial Digest

Aggregates today's sentiment signals and compares with yesterday's strategy
to generate actionable trading recommendations using Gemini AI.
Stores results in StrategyReports table.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal
from collections import defaultdict

import requests
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        # DynamoDB configuration
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.results_table_name = 'SentimentResults'
        self.strategy_table_name = 'StrategyReports'

        # Gemini configuration (REST API)
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        self.gemini_model = 'gemini-2.0-flash'  # Using flash for speed/cost
        self.gemini_api_url = f'https://generativelanguage.googleapis.com/v1beta/models/{self.gemini_model}:generateContent'

        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")

        # Initialize DynamoDB
        self.dynamodb = None
        self.results_table = None
        self.strategy_table = None

    def connect_to_dynamodb(self) -> bool:
        """Establish connection to DynamoDB tables."""
        try:
            logger.info(f"Connecting to DynamoDB in region {self.region}")
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region)

            self.results_table = self.dynamodb.Table(self.results_table_name)
            self.strategy_table = self.dynamodb.Table(self.strategy_table_name)

            # Verify tables exist
            for table in [self.results_table, self.strategy_table]:
                _ = table.table_status

            logger.info("Connected to all DynamoDB tables")
            return True

        except ClientError as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            return False

    def get_today_date(self) -> str:
        """Get today's date in YYYY-MM-DD format (UTC)."""
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')

    def get_yesterday_date(self) -> str:
        """Get yesterday's date in YYYY-MM-DD format (UTC)."""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d')

    def fetch_todays_signals(self) -> List[Dict]:
        """
        Fetch all sentiment results from the last 24 hours.

        Returns:
            List of signal records.
        """
        try:
            # Calculate cutoff time (24 hours ago)
            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            cutoff_iso = cutoff.isoformat()

            # Scan for recent results
            response = self.results_table.scan(
                FilterExpression=Attr('analyzed_at_utc').gte(cutoff_iso)
            )

            items = response.get('Items', [])

            # Handle pagination
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
        """
        Fetch yesterday's strategy report for comparison.

        Returns:
            Strategy report dict or None if not found.
        """
        try:
            yesterday = self.get_yesterday_date()

            response = self.strategy_table.get_item(
                Key={'report_date': yesterday}
            )

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
        """
        Aggregate signals by ticker, computing consensus metrics.

        Args:
            results: List of SentimentResults items.

        Returns:
            Dict mapping ticker to aggregated data.
        """
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

        # Calculate summary metrics
        aggregated = {}
        for ticker, data in ticker_data.items():
            total_signals = len(data['signals'])
            if total_signals == 0:
                continue

            avg_sentiment = sum(data['sentiment_scores']) / len(data['sentiment_scores'])

            # Determine consensus action
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
                'sample_reasons': data['reasons'][:3]  # Top 3 reasons
            }

        logger.info(f"Aggregated signals for {len(aggregated)} tickers")
        return aggregated

    def build_strategy_prompt(
        self,
        aggregated: Dict[str, Dict],
        yesterday_strategy: Optional[Dict]
    ) -> str:
        """
        Build prompt for Gemini to generate strategic advice.

        Args:
            aggregated: Aggregated signal data by ticker.
            yesterday_strategy: Previous day's strategy report (if any).

        Returns:
            Formatted prompt string.
        """
        today = self.get_today_date()

        # Format today's data
        if aggregated:
            today_summary = json.dumps(aggregated, indent=2, cls=DecimalEncoder)
        else:
            today_summary = "No signals collected today."

        # Format yesterday's data
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
        """
        Call Gemini API to generate strategy.

        Args:
            prompt: The strategy prompt.

        Returns:
            Strategy dict or None on error.
        """
        try:
            time.sleep(1.0)  # Rate limiting

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

            # Parse response
            data = response.json()
            result_text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '{}')

            # Parse the JSON response
            strategy = json.loads(result_text)

            # Validate structure
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
        """
        Recursively convert floats to Decimal for DynamoDB compatibility.
        """
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self.convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_floats_to_decimal(item) for item in obj]
        return obj

    def save_strategy(self, strategy: Dict) -> bool:
        """
        Save strategy report to DynamoDB.

        Args:
            strategy: The generated strategy.

        Returns:
            True if successful.
        """
        try:
            today = self.get_today_date()

            # Convert floats to Decimal
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
        """
        Main execution method - generates daily strategy.

        Returns:
            Dictionary with execution result and strategy.
        """
        logger.info("=" * 60)
        logger.info("Starting Strategy Generation Job")
        logger.info("=" * 60)

        if not self.connect_to_dynamodb():
            return {'success': False, 'error': 'DynamoDB connection failed'}

        try:
            # Step 1: Fetch today's signals
            results = self.fetch_todays_signals()

            if not results:
                logger.warning("No sentiment results found for today")
                # Still generate a report with empty data
                aggregated = {}
            else:
                # Step 2: Aggregate by ticker
                aggregated = self.aggregate_signals(results)

            # Step 3: Fetch yesterday's strategy
            yesterday_strategy = self.fetch_yesterdays_strategy()

            # Step 4: Generate strategy with Gemini
            prompt = self.build_strategy_prompt(aggregated, yesterday_strategy)
            strategy = self.generate_with_gemini(prompt)

            if not strategy:
                return {'success': False, 'error': 'Failed to generate strategy with Gemini'}

            # Step 5: Save strategy
            if not self.save_strategy(strategy):
                return {'success': False, 'error': 'Failed to save strategy'}

            # Log the strategy
            logger.info("=" * 60)
            logger.info("STRATEGY GENERATION COMPLETE")
            logger.info(f"Date: {self.get_today_date()}")
            logger.info(f"Market Mood: {strategy.get('market_mood')}")
            logger.info(f"Short-term plays: {len(strategy.get('short_term_plays', []))}")
            logger.info(f"Long-term plays: {len(strategy.get('long_term_plays', []))}")

            if strategy.get('market_summary'):
                logger.info(f"Summary: {strategy.get('market_summary')}")

            return {
                'success': True,
                'strategy': strategy,
                'aggregated_tickers': len(aggregated),
                'total_results': len(results)
            }

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {'success': False, 'error': str(e)}


def lambda_handler(event, context):
    """AWS Lambda entry point."""
    generator = StrategyGenerator()
    result = generator.run()

    if result.get('success'):
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Strategy generation completed',
                'strategy': result.get('strategy'),
                'aggregated_tickers': result.get('aggregated_tickers'),
                'total_results': result.get('total_results')
            }, cls=DecimalEncoder)
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Strategy generation failed',
                'error': result.get('error')
            })
        }


def main():
    """Main entry point for local execution."""
    generator = StrategyGenerator()
    result = generator.run()

    if result.get('success'):
        print("\n" + "=" * 60)
        print("DAILY STRATEGY REPORT")
        print("=" * 60)
        print(json.dumps(result.get('strategy'), indent=2, cls=DecimalEncoder))


if __name__ == "__main__":
    main()
