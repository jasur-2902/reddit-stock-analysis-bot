# Reddit Stock Sentiment Analysis Bot

An automated pipeline that scrapes Reddit for financial discussions, analyzes sentiment using AI, and delivers daily trading signals via email.

## Overview

This project monitors popular financial subreddits (r/stocks, r/wallstreetbets, r/investing, etc.), extracts posts and comments, runs AI-powered sentiment analysis, and generates actionable trading signals delivered to your inbox every morning.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         DAILY TRADING SIGNALS                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Market Mood: Risk-On                                                           │
│                                                                                 │
│  BUY:        NVDA, MSFT, META, SMCI, TTWO                                       │
│  SELL:       XOM, CVX, HIMS, PYPL                                               │
│  ACCUMULATE: BTC, VOO, AMZN, GOOGL                                              │
│  AVOID:      TSLA, GOLD                                                         │
│                                                                                 │
│  "Strong bullish sentiment in tech and semiconductors, bearish on energy..."   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Features

- **Automated Scraping**: Fetches 500+ posts daily from 10+ financial subreddits
- **Comment Analysis**: Extracts top 25 comments per post for deeper sentiment context
- **AI-Powered Moderation**: Filters spam, memes, and off-topic content using Gemini
- **Sentiment Analysis**: Analyzes each post/thread for bullish/bearish signals
- **Strategy Generation**: Aggregates signals into actionable BUY/SELL/HOLD recommendations
- **Daily Email Reports**: Beautiful HTML email delivered at 8:30 AM EST
- **Cloudflare Bypass**: Uses residential proxies to avoid rate limiting
- **Serverless Architecture**: Runs entirely on AWS Lambda (cost: ~$3-4/day)

## Architecture

### System Design

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              AWS CLOUD                                          │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                         EventBridge Scheduler                             │  │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                    │  │
│  │  │ 8:00 AM EST │    │ 8:10 AM EST │    │ 8:30 AM EST │                    │  │
│  │  │   SCRAPE    │    │  COMMENTS   │    │   ANALYZE   │                    │  │
│  │  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                    │  │
│  └─────────┼──────────────────┼──────────────────┼───────────────────────────┘  │
│            │                  │                  │                              │
│            ▼                  ▼                  ▼                              │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐                    │
│  │ DailyReddit     │ │ RedditComments  │ │ DailyReddit     │                    │
│  │ Ingest Lambda   │ │ Fetcher Lambda  │ │ Ingest Lambda   │                    │
│  │                 │ │                 │ │                 │                    │
│  │ action="scrape" │ │ 50 concurrent   │ │ action="analyze"│                    │
│  │                 │ │ workers         │ │                 │                    │
│  └────────┬────────┘ └────────┬────────┘ └────────┬────────┘                    │
│           │                   │                   │                             │
│           │    ┌──────────────┴───────────────────┘                             │
│           │    │                                                                │
│           ▼    ▼                                                                │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                        DynamoDB                                 │            │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐  │            │
│  │  │ RedditPosts │ │  Comments   │ │ Sentiment   │ │ Strategy  │  │            │
│  │  │             │ │             │ │ Results     │ │ Reports   │  │            │
│  │  │ ~500/day    │ │ ~3000/day   │ │ ~400/day    │ │ 1/day     │  │            │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘  │            │
│  └─────────────────────────────────────────────────────────────────┘            │
│                                        │                                        │
│                                        ▼                                        │
│                              ┌─────────────────┐                                │
│                              │    AWS SES      │                                │
│                              │  Email Report   │──────────► 📧 Your Inbox       │
│                              └─────────────────┘                                │
└─────────────────────────────────────────────────────────────────────────────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    │                    │                    │
                    ▼                    ▼                    ▼
           ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
           │   Decodo     │     │   Reddit     │     │   Gemini     │
           │   Proxy      │     │   JSON API   │     │   AI API     │
           │ (Residential)│     │              │     │              │
           └──────────────┘     └──────────────┘     └──────────────┘
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                          │
└─────────────────────────────────────────────────────────────────────────────────┘

STEP 1: SCRAPE (8:00 AM EST)
═══════════════════════════════════════════════════════════════════════════════════

  Reddit API                          Lambda                           DynamoDB
 ┌──────────┐                      ┌──────────┐                      ┌──────────┐
 │/r/stocks │  GET .json?limit=100 │          │   BatchWrite         │  Reddit  │
 │/r/wsb    │ ───────────────────► │  Scrape  │ ──────────────────►  │  Posts   │
 │/r/invest │  via Residential     │  + Clean │   500 posts/day      │          │
 │ ...      │  Proxy               │          │                      │          │
 └──────────┘                      └──────────┘                      └──────────┘
                                         │
                                         ▼
                                   ┌──────────┐
                                   │  Gemini  │  "Is this post about
                                   │   API    │   stocks/investing?"
                                   └──────────┘
                                         │
                                         ▼
                                   Mark posts as:
                                   • is_clean = True (relevant)
                                   • is_clean = False (spam/meme)


STEP 2: FETCH COMMENTS (8:10 AM EST)
═══════════════════════════════════════════════════════════════════════════════════

  DynamoDB                           Lambda                           DynamoDB
 ┌──────────┐                      ┌──────────┐                      ┌──────────┐
 │  Posts   │  Scan WHERE          │ Comments │   BatchWrite         │ Comments │
 │  with    │  num_comments > 7    │ Fetcher  │ ──────────────────►  │          │
 │  >7 cmts │ ───────────────────► │          │   ~3000 comments     │          │
 └──────────┘                      │ 50 workers│                      └──────────┘
                                   └──────────┘
                                         │
                                         ▼
                                   Filters applied:
                                   • Top 25 comments per post
                                   • Ignore AutoModerator
                                   • Ignore < 10 characters


STEP 3: ANALYZE & EMAIL (8:30 AM EST)
═══════════════════════════════════════════════════════════════════════════════════

  DynamoDB           Gemini API           DynamoDB           AWS SES
 ┌──────────┐       ┌──────────┐       ┌──────────┐       ┌──────────┐
 │ Posts +  │       │ Analyze  │       │Sentiment │       │  Email   │
 │ Comments │ ────► │ Sentiment│ ────► │ Results  │ ────► │  Report  │ ──► 📧
 └──────────┘       │ Extract  │       │          │       │          │
                    │ Signals  │       └──────────┘       └──────────┘
                    └──────────┘
                          │
                          ▼
                    ┌──────────┐
                    │ Generate │       ┌─────────────────────────────┐
                    │ Strategy │ ────► │ {                           │
                    └──────────┘       │   "market_mood": "Risk-On", │
                                       │   "short_term_plays": [...],│
                                       │   "long_term_plays": [...]  │
                                       │ }                           │
                                       └─────────────────────────────┘
```

### Why This Architecture?

| Design Decision | Why |
|----------------|-----|
| **3-Step Pipeline** | Originally a single Lambda, but 15-minute timeout was hit. Splitting into scrape→comments→analyze allows each step to complete within limits. |
| **Residential Proxy** | Reddit aggressively blocks datacenter IPs. Residential proxies (Decodo/Smartproxy) appear as real users and bypass Cloudflare. |
| **50 Concurrent Workers** | Maximizes throughput while staying under rate limits. Fetches 300 posts' comments in ~40 seconds. |
| **DynamoDB On-Demand** | Bursty workload (500 writes in seconds, then nothing for 24h). On-demand billing is cheaper than provisioned. |
| **Gemini for Analysis** | Cost-effective ($0.70/day for ~2000 comments), good at financial context, JSON output mode for structured signals. |
| **Comments > 7 Threshold** | Posts with few comments lack community signal. High-comment posts show what the crowd actually thinks. |

## Cost Breakdown

Running this pipeline daily costs approximately **$3.86/day** (~$116/month):

| Component | Daily Cost | Monthly Cost | Notes |
|-----------|-----------|--------------|-------|
| AWS Lambda | $0.11 | $3.30 | ~10 min total execution |
| DynamoDB | $0.09 | $2.70 | On-demand, ~4000 writes/day |
| Decodo Proxy | $2.94 | $88.20 | Pay-as-you-go $3.50/GB, ~0.84 GB/day |
| Gemini API | $0.72 | $21.60 | ~500K tokens/day |
| **Total** | **$3.86** | **$115.80** | |

## Project Structure

```
reddit-stock-analysis-bot/
├── package/
│   ├── lambda_function.py      # Main Lambda: scrape, clean, analyze, email
│   └── comments_fetcher.py     # Comments Lambda: parallel comment fetching
├── analyze_sentiment.py        # Standalone sentiment analyzer (dev/testing)
├── clean_posts.py              # Standalone content moderator (dev/testing)
├── generate_strategy.py        # Standalone strategy generator (dev/testing)
├── ingest_dynamo.py            # Standalone ingester (dev/testing)
├── ingest_reddit.py            # Reddit API wrapper (dev/testing)
├── test_cloudflare_bypass.py   # Proxy testing utilities
└── trust-policy.json           # IAM role trust policy
```

## Installation & Deployment

### Prerequisites

- AWS Account with CLI configured
- Python 3.11+
- Decodo/Smartproxy account (residential proxy)
- Google AI API key (Gemini)

### 1. Create DynamoDB Tables

```bash
# RedditPosts table
aws dynamodb create-table \
    --table-name RedditPosts \
    --attribute-definitions AttributeName=post_id,AttributeType=S \
    --key-schema AttributeName=post_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

# RedditComments table
aws dynamodb create-table \
    --table-name RedditComments \
    --attribute-definitions AttributeName=comment_id,AttributeType=S \
    --key-schema AttributeName=comment_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

# SentimentResults table
aws dynamodb create-table \
    --table-name SentimentResults \
    --attribute-definitions AttributeName=result_id,AttributeType=S \
    --key-schema AttributeName=result_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

# StrategyReports table
aws dynamodb create-table \
    --table-name StrategyReports \
    --attribute-definitions AttributeName=report_date,AttributeType=S \
    --key-schema AttributeName=report_date,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

### 2. Create IAM Role

```bash
aws iam create-role \
    --role-name RedditBotRole \
    --assume-role-policy-document file://trust-policy.json

aws iam attach-role-policy \
    --role-name RedditBotRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess

aws iam attach-role-policy \
    --role-name RedditBotRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonSESFullAccess

aws iam attach-role-policy \
    --role-name RedditBotRole \
    --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
```

### 3. Package and Deploy Lambda

```bash
# Install dependencies
cd package
pip install -t . boto3 requests google-generativeai

# Create deployment package
zip -r ../lambda_deployment.zip . -x "*.pyc" -x "__pycache__/*"

# Upload to S3
aws s3 mb s3://your-lambda-bucket
aws s3 cp ../lambda_deployment.zip s3://your-lambda-bucket/

# Create main Lambda
aws lambda create-function \
    --function-name DailyRedditIngest \
    --runtime python3.12 \
    --role arn:aws:iam::YOUR_ACCOUNT:role/RedditBotRole \
    --handler lambda_function.lambda_handler \
    --code S3Bucket=your-lambda-bucket,S3Key=lambda_deployment.zip \
    --timeout 300 \
    --memory-size 256 \
    --environment "Variables={
        PROXY_HOST=gate.decodo.com,
        PROXY_PORT=7000,
        PROXY_USER=your_proxy_user,
        PROXY_PASS=your_proxy_pass,
        GEMINI_API_KEY=your_gemini_key,
        REPORT_EMAIL=your_email@example.com
    }"

# Create comments Lambda
aws lambda create-function \
    --function-name RedditCommentsFetcher \
    --runtime python3.12 \
    --role arn:aws:iam::YOUR_ACCOUNT:role/RedditBotRole \
    --handler comments_fetcher.lambda_handler \
    --code S3Bucket=your-lambda-bucket,S3Key=lambda_deployment.zip \
    --timeout 300 \
    --memory-size 256 \
    --environment "Variables={
        PROXY_HOST=gate.decodo.com,
        PROXY_PORT=7000,
        PROXY_USER=your_proxy_user,
        PROXY_PASS=your_proxy_pass
    }"
```

### 4. Set Up Scheduled Triggers

```bash
# 8:00 AM EST - Scrape posts
aws events put-rule \
    --name DailyRedditTrigger \
    --schedule-expression "cron(0 13 * * ? *)" \
    --state ENABLED

aws events put-targets \
    --rule DailyRedditTrigger \
    --targets '[{
        "Id": "ScrapeTarget",
        "Arn": "arn:aws:lambda:us-east-1:YOUR_ACCOUNT:function:DailyRedditIngest",
        "Input": "{\"action\":\"scrape\"}"
    }]'

# 8:10 AM EST - Fetch comments
aws events put-rule \
    --name RedditCommentsTrigger \
    --schedule-expression "cron(10 13 * * ? *)" \
    --state ENABLED

aws events put-targets \
    --rule RedditCommentsTrigger \
    --targets '[{
        "Id": "CommentsTarget",
        "Arn": "arn:aws:lambda:us-east-1:YOUR_ACCOUNT:function:RedditCommentsFetcher"
    }]'

# 8:30 AM EST - Analyze and email
aws events put-rule \
    --name RedditAnalyzeTrigger \
    --schedule-expression "cron(30 13 * * ? *)" \
    --state ENABLED

aws events put-targets \
    --rule RedditAnalyzeTrigger \
    --targets '[{
        "Id": "AnalyzeTarget",
        "Arn": "arn:aws:lambda:us-east-1:YOUR_ACCOUNT:function:DailyRedditIngest",
        "Input": "{\"action\":\"analyze\"}"
    }]'
```

### 5. Verify SES Email

```bash
aws ses verify-email-identity --email-address your_email@example.com
```

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PROXY_HOST` | Yes | Proxy hostname (e.g., gate.decodo.com) |
| `PROXY_PORT` | Yes | Proxy port (e.g., 7000) |
| `PROXY_USER` | Yes | Proxy username |
| `PROXY_PASS` | Yes | Proxy password |
| `GEMINI_API_KEY` | Yes | Google AI API key |
| `REPORT_EMAIL` | Yes | Email for daily reports |
| `AWS_REGION` | No | AWS region (default: us-east-1) |

### Subreddits Monitored

The bot monitors these subreddits by default (configurable in `lambda_function.py`):

- r/stocks
- r/investing
- r/wallstreetbets
- r/StockMarket
- r/bitcoin
- r/CryptoCurrency
- r/options
- r/theraceto10million
- r/Superstonk
- r/ValueInvesting

### Filtering Rules

| Filter | Threshold | Reason |
|--------|-----------|--------|
| Minimum comments for fetching | > 7 | Posts with few comments lack signal |
| Comments per post | Top 25 | Focus on highest-voted responses |
| Minimum comment length | 10 chars | Filter out low-effort responses |
| Moderator comments | Ignored | AutoModerator adds noise |

## Sample Output

### Daily Email Report

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🟢 DAILY TRADING STRATEGY - February 10, 2026
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MARKET MOOD: Risk-On

The market sentiment is strongly bullish on technology
and semiconductors, with positive signals for AI-related
plays. Energy sector showing weakness.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SHORT-TERM PLAYS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🟢 BUY: NVDA
   Strong data center demand, AI infrastructure growth

🟢 BUY: MSFT
   Azure revenue acceleration, Copilot adoption

🔴 SELL: XOM
   Declining earnings, rotation out of energy

🔴 SELL: HIMS
   FDA restrictions, lawsuit concerns

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LONG-TERM PLAYS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 ACCUMULATE: BTC
   Post-halving appreciation, institutional adoption

📈 ACCUMULATE: VOO
   Consistent index exposure for long-term growth

⚠️ AVOID: TSLA
   Valuation concerns, competitive pressure

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STATS: 500 posts → 340 analyzed → 161 signals
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Sentiment Signal Structure

```json
{
  "ticker": "NVDA",
  "action": "BUY",
  "sentiment_score": 0.85,
  "reason": "Strong consensus on data center growth and AI demand",
  "mentions": 47,
  "avg_sentiment": 0.72
}
```

## Monitoring & Debugging

### CloudWatch Logs

```bash
# View scrape logs
aws logs tail /aws/lambda/DailyRedditIngest --since 1h

# View comments fetcher logs
aws logs tail /aws/lambda/RedditCommentsFetcher --since 1h
```

### Manual Invocation

```bash
# Run scrape step
aws lambda invoke --function-name DailyRedditIngest \
    --payload '{"action":"scrape"}' output.json

# Run comments fetch
aws lambda invoke --function-name RedditCommentsFetcher output.json

# Run analyze step
aws lambda invoke --function-name DailyRedditIngest \
    --payload '{"action":"analyze"}' output.json

# Run full pipeline (all steps)
aws lambda invoke --function-name DailyRedditIngest output.json
```

### Check Database State

```bash
# Count posts
aws dynamodb scan --table-name RedditPosts --select COUNT

# Count comments
aws dynamodb scan --table-name RedditComments --select COUNT

# Count sentiment results
aws dynamodb scan --table-name SentimentResults --select COUNT
```

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| 0 posts scraped | Proxy blocked or credentials wrong | Check PROXY_USER/PROXY_PASS, verify proxy account |
| 0 comments fetched | No posts with >7 comments | Lower threshold or wait for more active posts |
| 0 posts analyzed | DynamoDB scan pagination | Already fixed in latest version |
| Email not received | SES sandbox or unverified email | Verify email in SES, request production access |
| Lambda timeout | Too many posts/comments | Increase timeout or reduce batch size |

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Disclaimer

This tool is for informational and educational purposes only. The trading signals generated are based on Reddit sentiment analysis and should not be considered financial advice. Always do your own research and consult with a qualified financial advisor before making investment decisions.
