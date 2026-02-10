#!/usr/bin/env python3
"""
Test script to find the best approach for bypassing Cloudflare on Reddit comment endpoints.

Tests:
1. Sticky sessions (same IP for all requests)
2. Rotating IPs (new IP per request)
3. More realistic browser headers
4. Parallel requests with multiple IPs
"""

import os
import time
import requests
import concurrent.futures
from urllib.parse import quote

# Load proxy credentials from environment (required)
PROXY_HOST = os.getenv('PROXY_HOST', 'gate.decodo.com')
PROXY_PORT = os.getenv('PROXY_PORT', '7000')
PROXY_USER = os.getenv('PROXY_USER')
PROXY_PASS = os.getenv('PROXY_PASS')

if not PROXY_USER or not PROXY_PASS:
    raise ValueError("PROXY_USER and PROXY_PASS environment variables are required")

# Test URLs - a few recent posts with comments
TEST_URLS = [
    'https://www.reddit.com/r/stocks/new.json?limit=5',  # Get post IDs first
]

# Basic headers (current approach)
BASIC_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# Realistic Chrome headers (more complete fingerprint)
REALISTIC_HEADERS = {
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


def get_proxy_url(sticky_session=None):
    """
    Build proxy URL with optional sticky session.

    Decodo/Smartproxy sticky session format:
    - Add session ID to username: user-session-XXXX
    """
    encoded_user = quote(PROXY_USER, safe='')
    encoded_pass = quote(PROXY_PASS, safe='')

    if sticky_session:
        # Sticky session - same IP for duration
        encoded_user = quote(f"{PROXY_USER}-session-{sticky_session}", safe='')

    return f"http://{encoded_user}:{encoded_pass}@{PROXY_HOST}:{PROXY_PORT}"


def fetch_with_config(url, headers, proxy_url, timeout=30):
    """Fetch URL with given configuration, return timing and status."""
    proxies = {'http': proxy_url, 'https': proxy_url}

    start = time.time()
    try:
        response = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
        elapsed = time.time() - start
        return {
            'success': response.status_code == 200,
            'status_code': response.status_code,
            'elapsed': elapsed,
            'size': len(response.content),
            'error': None
        }
    except Exception as e:
        elapsed = time.time() - start
        return {
            'success': False,
            'status_code': None,
            'elapsed': elapsed,
            'size': 0,
            'error': str(e)
        }


def get_comment_urls():
    """Fetch a few post IDs to test comment endpoints."""
    print("\n📋 Fetching post IDs for testing...")
    proxy_url = get_proxy_url()

    result = fetch_with_config(
        'https://www.reddit.com/r/stocks/new.json?limit=10',
        REALISTIC_HEADERS,
        proxy_url
    )

    if not result['success']:
        print(f"   ❌ Failed to get posts: {result['error'] or result['status_code']}")
        # Fallback to known post IDs
        return [
            'https://www.reddit.com/r/stocks/comments/1r066es.json?limit=50',
            'https://www.reddit.com/r/stocks/comments/1r062gi.json?limit=50',
            'https://www.reddit.com/r/stocks/comments/1r0023k.json?limit=50',
        ]

    import json
    data = requests.get(
        'https://www.reddit.com/r/stocks/new.json?limit=10',
        headers=REALISTIC_HEADERS,
        proxies={'http': proxy_url, 'https': proxy_url},
        timeout=30
    ).json()

    urls = []
    for post in data.get('data', {}).get('children', [])[:5]:
        post_id = post['data']['id']
        urls.append(f"https://www.reddit.com/r/stocks/comments/{post_id}.json?limit=50")

    print(f"   ✅ Got {len(urls)} comment URLs to test")
    return urls


def test_basic_vs_realistic_headers(comment_urls):
    """Test #4: Compare basic headers vs realistic Chrome headers."""
    print("\n" + "="*60)
    print("TEST #4: Basic Headers vs Realistic Chrome Headers")
    print("="*60)

    proxy_url = get_proxy_url()
    url = comment_urls[0]

    print(f"\nTesting URL: {url[:60]}...")

    # Test with basic headers
    print("\n  [Basic Headers]")
    result = fetch_with_config(url, BASIC_HEADERS, proxy_url)
    print(f"    Status: {result['status_code']}, Time: {result['elapsed']:.2f}s, Size: {result['size']} bytes")
    if result['error']:
        print(f"    Error: {result['error']}")

    time.sleep(2)  # Small delay between tests

    # Test with realistic headers
    print("\n  [Realistic Chrome Headers]")
    result = fetch_with_config(url, REALISTIC_HEADERS, proxy_url)
    print(f"    Status: {result['status_code']}, Time: {result['elapsed']:.2f}s, Size: {result['size']} bytes")
    if result['error']:
        print(f"    Error: {result['error']}")


def test_sticky_vs_rotating(comment_urls):
    """Test #3: Compare sticky sessions vs rotating IPs."""
    print("\n" + "="*60)
    print("TEST #3: Sticky Session vs Rotating IPs")
    print("="*60)

    # Test rotating IPs (default behavior)
    print("\n  [Rotating IPs - 3 sequential requests]")
    proxy_url = get_proxy_url()  # No sticky session

    for i, url in enumerate(comment_urls[:3]):
        result = fetch_with_config(url, REALISTIC_HEADERS, proxy_url)
        print(f"    Request {i+1}: Status={result['status_code']}, Time={result['elapsed']:.2f}s")
        if result['error']:
            print(f"      Error: {result['error']}")
        time.sleep(1)

    time.sleep(3)  # Pause between tests

    # Test sticky session (same IP)
    print("\n  [Sticky Session - 3 sequential requests, same IP]")
    session_id = f"test{int(time.time())}"
    proxy_url = get_proxy_url(sticky_session=session_id)

    for i, url in enumerate(comment_urls[:3]):
        result = fetch_with_config(url, REALISTIC_HEADERS, proxy_url)
        print(f"    Request {i+1}: Status={result['status_code']}, Time={result['elapsed']:.2f}s")
        if result['error']:
            print(f"      Error: {result['error']}")
        time.sleep(1)


def test_parallel_requests(comment_urls):
    """Test #5: Parallel requests with different IPs."""
    print("\n" + "="*60)
    print("TEST #5: Parallel Requests with Multiple IPs")
    print("="*60)

    urls_to_test = comment_urls[:5]

    # Sequential baseline
    print(f"\n  [Sequential - {len(urls_to_test)} requests]")
    start = time.time()
    results = []
    for url in urls_to_test:
        proxy_url = get_proxy_url()  # Each gets potentially different IP
        result = fetch_with_config(url, REALISTIC_HEADERS, proxy_url)
        results.append(result)
        time.sleep(0.5)

    total_time = time.time() - start
    successes = sum(1 for r in results if r['success'])
    print(f"    Total time: {total_time:.2f}s, Success: {successes}/{len(results)}")
    for i, r in enumerate(results):
        print(f"      Request {i+1}: {r['status_code']} in {r['elapsed']:.2f}s")

    time.sleep(5)  # Pause between tests

    # Parallel with different IPs (no sticky session - let proxy rotate)
    print(f"\n  [Parallel - {len(urls_to_test)} concurrent requests, rotating IPs]")

    def fetch_parallel(args):
        url, idx = args
        # Plain rotating IPs - no session parameter
        proxy_url = get_proxy_url()  # No sticky session
        return fetch_with_config(url, REALISTIC_HEADERS, proxy_url)

    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(fetch_parallel, [(url, i) for i, url in enumerate(urls_to_test)]))

    total_time = time.time() - start
    successes = sum(1 for r in results if r['success'])
    print(f"    Total time: {total_time:.2f}s, Success: {successes}/{len(results)}")
    for i, r in enumerate(results):
        print(f"      Request {i+1}: {r['status_code']} in {r['elapsed']:.2f}s")


def test_old_reddit():
    """Bonus test: Try old.reddit.com subdomain."""
    print("\n" + "="*60)
    print("BONUS: Testing old.reddit.com")
    print("="*60)

    proxy_url = get_proxy_url()

    # Test new reddit
    print("\n  [www.reddit.com]")
    url = 'https://www.reddit.com/r/stocks/new.json?limit=5'
    result = fetch_with_config(url, REALISTIC_HEADERS, proxy_url)
    print(f"    Status: {result['status_code']}, Time: {result['elapsed']:.2f}s")

    time.sleep(2)

    # Test old reddit
    print("\n  [old.reddit.com]")
    url = 'https://old.reddit.com/r/stocks/new.json?limit=5'
    result = fetch_with_config(url, REALISTIC_HEADERS, proxy_url)
    print(f"    Status: {result['status_code']}, Time: {result['elapsed']:.2f}s")


def main():
    print("🔬 Cloudflare Bypass Testing")
    print("="*60)
    print(f"Proxy: {PROXY_HOST}:{PROXY_PORT}")
    print(f"User: {PROXY_USER}")

    # Get comment URLs to test
    comment_urls = get_comment_urls()

    if not comment_urls:
        print("❌ No URLs to test!")
        return

    # Run tests
    test_old_reddit()
    test_basic_vs_realistic_headers(comment_urls)
    test_sticky_vs_rotating(comment_urls)
    test_parallel_requests(comment_urls)

    print("\n" + "="*60)
    print("✅ Testing complete!")
    print("="*60)


if __name__ == "__main__":
    main()
