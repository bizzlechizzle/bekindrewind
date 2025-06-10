#!/usr/bin/env python3
"""Search and scrape source pages for media entries."""

import json
import logging
import random
import sqlite3
from urllib.parse import quote_plus

from playwright.sync_api import sync_playwright

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36'
]


def load_config():
    try:
        with open('2jznoshit.json', 'r') as f:
            cfg = json.load(f).get('2fast2furious', {})
        return cfg.get('logs', False), cfg.get('json', False)
    except (FileNotFoundError, json.JSONDecodeError):
        return False, False


def setup_logging(enabled: bool):
    if enabled:
        logging.basicConfig(
            filename='2fast2furious.log',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a'
        )
    else:
        logging.disable(logging.CRITICAL)


def fetch_html(page, query: str, series: str, season: str, src_link: str) -> str:
    search_url = f'https://www.google.com/search?q={quote_plus(query)}'
    logging.debug('Searching %s', search_url)
    page.goto(search_url, wait_until='networkidle')

    links = []
    for a in page.query_selector_all('a'):
        href = a.get_attribute('href')
        if href and href.startswith('http') and 'google' not in href:
            links.append(href)
        if len(links) == 3:
            break

    scored = []
    for i, url in enumerate(links):
        score = 1 if i == 0 else 0
        u = url.lower()
        if series.lower() in u:
            score += 1
        if season.lower() in u:
            score += 1
        if src_link.lower() in u:
            score += 2
        scored.append((score, url))
        logging.debug('Scored %s -> %d', url, score)

    best_url = None
    best_score = -1
    for score, url in scored:
        if score > best_score:
            best_score = score
            best_url = url

    if best_score < 4:
        best_url = input(f'Enter URL for {series} {season}: ').strip()

    logging.info('Navigating to %s (score %d)', best_url, best_score)
    page.goto(best_url, wait_until='load')
    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
    try:
        page.click('details', timeout=2000)
    except Exception:
        pass
    page.wait_for_timeout(1000)
    return page.content()


def main():
    log_enabled, json_output = load_config()
    setup_logging(log_enabled)

    with sqlite3.connect('danger2manifold.db') as conn, sync_playwright() as pw:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT it_checksum, it_series, it_sea_no, it_src, it_src_link FROM monica WHERE no_monica IS NULL")
        rows = cursor.fetchall()
        if not rows:
            print('No rows to process')
            return

        groups = {}
        for row in rows:
            key = (row['it_series'], row['it_sea_no'], row['it_src'], row['it_src_link'])
            groups.setdefault(key, []).append(row['it_checksum'])

        browser = pw.firefox.launch(headless=True)
        for (series, season, src, src_link), checksums in groups.items():
            context = browser.new_context(user_agent=random.choice(USER_AGENTS))
            page = context.new_page()
            query = f'"{series}" "{season}" "{src}"'
            html = fetch_html(page, query, series, season, src_link)
            for checksum in checksums:
                cursor.execute('UPDATE monica SET no_monica = ? WHERE it_checksum = ?', (html, checksum))
            conn.commit()
            if json_output:
                print(json.dumps({checksums[0]: {'url': page.url}}))
            page.close()
            context.close()
        browser.close()


if __name__ == '__main__':
    main()
