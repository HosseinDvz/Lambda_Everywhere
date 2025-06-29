import json
import boto3
import requests
import csv
import io
import os
import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from robotexclusionrulesparser import RobotExclusionRulesParser

s3 = boto3.client('s3')

def can_scrape(url):
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    try:
        r = requests.get(robots_url, timeout=5)
        rp = RobotExclusionRulesParser()
        rp.parse(r.text.splitlines())
        return rp.is_allowed("*", parsed.path)
    except Exception:
        return True

def get_html_with_requests(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; RespectfulScraper/1.0)"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200 and 'text/html' in response.headers.get("Content-Type", ""):
            return response.text
    except requests.RequestException:
        pass
    return None

"""
def get_html_with_headless_browser(url):
    chrome_options = Options()
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"

    chrome_options.add_argument("--headless=new")  # ✅ Modern headless mode
    chrome_options.add_argument("--no-sandbox")    # ✅ Required for Lambda
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--remote-debugging-port=9222")

    # Temp directories to avoid session crash errors
    chrome_options.add_argument(f"--user-data-dir={tempfile.mkdtemp()}")
    chrome_options.add_argument(f"--data-path={tempfile.mkdtemp()}")
    chrome_options.add_argument(f"--disk-cache-dir={tempfile.mkdtemp()}")

    service = Service(executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver")

    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.get(url)

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "p"))
            )
        except TimeoutException:
            print("[WARN] Timed out waiting for <p> tag.")

        html = driver.page_source
        driver.quit()
        return html

    except WebDriverException as e:
        print(f"[ERROR] Browser failed: {str(e)}")

        fallback_html = get_html_with_requests(url)
        if fallback_html:
            print("🪂 Fallback to simple requests succeeded.")
            return fallback_html
        else:
            return "<html><body><p>Browser and requests failed.</p></body></html>"
"""

def summarize_homepage(html):
    soup = BeautifulSoup(html, 'html.parser')
    summary = {}
    if soup.title:
        summary['title'] = soup.title.string.strip()
    meta = soup.find('meta', attrs={'name': 'description'})
    if meta and meta.get('content'):
        summary['meta_description'] = meta['content'].strip()
    h1 = soup.find('h1')
    if h1:
        summary['h1'] = h1.get_text(strip=True)
    paragraphs = soup.find_all('p')
    if paragraphs:
        text_snippets = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20]
        summary['sample_paragraphs'] = text_snippets[:20]
    nav_links = soup.select('nav a')
    if nav_links:
        summary['nav_links'] = [a.get_text(strip=True) for a in nav_links if a.get_text(strip=True)]
    return summary

def generate_text_for_classification(summary):
    parts = []
    if 'title' in summary:
        parts.append(f"Title: {summary['title']}")
    if 'meta_description' in summary:
        parts.append(f"Description: {summary['meta_description']}")
    if 'h1' in summary:
        parts.append(f"Main Heading: {summary['h1']}")
    if 'sample_paragraphs' in summary:
        parts.append("Content:")
        parts.extend(summary['sample_paragraphs'])
    if 'nav_links' in summary:
        parts.append("Navigation Links: " + ", ".join(summary['nav_links'][:10]))
    return "\n".join(parts)

def lambda_handler(event, context):
    records = event.get("Records")
    if records:
        for record in records:
            try:
                body = json.loads(record["body"])
                bucket = body.get("bucket")
                input_key = body.get("key")
            except Exception as e:
                print(f"Invalid SQS message format: {str(e)}")
                continue
            if not bucket or not input_key:
                print("Missing 'bucket' or 'key' in message")
                continue
            process_scrape(bucket, input_key)
        return {"statusCode": 200, "body": "✅ SQS-based scraping complete."}
    else:
        bucket = event.get("bucket")
        input_key = event.get("key")
        if not bucket or not input_key:
            return {"statusCode": 400, "body": "Missing 'bucket' or 'key' in event"}
        process_scrape(bucket, input_key)
        return {"statusCode": 200, "body": f"✅ Scraped from {input_key}"}


def process_scrape(bucket, input_key):
    try:
        response = s3.get_object(Bucket=bucket, Key=input_key)
        url_list = response['Body'].read().decode('utf-8').splitlines()
    except Exception as e:
        print(f"Failed to read input: {str(e)}")
        return

    results = []
    for url in url_list:
        print(f"Scraping: {url}")
        if not can_scrape(url):
            results.append({"website": url, "content": "Blocked by robots.txt"})
            continue

        html = get_html_with_requests(url)
        if not html:
            html = "<html><body><p>No content available</p></body></html>"

        summary = summarize_homepage(html)
        classified_text = generate_text_for_classification(summary)
        results.append({"website": url, "content": classified_text})

    chunk_name = os.path.basename(input_key).replace(".txt", "")
    output_key = f"outputs_sqs/{chunk_name}_results.csv"

    output_csv = io.StringIO()
    writer = csv.DictWriter(output_csv, fieldnames=["website", "content"])
    writer.writeheader()
    writer.writerows(results)

    try:
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=output_csv.getvalue().encode("utf-8")
        )
    except Exception as e:
        print(f"Failed to write output: {str(e)}")
