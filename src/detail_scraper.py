"""Fetch additional fields from LianJia detail pages and merge with existing crawl results."""
from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import requests
from lxml import etree

from session_config import (
    get_default_cookie_dict,
    get_default_user_agent,
    load_cookie_file,
    parse_cookie_string,
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"

USER_AGENTS_PATH = DATA_DIR / "USER_AGENTS.json"
if not USER_AGENTS_PATH.exists():
    raise FileNotFoundError(f"Missing USER_AGENTS.json at {USER_AGENTS_PATH}")

with open(USER_AGENTS_PATH, "r", encoding="utf-8") as file:
    USER_AGENTS: List[str] = json.load(file)

PROFILE_USER_AGENT = get_default_user_agent()
USER_AGENT_POOL: List[str] = (
    [PROFILE_USER_AGENT] if PROFILE_USER_AGENT else list(USER_AGENTS)
)


def filtrate(data: str) -> str:
    """Filter out characters that cannot be encoded by GBK (for CSV compatibility)."""
    result = []
    for char in data:
        try:
            char.encode("GBK")
        except UnicodeEncodeError:
            continue
        result.append(char)
    return "".join(result)


def load_infos(input_path: Path) -> List[Dict]:
    """Load previously scraped listing summaries from JSON."""
    with open(input_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, list):
        raise ValueError("Input JSON must be a list of listing dictionaries.")
    return data


def request_detail(url: str, session: requests.Session, retries: int = 3) -> str:
    last_exc: Exception | None = None
    parsed = urlparse(url)
    domain = f"{parsed.scheme}://{parsed.netloc}"
    referer = f"{domain}/ershoufang/"
    warmup_headers = {
        "User-Agent": random.choice(USER_AGENT_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }

    if not getattr(request_detail, "_prewarmed", None):
        request_detail._prewarmed = set()

    if domain not in request_detail._prewarmed:
        try:
            session.get(referer, headers=warmup_headers, timeout=6)
        except requests.RequestException:
            pass
        request_detail._prewarmed.add(domain)

    for attempt in range(retries):
        headers = {
            "User-Agent": random.choice(USER_AGENT_POOL),
            "Referer": referer,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }
        try:
            response = session.get(url, headers=headers, timeout=8)
            if response.status_code == 200 and response.text:
                response.encoding = response.apparent_encoding or response.encoding
                text = response.text
                if "访问验证" in text or "请开启JavaScript" in text:
                    raise RuntimeError("Blocked by anti-bot verification")
                return text
        except requests.RequestException as exc:  # pragma: no cover - network failure path
            last_exc = exc
        time.sleep(1 + random.random())
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to fetch detail page after {retries} attempts: {url}")


def parse_detail(html: str) -> Dict[str, str]:
    """Extract key/value data from the detail page."""
    tree = etree.HTML(html)
    if tree is None:
        return {}

    # 部分房源详情在未登录时只展示提示信息
    if '登录查看更多房源信息' in html or '需登录后查看完整信息' in html:
        return {'解析状态': 'login_required'}

    detail_data: Dict[str, str] = {}

    # Feature tags under "房源标签"
    tags = [tag.strip() for tag in tree.xpath(
        '//div[contains(@class, "newwrap") and contains(@class, "baseinform")]'
        '//div[contains(@class, "tags")]//a/text()'
    ) if tag.strip()]
    if tags:
        detail_data['房源标签'] = ' | '.join(tags)

    # General feature sections with title and content (e.g., 核心卖点、小区介绍)
    sections = tree.xpath(
        '//div[contains(@class, "newwrap") and contains(@class, "baseinform")]'
        '//div[contains(@class, "baseattribute") and div[@class="name"]]'
    )
    for section in sections:
        title = ''.join(section.xpath('./div[@class="name"]/text()')).strip()
        content = ''.join(section.xpath('.//div[@class="content"]//text()')).strip()
        if title and content:
            detail_data[title] = content

    # Transaction attributes (挂牌时间、上次交易等)
    transaction_pairs = []
    transaction_items = tree.xpath('//div[contains(@class, "transaction")]//li')
    for item in transaction_items:
        label = ''.join(item.xpath('./span[contains(@class, "label")]/text()')).strip()
        value = ''.join(item.xpath('./span[position()>1]//text()')).strip()
        if label and value:
            detail_data[label] = value
            transaction_pairs.append(f"{label}:{value}")
    if transaction_pairs:
        detail_data['交易属性'] = ' | '.join(transaction_pairs)

    # Layout information (户型分间)
    layout_rows = tree.xpath('//div[contains(@class, "layout-wrapper")]//div[contains(@class, "row")]')
    layout_items = []
    for layout_row in layout_rows:
        cols = [
            ''.join(col.xpath('.//text()')).strip()
            for col in layout_row.xpath('./div[contains(@class, "col")]')
        ]
        cols = [col for col in cols if col]
        if cols:
            layout_items.append(' / '.join(cols))
    if layout_items:
        detail_data['户型分间'] = ' ; '.join(layout_items)

    return detail_data


BASE_COLUMNS: Tuple[str, ...] = (
    '标题', '地址', '户型', '面积', '朝向', '装修情况', '层数', '建造时间', '楼型',
    '总价', '每平米单价', '关注人数', '带看次数', '发布时间', '标签', '详情链接'
)

DETAIL_COLUMNS: Tuple[str, ...] = (
    '房源标签', '核心卖点', '小区介绍', '周边配套', '交通出行', '税费解析',
    '权属抵押', '上次交易', '挂牌时间', '交易权属', '房屋用途', '房屋年限',
    '产权所属', '抵押信息', '交易属性', '户型分间'
)


def build_base_row(info: Dict) -> Dict[str, str]:
    details = info.get('details') or {}
    price = info.get('price') or {}

    row = {
        '标题': info.get('title', '') or '',
        '地址': info.get('location', '') or '',
        '户型': details.get('configuration', '') or '',
        '面积': details.get('area', '') or '',
        '朝向': details.get('towards', '') or '',
        '装修情况': details.get('decorate', '') or '',
        '层数': details.get('storey', '') or '',
        '建造时间': details.get('period', '') or '',
        '楼型': details.get('categorie', '') or '',
        '总价': price.get('total_price', '') or '',
        '每平米单价': price.get('unit_price', '') or '',
        '关注人数': info.get('follow_count', '') or '',
        '带看次数': info.get('visit_count', '') or '',
        '发布时间': info.get('publish_time', '') or '',
        '标签': ' | '.join(info.get('tags', [])) if isinstance(info.get('tags'), list) else info.get('tags', ''),
        '详情链接': info.get('link', '') or ''
    }
    return {key: (value if isinstance(value, str) else str(value)) for key, value in row.items()}


def merge_detail(row: Dict[str, str], detail_data: Dict[str, str]) -> None:
    if '解析状态' in detail_data:
        row['解析状态'] = detail_data['解析状态']
    for column in DETAIL_COLUMNS:
        value = detail_data.get(column, '')
        if isinstance(value, list):
            value = ' | '.join(value)
        if isinstance(value, str):
            row[column] = filtrate(value.strip())
        else:
            row[column] = str(value) if value is not None else ''


def write_csv(rows: List[Dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    headers = list(BASE_COLUMNS + DETAIL_COLUMNS) + ['解析状态']
    with open(output_path, 'w', encoding='GBK', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            # Ensure required keys exist even if details were missing
            for column in headers:
                row.setdefault(column, '')
                if isinstance(row[column], str):
                    row[column] = filtrate(row[column])
            writer.writerow(row)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Fetch detail information for LianJia listings.')
    parser.add_argument('--input', required=True, help='Path to the JSON file generated by main.py')
    parser.add_argument('--output', help='Path to the output CSV file (defaults to *_details.csv beside input)')
    parser.add_argument('--min-delay', type=float, default=0.6, help='Minimum delay between requests (seconds)')
    parser.add_argument('--max-delay', type=float, default=1.6, help='Maximum delay between requests (seconds)')
    parser.add_argument('--limit', type=int, help='Optional limit for number of listings (for testing)')
    parser.add_argument('--cookie-string', help='Cookie header string captured from a logged-in browser session')
    parser.add_argument('--cookie-file', help='Path to a file containing cookies (JSON mapping or raw Cookie header format)')
    parser.add_argument('--user-agent', help='Override User-Agent header with one captured from the logged-in browser')
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    input_path = Path(args.input).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input JSON not found: {input_path}")

    output_path = Path(args.output).resolve() if args.output else input_path.with_name(f"{input_path.stem}_details.csv")
    min_delay = max(0.0, args.min_delay)
    max_delay = max(min_delay, args.max_delay)

    cookie_jar: Dict[str, str] = get_default_cookie_dict(BASE_DIR.parent)
    if args.cookie_string:
        cookie_jar.update(parse_cookie_string(args.cookie_string))
    if args.cookie_file:
        cookie_jar.update(load_cookie_file(Path(args.cookie_file).expanduser()))

    if args.user_agent:
        USER_AGENT_POOL.clear()
        USER_AGENT_POOL.append(args.user_agent)

    infos = load_infos(input_path)
    if args.limit:
        infos = infos[: args.limit]

    session = requests.Session()
    if cookie_jar:
        session.cookies.update(cookie_jar)
    ua_for_session = args.user_agent or PROFILE_USER_AGENT
    if ua_for_session:
        session.headers['User-Agent'] = ua_for_session
    rows: List[Dict[str, str]] = []

    for index, info in enumerate(infos, start=1):
        row = build_base_row(info)
        url = row['详情链接']
        if not url:
            print(f"[{index}/{len(infos)}] Skipping entry without detail link", flush=True)
            rows.append(row)
            continue

        try:
            html = request_detail(url, session)
            detail_data = parse_detail(html)
            merge_detail(row, detail_data)
            status = 'ok'
        except Exception as exc:  # pragma: no cover - network dependent
            row['解析状态'] = f"failed: {exc}"[:120]
            status = f"failed ({exc})"
        rows.append(row)
        print(f"[{index}/{len(infos)}] {url} -> {status}", flush=True)

        if index != len(infos):
            time.sleep(random.uniform(min_delay, max_delay))

    write_csv(rows, output_path)
    print(f"Detail table saved to {output_path}")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
