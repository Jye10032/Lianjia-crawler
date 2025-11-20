"""Fetch additional fields from LianJia detail pages.

The script now supports batch processing: by default it scans
``result/information`` for JSON datasets produced by ``main.py`` and generates
per-file ``*_details.csv`` outputs under ``result/detail``.  You can still
target a specific JSON via ``--input`` if needed.
"""
from __future__ import annotations
from session_config import (
    get_default_cookie_dict,
    get_default_user_agent,
    load_cookie_file,
    parse_cookie_string,
)

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
import threading
import time


class TokenBucket:
    def __init__(self, rate: float, capacity: float = 1.0):
        self._rate = float(rate)
        self._capacity = float(capacity)
        # Protect against zero/negative values which would never allow tokens
        # to replenish (leading to an infinite wait). Enforce a small
        # positive minimum so wait_for_token can make progress.
        if self._rate <= 0.0:
            self._rate = 0.01
        # Ensure capacity is at least 1.0 so consuming 1 token per request is possible
        if self._capacity < 1.0:
            self._capacity = 1.0
        self._tokens = float(self._capacity)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def consume(self, amount: float = 1.0) -> bool:
        with self._lock:
            now = time.monotonic()
            delta = now - self._last
            self._tokens = min(
                self._capacity, self._tokens + delta * self._rate)
            self._last = now
            if self._tokens >= amount:
                self._tokens -= amount
                return True
            return False

    def wait_for_token(self, amount: float = 1.0):
        while True:
            if self.consume(amount):
                return
            time.sleep(max(0.05, 1.0 / max(1.0, self._rate)))


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
RESULT_DIR = PROJECT_ROOT / "result"
DEFAULT_INPUT_DIR = RESULT_DIR / "information"
DEFAULT_OUTPUT_DIR = RESULT_DIR / "detail"

USER_AGENTS_PATH = DATA_DIR / "USER_AGENTS.json"
if not USER_AGENTS_PATH.exists():
    raise FileNotFoundError(f"Missing USER_AGENTS.json at {USER_AGENTS_PATH}")

with open(USER_AGENTS_PATH, "r", encoding="utf-8") as file:
    USER_AGENTS: List[str] = json.load(file)

PROFILE_USER_AGENT = get_default_user_agent()
USER_AGENT_POOL: List[str] = (
    [PROFILE_USER_AGENT] if PROFILE_USER_AGENT else list(USER_AGENTS)
)

# Globals for runtime control (initialized with defaults, overridden in main)
GLOBAL_TOKEN_BUCKET: TokenBucket = TokenBucket(rate=0.5, capacity=1.0)
GLOBAL_COOLDOWN: int = 300
# consecutive block counter (protected by lock)
_BLOCK_COUNTER_LOCK = threading.Lock()
CONSECUTIVE_BLOCKS = 0
GLOBAL_MAX_BLOCKS = 5


def log_interception(url: str, status_code: int, snippet: str, ua: str | None = None) -> None:
    """Append a short record about an interception to result/information/interceptions.log"""
    try:
        log_dir = DEFAULT_INPUT_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / 'interceptions.log'
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"[{ts}] {status_code} {url} UA={ua or ''} \n")
            f.write(snippet[:200].replace('\n', ' ') + "\n---\n")
    except Exception:
        # Logging must not break crawling
        pass


def log_failure(url: str, exc: Exception | str, snippet: str | None = None) -> None:
    """Append a short record about a non-retryable failure to result/information/failures.log"""
    try:
        log_dir = DEFAULT_INPUT_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / 'failures.log'
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"[{ts}] {url} ERROR={repr(exc)}\n")
            if snippet:
                f.write((snippet[:400] or '').replace('\n', ' ') + "\n---\n")
    except Exception:
        # don't let logging break the main flow
        pass


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
    global CONSECUTIVE_BLOCKS
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
        # global rate limiting
        try:
            GLOBAL_TOKEN_BUCKET.wait_for_token()
        except Exception:
            # if token bucket broken, fallback to sleep
            time.sleep(1.0)

        headers = {
            "User-Agent": random.choice(USER_AGENT_POOL),
            "Referer": referer,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        try:
            response = session.get(url, headers=headers, timeout=8)
            # Detect WAF / Forbidden
            if response.status_code == 403 or (response.status_code == 200 and response.text and ("拦截" in response.text or "访问验证" in response.text or "请开启JavaScript" in response.text)):
                snippet = response.text or ''
                log_interception(url, response.status_code,
                                 snippet, headers.get('User-Agent'))
                # cooldown on interception
                try:
                    time.sleep(GLOBAL_COOLDOWN)
                except Exception:
                    pass
                # record to failures log
                log_failure(url, RuntimeError(
                    'Blocked by anti-bot verification or 403'), snippet[:400])
                # increment global consecutive block counter
                try:
                    with _BLOCK_COUNTER_LOCK:
                        CONSECUTIVE_BLOCKS += 1
                except Exception:
                    pass
                raise RuntimeError('Blocked by anti-bot verification or 403')

            if response.status_code == 200 and response.text:
                response.encoding = response.apparent_encoding or response.encoding
                # reset consecutive block counter on success
                try:
                    with _BLOCK_COUNTER_LOCK:
                        CONSECUTIVE_BLOCKS = 0
                except Exception:
                    pass
                return response.text
            else:
                # non-200 - log and retry according to retry loop
                snippet = (response.text or '')[:400]
                log_failure(
                    url, f'Non-200 status: {response.status_code}', snippet)
        except requests.RequestException as exc:  # pragma: no cover - network failure path
            last_exc = exc
        # jittered retry backoff
        time.sleep(1 + random.random())
    if last_exc:
        raise last_exc
    raise RuntimeError(
        f"Failed to fetch detail page after {retries} attempts: {url}")


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
        content = ''.join(section.xpath(
            './/div[@class="content"]//text()')).strip()
        if title and content:
            detail_data[title] = content

    # Transaction attributes (挂牌时间、上次交易等)
    transaction_pairs = []
    transaction_items = tree.xpath(
        '//div[contains(@class, "transaction")]//li')
    for item in transaction_items:
        label = ''.join(item.xpath(
            './span[contains(@class, "label")]/text()')).strip()
        value = ''.join(item.xpath('./span[position()>1]//text()')).strip()
        if label and value:
            detail_data[label] = value
            transaction_pairs.append(f"{label}:{value}")
    if transaction_pairs:
        detail_data['交易属性'] = ' | '.join(transaction_pairs)

    # Layout information (户型分间)
    layout_rows = tree.xpath(
        '//div[contains(@class, "layout-wrapper")]//div[contains(@class, "row")]')
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
    # Support both flat JSON (fields at top-level) and nested format where
    # main attributes are under 'details' and 'price'. Prefer top-level
    # keys when present (this matches how main.py currently writes results).
    details = info if any(k in info for k in ('configuration', 'area', 'towards',
                          'decorate', 'storey', 'period', 'categorie')) else info.get('details') or {}
    price = info if any(k in info for k in (
        'total_price', 'unit_price')) else info.get('price') or {}

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


def enrich_infos(
    infos: List[Dict],
    session: requests.Session,
    *,
    min_delay: float,
    max_delay: float,
    limit: int | None = None,
    progress_label: str | None = None,
) -> List[Dict[str, str]]:
    if limit is not None and limit > 0:
        infos = infos[:limit]
    total = len(infos)
    rows: List[Dict[str, str]] = []
    if total == 0:
        return rows

    label_prefix = f"{progress_label} " if progress_label else ''

    for index, info in enumerate(infos, start=1):
        # safety: if many consecutive blocks detected, stop to avoid further damage
        try:
            with _BLOCK_COUNTER_LOCK:
                if GLOBAL_MAX_BLOCKS and CONSECUTIVE_BLOCKS >= GLOBAL_MAX_BLOCKS:
                    print(
                        f"{label_prefix}检测到连续 {CONSECUTIVE_BLOCKS} 次拦截，达到阈值 {GLOBAL_MAX_BLOCKS}，脚本将停止以避免进一步封锁。")
                    raise SystemExit(2)
        except SystemExit:
            raise
        except Exception:
            pass
        row = build_base_row(info)
        url = row['详情链接']
        if not url:
            print(f"{label_prefix}[{index}/{total}] 缺少详情链接，跳过", flush=True)
            rows.append(row)
        else:
            try:
                html = request_detail(url, session)
                detail_data = parse_detail(html)
                merge_detail(row, detail_data)
                status = 'ok'
            except Exception as exc:  # pragma: no cover - network dependent
                row['解析状态'] = f"failed: {exc}"[:120]
                status = f"failed ({exc})"
            rows.append(row)
            print(
                f"{label_prefix}[{index}/{total}] {url} -> {status}", flush=True)

        if index != total:
            time.sleep(random.uniform(min_delay, max_delay))

    return rows


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Fetch detail information for LianJia listings.')
    parser.add_argument(
        '--input', help='Path to a single JSON file generated by main.py (overrides --input-dir)')
    parser.add_argument(
        '--input-dir', help='Directory containing JSON files (defaults to result/information)')
    parser.add_argument(
        '--output', help='Path to the output CSV file (only valid with --input)')
    parser.add_argument(
        '--output-dir', help='Directory to store CSV outputs when processing a directory (defaults to result/detail)')
    parser.add_argument('--min-delay', type=float, default=0.6,
                        help='Minimum delay between requests (seconds)')
    parser.add_argument('--max-delay', type=float, default=1.6,
                        help='Maximum delay between requests (seconds)')
    parser.add_argument(
        '--limit', type=int, help='Optional limit for number of listings per JSON file (for testing)')
    parser.add_argument(
        '--cookie-string', help='Cookie header string captured from a logged-in browser session')
    parser.add_argument(
        '--cookie-file', help='Path to a file containing cookies (JSON mapping or raw Cookie header format)')
    parser.add_argument(
        '--user-agent', help='Override User-Agent header with one captured from the logged-in browser')
    parser.add_argument('--max-qps', type=float,
                        default=0.5, help='最大全局 QPS（默认 0.5）')

    parser.add_argument('--workers', type=int, default=1,
                        help='并发 worker 数（默认 1）')
    parser.add_argument('--cooldown', type=int,
                        default=300, help='遇到 403 时的冷却（秒）')
    parser.add_argument('--max-blocks', type=int,
                        default=5, help='达到连续拦截次数后自动停止（默认 5）')

    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    min_delay = max(0.0, args.min_delay)
    max_delay = max(min_delay, args.max_delay)
    global GLOBAL_TOKEN_BUCKET, GLOBAL_COOLDOWN
    # initialize global rate limiter and cooldown from args
    try:
        # Ensure we don't pass zero to TokenBucket which would never refill.
        qps = max(0.01, float(args.max_qps))
        GLOBAL_TOKEN_BUCKET = TokenBucket(rate=qps, capacity=qps)
    except Exception:
        GLOBAL_TOKEN_BUCKET = TokenBucket(rate=0.5, capacity=0.5)
    GLOBAL_COOLDOWN = int(args.cooldown)
    # configure global max blocks
    global GLOBAL_MAX_BLOCKS
    try:
        GLOBAL_MAX_BLOCKS = max(1, int(args.max_blocks))
    except Exception:
        GLOBAL_MAX_BLOCKS = 5
    max_blocks_threshold = max(1, int(args.max_blocks))

    cookie_jar: Dict[str, str] = get_default_cookie_dict(BASE_DIR.parent)
    if args.cookie_string:
        cookie_jar.update(parse_cookie_string(args.cookie_string))
    if args.cookie_file:
        cookie_jar.update(load_cookie_file(
            Path(args.cookie_file).expanduser()))

    if args.user_agent:
        USER_AGENT_POOL.clear()
        USER_AGENT_POOL.append(args.user_agent)

    if args.input:
        input_paths = [Path(args.input).expanduser().resolve()]
    else:
        input_dir = Path(args.input_dir).expanduser(
        ).resolve() if args.input_dir else DEFAULT_INPUT_DIR
        if not input_dir.exists():
            raise FileNotFoundError(f"Input directory not found: {input_dir}")
        input_paths = sorted(path for path in input_dir.glob(
            '*.json') if path.is_file())

    if not input_paths:
        target_dir = Path(args.input_dir).expanduser(
        ).resolve() if args.input_dir else DEFAULT_INPUT_DIR
        print(f"未在 {target_dir} 下找到任何 JSON 文件。")
        return 0

    if len(input_paths) > 1 and args.output:
        raise ValueError(
            '--output 仅在指定单个 --input 时可用；请使用 --output-dir 指定输出目录。')

    if len(input_paths) == 1 and args.output:
        output_paths = [Path(args.output).expanduser().resolve()]
    else:
        output_dir = Path(args.output_dir).expanduser(
        ).resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        output_paths = [output_dir /
                        f"{path.stem}_details.csv" for path in input_paths]

    session = requests.Session()
    if cookie_jar:
        session.cookies.update(cookie_jar)
    ua_for_session = args.user_agent or PROFILE_USER_AGENT
    if ua_for_session:
        session.headers['User-Agent'] = ua_for_session

    for input_path, output_path in zip(input_paths, output_paths):
        if not input_path.exists():
            print(f"跳过不存在的文件: {input_path}")
            continue

        print(f"开始处理 {input_path.name}")
        infos = load_infos(input_path)
        rows = enrich_infos(
            infos,
            session,
            min_delay=min_delay,
            max_delay=max_delay,
            limit=args.limit,
            progress_label=input_path.stem,
        )
        write_csv(rows, output_path)
        print(f"Detail table saved to {output_path}")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
