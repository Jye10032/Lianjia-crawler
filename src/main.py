import time
import random
import json
import csv
import re
from pathlib import Path

import requests
from lxml import etree
from tqdm.auto import tqdm

from session_config import (
    get_default_cookie_dict,
    get_default_user_agent,
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / 'data'
RESULT_DIR = BASE_DIR.parent / 'result'

with open(DATA_DIR / 'USER_AGENTS.json', 'r', encoding='UTF-8') as file:
    USER_AGENTS = json.load(file)
with open(DATA_DIR / 'CITY_CODE.json', 'r', encoding='UTF-8') as file:
    CITY_CODE = json.load(file)
INFORMATION_PATH = RESULT_DIR / 'information'

PROFILE_USER_AGENT = get_default_user_agent()

REQUEST_DELAY_RANGE = (1.2, 2.8)  # seconds between consecutive HTTP requests
PAGE_DELAY_RANGE = (2.5, 5.5)      # seconds between page crawls

CSV_COLUMNS = ['title', 'location', 'configuration', 'area', 'towards', 'decorate',
               'storey', 'period', 'categorie', 'total_price', 'unit_price',
               'follow_count', 'visit_count', 'publish_time', 'tags', 'link']

CSV_HEADER_CN = ['标题', '地址', '户型', '面积', '朝向', '装修情况',
                 '层数', '建造时间', '楼型', '总价', '每平米单价',
                 '关注人数', '带看次数', '发布时间', '标签', '详情链接']


def random_delay(delay_range: tuple[float, float]) -> None:
    time.sleep(random.uniform(*delay_range))


def build_headers(city: str, page: int, preferred_user_agent: str | None = None) -> dict:
    referer = f'https://{city}.lianjia.com/ershoufang/pg{page}/'
    user_agent = preferred_user_agent or PROFILE_USER_AGENT or random.choice(USER_AGENTS)
    return {
        'User-Agent': user_agent,
        'Referer': referer,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }


def get_html(
    url: str,
    session: requests.Session,
    headers: dict,
    retries: int = 3,
    preferred_user_agent: str | None = None,
) -> str:
    """网络请求, 获取页面 html 代码"""
    for attempt in range(retries):
        request_headers = dict(headers)
        request_headers['User-Agent'] = (
            preferred_user_agent
            or headers.get('User-Agent')
            or PROFILE_USER_AGENT
            or random.choice(USER_AGENTS)
        )
        try:
            response = session.get(url=url, headers=request_headers, timeout=6)
            response.raise_for_status()
            return response.text
        except Exception:
            if attempt + 1 == retries:
                raise
            random_delay((0.8, 1.6))
    raise RuntimeError('Failed to retrieve html')


def categorise(details: list) -> dict:
    """将房屋细节 list 转化为 dict"""
    features = {
        'configuration': ['室', '厅'],
        'area': ['平米'],
        'towards': ['东', '南', '西', '北'],
        'decorate': ['精装', '简装', '毛坯'],
        'storey': ['层'],
        'period': ['年'],
        'categorie': ['板塔结合', '板楼', '塔楼']
    }
    res = {}
    for detail in details:
        for key, values in features.items():
            for value in values:
                if value in detail:
                    res[key] = detail
    return res


def extract(html_code):
    """将 html 中的关键信息解析并存入字典"""
    try:
        root = etree.HTML(html_code)
    except Exception:
        return None
    li_list = root.xpath('//ul[@class="sellListContent"]/li')
    if not li_list:
        return None

    infos = []
    for li in li_list:
        # 获取标题
        title = li.xpath(
            './div[@class="info clear"]/div[@class="title"]/a/text()')
        title = title[0] if title else None

        # 获取地址
        location = li.xpath(
            './div[@class="info clear"]/div[@class="flood"]/div[@class="positionInfo"]/a/text()')
        location = ', '.join(location)

        # 获取房屋细节
        details = li.xpath(
            './div[@class="info clear"]/div[@class="address"]/div/text()')
        details = details[0].split(' | ') if details else None

        # 获取总价
        total_price = li.xpath(
            './div[@class="info clear"]/div[@class="priceInfo"]/div/span/text()')
        total_price = total_price[0] + ' 万' if total_price else None

        # 获取单价
        unit_price = li.xpath(
            './div[@class="info clear"]/div[@class="priceInfo"]/div[@class="unitPrice"]/@data-price')
        unit_price = unit_price[0] + ' 元' if unit_price else None

        # 获取图片 url
        # image_url = li.xpath(
        #     './a[@class="noresultRecommend img LOGCLICKDATA"]/img[@class="lj-lazy"]/@data-original')
        # image_url = image_url[0] if image_url else None

        # 获取跳转链接
        jump_link = li.xpath(
            './div[@class="info clear"]/div[@class="title"]/a/@href')
        jump_link = jump_link[0] if jump_link else None

                # 获取关注与带看信息
        follow_parts = li.xpath(
            './div[@class="info clear"]/div[@class="followInfo"]/text()')
        follow_text = ' / '.join([part.strip()
                                  for part in follow_parts if part.strip()])
        follow_count = visit_count = publish_time = None
        if follow_text:
            match_follow = re.search(r'(\d+)\s*人关注', follow_text)
            if match_follow:
                follow_count = match_follow.group(1)
            match_visit = re.search(r'共(\d+)\s*次带看', follow_text)
            if match_visit:
                visit_count = match_visit.group(1)
            match_publish = re.search(
                r'((?:\d+天(?:以前|以内)发布)|今天发布|刚刚发布)', follow_text)
            if match_publish:
                publish_time = match_publish.group(1)

        # 获取标签
        tags = li.xpath(
            './div[@class="info clear"]/div[@class="tag"]//text()')
        tags = [tag.strip() for tag in tags if tag.strip()]

        infos.append({
            'title': title,
            'location': location,
            'details': categorise(details),
            'price': {
                'total_price': total_price,
                'unit_price': unit_price
            },
            'follow_count': follow_count,
            'visit_count': visit_count,
            'publish_time': publish_time,
            'tags': tags,
            'follow_info': follow_text,
            'link': jump_link
        })
    return infos


def filtrate(data: str) -> str:
    """过滤非 GBK 编码字符"""
    result = ''
    for char in data:
        try:
            char.encode('GBK')
            result += char
        except UnicodeEncodeError:
            continue
    return result


def info_to_row(info: dict) -> list[str]:
    details = info.get('details') or {}
    price = info.get('price') or {}
    row = []
    for column in CSV_COLUMNS:
        if column in ('title', 'location', 'follow_count', 'visit_count', 'publish_time', 'link'):
            value = info.get(column)
        elif column == 'tags':
            value = info.get('tags')
            if isinstance(value, list):
                value = ' | '.join(value)
        elif column in ('total_price', 'unit_price'):
            value = price.get(column)
        else:
            value = details.get(column)
        if isinstance(value, str):
            value = filtrate(value)
        elif value is None:
            value = ''
        row.append(value)
    return row


def save_result() -> None:
    """保存爬取到的数据 (兼容旧接口)."""
    global city_chinese, left, right, infos
    write_json_dataset(city_chinese, left, right, infos)
    print('JSON 数据保存完成。')


def write_json_dataset(city_name: str, start_page: int, end_page: int, infos: list[dict]) -> None:
    """将完整数据写入单个 JSON 文件"""
    INFORMATION_PATH.mkdir(parents=True, exist_ok=True)
    json_path = INFORMATION_PATH / f'{city_name}_{start_page}-{end_page}.json'
    print(f'Saving dataset to {json_path.name} ...')
    with open(json_path, 'w', encoding='utf-8') as file:
        json.dump(fp=file, obj=infos, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    # 获取爬取范围
    city_chinese = input('请输入城市:')
    if city_chinese not in CITY_CODE:
        print('链家暂未提供该城市相关信息.')
        exit(0)
    city = CITY_CODE[city_chinese]
    match = re.search(r'^\D*(\d+)\D+(\d+)\D*$', input('请输入页数范围:'))
    if match:
        left, right = map(int, match.groups())
        if left < 1 or left > right or right > 100:
            print('页数范围不合法')
            exit(0)
    else:
        print('输入格式不合法')
        exit(0)

    session = requests.Session()
    default_cookies = get_default_cookie_dict(BASE_DIR.parent)
    if default_cookies:
        session.cookies.update(default_cookies)
    if PROFILE_USER_AGENT:
        session.headers['User-Agent'] = PROFILE_USER_AGENT
    infos: list[dict] = []
    page_range = range(left, right + 1)
    page_bar = tqdm(page_range, desc='Pages', unit='page', dynamic_ncols=True)

    INFORMATION_PATH.mkdir(parents=True, exist_ok=True)
    csv_path = INFORMATION_PATH / f'{city_chinese}_{left}-{right}.csv'
    csv_is_new = not csv_path.exists()

    with open(csv_path, 'a', encoding='GBK', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if csv_is_new:
            writer.writerow(CSV_HEADER_CN)

        for processed_count, page in enumerate(page_bar, start=1):
            url = f'https://{city}.lianjia.com/ershoufang/pg{page}/'
            headers = build_headers(city, page, PROFILE_USER_AGENT)

            info = None
            last_error = None
            for attempt in range(5):
                try:
                    html_code = get_html(
                        url,
                        session,
                        headers,
                        preferred_user_agent=PROFILE_USER_AGENT,
                    )
                    info = extract(html_code)
                    if info:
                        break
                except Exception as exc:
                    last_error = exc
                random_delay(REQUEST_DELAY_RANGE)

            if not info:
                message = f'page {page} failed after retries.'
                if last_error:
                    message += f' last error: {last_error}'
                page_bar.write(message)
                continue

            rows = [info_to_row(item) for item in info]
            writer.writerows(rows)
            csv_file.flush()

            infos.extend(info)
            page_bar.set_postfix(total_listings=len(infos))

            random_delay(PAGE_DELAY_RANGE)

    if infos:
        save_result()
        print('爬取完成，最终数据已保存。')
    else:
        print('未成功抓取到任何数据，请检查登录状态或网络环境。')
