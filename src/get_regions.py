import time
import random
import json
import re
from pathlib import Path
import requests
from lxml import etree

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / 'data'

with open(DATA_DIR / 'USER_AGENTS.json', 'r', encoding='UTF-8') as file:
    USER_AGENTS = json.load(file)
with open(DATA_DIR / 'CITY_CODE.json', 'r', encoding='UTF-8') as file:
    CITY_CODE = json.load(file)

HEADERS_TEMPLATE = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
}


def get_html(url: str, retries: int = 3) -> str:
    headers = dict(HEADERS_TEMPLATE)
    headers['User-Agent'] = random.choice(USER_AGENTS)
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=8)
            if resp.status_code == 200:
                return resp.text
        except Exception:
            pass
        time.sleep(random.uniform(0.5, 1.5))
    return ''


def get_regions(city_code: str) -> dict[str, list[str]]:
    """爬取城市的区域和子区域"""
    base_url = f'https://{city_code}.lianjia.com/ershoufang/'
    html = get_html(base_url)
    if not html:
        return {}

    try:
        root = etree.HTML(html)
    except Exception:
        return {}

    # 获取区域列表 (位置筛选中的第一级)
    region_links = root.xpath('//div[@data-role="ershoufang"]/div[1]//a')
    regions = {}

    for link in region_links:
        href = link.get('href', '')
        text = link.text or ''
        # 跳过"不限"和空链接
        if not href or text in ('不限', '') or '/ershoufang/' not in href:
            continue
        # 提取区域代码: /ershoufang/futianqu/ -> futianqu
        match = re.search(r'/ershoufang/([a-z0-9]+)/?', href)
        if match:
            region_code = match.group(1)
            # 跳过分页链接
            if region_code.startswith('pg'):
                continue
            regions[region_code] = []

    # 爬取每个区域的子区域
    for region_code in regions:
        print(f'  获取 {region_code} 的子区域...')
        region_url = f'{base_url}{region_code}/'
        html = get_html(region_url)
        if not html:
            continue
        time.sleep(random.uniform(0.3, 0.8))

        try:
            root = etree.HTML(html)
        except Exception:
            continue

        # 获取子区域 (位置筛选中的第二级)
        subregion_links = root.xpath('//div[@data-role="ershoufang"]/div[2]//a')
        for link in subregion_links:
            href = link.get('href', '')
            text = link.text or ''
            if not href or text in ('不限', '') or '/ershoufang/' not in href:
                continue
            match = re.search(r'/ershoufang/([a-z0-9]+)/?', href)
            if match:
                subregion_code = match.group(1)
                if subregion_code.startswith('pg') or subregion_code == region_code:
                    continue
                regions[region_code].append(subregion_code)

    return regions


def fetch_and_save(city_chinese: str) -> dict:
    """获取城市区域并保存到配置文件"""
    if city_chinese not in CITY_CODE:
        print(f'城市 "{city_chinese}" 不在支持列表中')
        return {}

    city_code = CITY_CODE[city_chinese]
    print(f'正在获取 {city_chinese}({city_code}) 的区域信息...')

    regions = get_regions(city_code)
    if not regions:
        print('获取区域信息失败')
        return {}

    config = {
        'domain_root': f'https://{city_code}.lianjia.com/ershoufang/',
        'regions': regions
    }

    config_path = DATA_DIR / 'region_subregions.json'
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f'已保存到 {config_path}')
    print(f'共 {len(regions)} 个区域')
    for region, subs in regions.items():
        print(f'  {region}: {len(subs)} 个子区域')

    return config


if __name__ == '__main__':
    city = input('请输入城市名称: ')
    fetch_and_save(city)
