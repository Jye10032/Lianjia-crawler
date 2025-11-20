"""改进版：正确提取东莞和惠州的区域信息

根据链家网站的特点，东莞的结构比较特殊，它主要是镇为主的行政区划。
我们需要改变策略：直接从首页提取所有区域，不再分层级。
"""

import requests
from lxml import etree
from session_config import get_default_cookie_dict, get_default_user_agent
from pathlib import Path
import time
import re

BASE_DIR = Path(__file__).resolve().parent


def get_city_simple_regions(city_code: str, city_name: str):
    """直接获取城市的所有区域（不分层级）"""
    url = f'https://{city_code}.lianjia.com/ershoufang/'

    cookies = get_default_cookie_dict(BASE_DIR.parent)
    user_agent = get_default_user_agent()

    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }

    session = requests.Session()
    if cookies:
        session.cookies.update(cookies)

    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        # 保存HTML用于调试
        debug_file = BASE_DIR.parent / f'debug_{city_code}.html'
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"已保存HTML到 {debug_file}")

        root = etree.HTML(response.text)

        # 方法：找到区域筛选部分
        # 通常在 data-role="ershoufang" 下的第一个 position 相关的 div
        region_section = root.xpath('//div[@data-role="ershoufang"]/div[1]')

        if region_section:
            # 获取该部分下的所有链接
            all_links = region_section[0].xpath('.//a[@href]')

            regions = {}
            for link in all_links:
                href = link.get('href', '')
                text = (link.text or '').strip()

                if not text or text in ['不限', '全部']:
                    continue

                # 提取区域代码
                match = re.search(r'/ershoufang/([a-z0-9]+)/', href)
                if match:
                    region_code = match.group(1)

                    # 跳过分页链接
                    if region_code.startswith('pg') or region_code == 'rs':
                        continue

                    if region_code not in regions:
                        regions[region_code] = {
                            'name': text,
                            'code': region_code
                        }
                        print(f"  发现区域: {text} ({region_code})")

            print(f"\n总共找到 {len(regions)} 个区域")
            return regions
        else:
            print("未找到区域筛选部分")
            return None

    except Exception as e:
        print(f"获取{city_name}区域信息失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def format_simple_regions(city_name: str, regions: dict):
    """将区域数据格式化为简单列表"""
    print(f"\n{city_name}区域代码列表:")
    print("=" * 80)

    region_codes = []
    region_names = []

    for region_code, region_info in sorted(regions.items()):
        region_codes.append(region_code)
        region_names.append(region_info['name'])

    # 打印为Python列表格式
    print(f"# {city_name}区域配置")
    print(f"'{city_code}': [  # {city_name}")
    for i, (code, name) in enumerate(zip(region_codes, region_names)):
        comma = ',' if i < len(region_codes) - 1 else ''
        print(f"    '{code}'{comma}  # {name}")
    print("],")

    return region_codes


if __name__ == '__main__':
    # 东莞
    print("=" * 80)
    print("获取东莞区域信息...")
    print("=" * 80)
    city_code = 'dg'
    dg_regions = get_city_simple_regions('dg', '东莞')
    if dg_regions:
        dg_codes = format_simple_regions('东莞', dg_regions)

    print("\n\n")

    # 惠州
    print("=" * 80)
    print("获取惠州区域信息...")
    print("=" * 80)
    city_code = 'hui'
    hui_regions = get_city_simple_regions('hui', '惠州')
    if hui_regions:
        hui_codes = format_simple_regions('惠州', hui_regions)
