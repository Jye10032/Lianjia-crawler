"""获取东莞和惠州的完整层级结构（区域 -> 子区域）"""

import requests
from lxml import etree
from session_config import get_default_cookie_dict, get_default_user_agent
from pathlib import Path
import time
import re

BASE_DIR = Path(__file__).resolve().parent


def get_subregions_for_region(city_code: str, region_code: str, region_name: str, session):
    """获取某个区域下的子区域"""
    url = f'https://{city_code}.lianjia.com/ershoufang/{region_code}/'

    user_agent = get_default_user_agent()
    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Referer': f'https://{city_code}.lianjia.com/ershoufang/',
    }

    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        root = etree.HTML(response.text)

        # 查找区域筛选部分 - 通常是第二个位置相关的选择器（第一个是区，第二个是片区）
        # 尝试找到商圈/片区的筛选部分
        # 通常在 data-role="ershoufang" 下的第二个 position 相关的 div
        all_divs = root.xpath('//div[@data-role="ershoufang"]/div')

        subregions = []

        # 尝试从第二个div（如果存在）中提取子区域
        if len(all_divs) >= 2:
            subregion_links = all_divs[1].xpath('.//a[@href]')

            for link in subregion_links:
                href = link.get('href', '')
                text = (link.text or '').strip()

                if not text or text in ['不限', '全部']:
                    continue

                # 提取子区域代码
                match = re.search(r'/ershoufang/([a-z0-9]+)/', href)
                if match:
                    subregion_code = match.group(1)

                    # 跳过分页链接和主区域本身
                    if subregion_code.startswith('pg') or subregion_code == 'rs' or subregion_code == region_code:
                        continue

                    # 检查是否是其他主要区域（通常包含'qu'或'zhen'结尾）
                    is_main_region = (
                        subregion_code.endswith('qu') or
                        subregion_code.endswith('zhen') or
                        subregion_code.endswith('qu1') or
                        subregion_code.endswith('zhen1') or
                        subregion_code.endswith('zhen2') or
                        subregion_code.endswith('zhen3') or
                        subregion_code.endswith('gaoxinqu') or
                        subregion_code in ['huicheng', 'zhongkai', 'huiyang', 'dayawan', 'huidong', 'boluo']
                    )

                    if not is_main_region:
                        subregions.append({
                            'name': text,
                            'code': subregion_code
                        })

        return subregions

    except Exception as e:
        print(f"  获取 {region_name} ({region_code}) 子区域时出错: {e}")
        return []


def get_city_hierarchical_regions(city_code: str, city_name: str):
    """获取城市的层级区域结构"""
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

        root = etree.HTML(response.text)

        # 获取主要区域
        region_section = root.xpath('//div[@data-role="ershoufang"]/div[1]')

        if not region_section:
            print("未找到区域筛选部分")
            return None

        all_links = region_section[0].xpath('.//a[@href]')

        regions = {}
        for link in all_links:
            href = link.get('href', '')
            text = (link.text or '').strip()

            if not text or text in ['不限', '全部']:
                continue

            match = re.search(r'/ershoufang/([a-z0-9]+)/', href)
            if match:
                region_code = match.group(1)

                if region_code.startswith('pg') or region_code == 'rs':
                    continue

                if region_code not in regions:
                    regions[region_code] = {
                        'name': text,
                        'code': region_code,
                        'subregions': []
                    }

        print(f"找到 {len(regions)} 个主要区域")

        # 获取每个区域的子区域
        for region_code, region_info in regions.items():
            print(f"  正在获取 {region_info['name']} 的子区域...")
            time.sleep(0.5)  # 避免请求过快

            subregions = get_subregions_for_region(
                city_code, region_code, region_info['name'], session
            )

            region_info['subregions'] = subregions
            print(f"    -> 找到 {len(subregions)} 个子区域")

        return regions

    except Exception as e:
        print(f"获取{city_name}区域信息失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def format_hierarchical_regions(city_name: str, regions: dict):
    """格式化为类似深圳的层级结构"""
    print(f"\n{city_name}区域信息（类似深圳格式）:")
    print("=" * 80)

    for region_code, region_info in sorted(regions.items()):
        subregion_codes = [sub['code'] for sub in region_info['subregions']]

        if subregion_codes:
            print(f"    '{region_code}': [  # {region_info['name']}")
            for i, subregion_code in enumerate(subregion_codes):
                subregion_name = next(
                    (sub['name'] for sub in region_info['subregions'] if sub['code'] == subregion_code),
                    ''
                )
                comma = ',' if i < len(subregion_codes) - 1 else ''
                print(f"        '{subregion_code}'{comma}  # {subregion_name}")
            print("    ],")
        else:
            # 如果没有子区域，可以选择留空或者以空列表表示
            print(f"    '{region_code}': [],  # {region_info['name']} (无子区域)")


if __name__ == '__main__':
    # 东莞
    print("=" * 80)
    print("获取东莞层级区域信息...")
    print("=" * 80)
    dg_regions = get_city_hierarchical_regions('dg', '东莞')
    if dg_regions:
        format_hierarchical_regions('东莞', dg_regions)

    print("\n\n")

    # 惠州
    print("=" * 80)
    print("获取惠州层级区域信息...")
    print("=" * 80)
    hui_regions = get_city_hierarchical_regions('hui', '惠州')
    if hui_regions:
        format_hierarchical_regions('惠州', hui_regions)
