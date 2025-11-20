"""Script to fetch region and subregion information for Dongguan and Huizhou from Lianjia."""

import requests
from lxml import etree
from session_config import get_default_cookie_dict, get_default_user_agent
from pathlib import Path
import time

BASE_DIR = Path(__file__).resolve().parent


def get_city_regions(city_code: str, city_name: str):
    """获取城市的区域和子区域信息"""
    url = f'https://{city_code}.lianjia.com/ershoufang/'

    # 使用配置的 cookie 和 user agent
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

        # 查找区域链接
        # 通常格式为: <a href="/ershoufang/REGION/">区域名称</a>
        region_links = root.xpath('//div[@data-role="ershoufang"]//a[contains(@href, "/ershoufang/")]')

        if not region_links:
            print(f"未找到{city_name}的区域信息，可能需要登录或页面结构已变化")
            print(f"尝试查找其他选择器...")

            # 尝试其他可能的选择器
            region_links = root.xpath('//a[contains(@href, "/ershoufang/") and not(contains(@href, "pg"))]')

        if not region_links:
            print(f"仍未找到区域信息")
            return None

        region_data = {}

        for link in region_links:
            href = link.get('href', '')
            text = (link.text or '').strip()

            # 跳过 "不限" 等选项
            if not text or text in ['不限', '全部']:
                continue

            # 提取区域代码，格式通常是 /ershoufang/REGION_CODE/
            if '/ershoufang/' in href and href != '/ershoufang/':
                # 去除开头和结尾的斜杠
                parts = href.strip('/').split('/')
                if len(parts) >= 2:
                    region_code = parts[1]

                    # 避免分页链接
                    if region_code.startswith('pg'):
                        continue

                    if region_code not in region_data:
                        region_data[region_code] = {
                            'name': text,
                            'code': region_code,
                            'subregions': []
                        }

        print(f"找到 {len(region_data)} 个区域")

        # 现在我们需要获取每个区下的子区域
        for region_code, region_info in list(region_data.items()):
            region_url = f'https://{city_code}.lianjia.com/ershoufang/{region_code}/'

            try:
                print(f"  正在获取 {region_info['name']} 的子区域...")
                time.sleep(0.5)  # 短暂延迟避免请求过快

                region_response = session.get(region_url, headers=headers, timeout=10)
                region_response.raise_for_status()

                region_root = etree.HTML(region_response.text)

                # 查找该区下的子区域
                subregion_links = region_root.xpath('//div[@data-role="ershoufang"]//a[contains(@href, "/ershoufang/")]')

                if not subregion_links:
                    subregion_links = region_root.xpath('//a[contains(@href, "/ershoufang/") and not(contains(@href, "pg"))]')

                subregions = []
                seen_codes = set()

                for link in subregion_links:
                    href = link.get('href', '')
                    text = (link.text or '').strip()

                    if not text or text in ['不限', '全部', region_info['name']]:
                        continue

                    if '/ershoufang/' in href:
                        parts = href.strip('/').split('/')
                        if len(parts) >= 2:
                            subregion_code = parts[1]

                            # 避免分页链接和重复
                            if subregion_code.startswith('pg'):
                                continue

                            if subregion_code != region_code and subregion_code not in seen_codes:
                                seen_codes.add(subregion_code)
                                subregions.append({
                                    'name': text,
                                    'code': subregion_code
                                })

                region_info['subregions'] = subregions
                print(f"    {region_info['name']} ({region_code}): 找到 {len(subregions)} 个子区域")

            except Exception as e:
                print(f"    获取 {region_code} 的子区域时出错: {e}")

        return region_data

    except Exception as e:
        print(f"获取{city_name}区域信息失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def format_as_python_dict(city_name: str, region_data: dict):
    """将区域数据格式化为Python字典格式，类似深圳的REGION_SUBREGIONS"""
    print(f"\n{city_name}区域信息（Python字典格式）:")
    print("=" * 80)

    result = {}
    for region_code, region_info in region_data.items():
        subregion_codes = [sub['code'] for sub in region_info['subregions']]
        result[region_code] = subregion_codes

    # 打印格式化的字典
    print(f"# {city_name}区域配置")
    for region_code, subregions in result.items():
        region_name = region_data[region_code]['name']
        print(f"    '{region_code}': [  # {region_name}")
        if subregions:
            for i, subregion_code in enumerate(subregions):
                # 找到对应的中文名
                subregion_name = next(
                    (sub['name'] for sub in region_data[region_code]['subregions'] if sub['code'] == subregion_code),
                    ''
                )
                comma = ',' if i < len(subregions) - 1 else ''
                print(f"        '{subregion_code}'{comma}  # {subregion_name}")
        print(f"    ],")

    return result


if __name__ == '__main__':
    print("开始获取东莞和惠州的区域信息...\n")

    # 获取东莞区域信息
    print("=" * 80)
    print("正在获取东莞区域信息...")
    print("=" * 80)
    dg_data = get_city_regions('dg', '东莞')
    if dg_data:
        dg_dict = format_as_python_dict('东莞', dg_data)

    print("\n")

    # 获取惠州区域信息
    print("=" * 80)
    print("正在获取惠州区域信息...")
    print("=" * 80)
    hui_data = get_city_regions('hui', '惠州')
    if hui_data:
        hui_dict = format_as_python_dict('惠州', hui_data)
