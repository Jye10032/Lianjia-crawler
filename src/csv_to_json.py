#!/usr/bin/env python3
"""
将 CSV 文件转换为 JSON 格式
用于恢复因程序中断而未生成的 JSON 文件
"""
import csv
import json
from pathlib import Path
from typing import List, Dict, Any


def csv_row_to_info(row: Dict[str, str]) -> Dict[str, Any]:
    """
    将 CSV 行转换为 info 字典格式

    CSV 格式（中文表头）：
    ['标题', '地址', '户型', '面积', '朝向', '装修情况', '层数', '建造时间', '楼型',
     '总价', '每平米单价', '关注人数', '带看次数', '发布时间', '标签', '详情链接']

    JSON 格式：
    {
        'title': ...,
        'location': ...,
        'details': {'configuration': ..., 'area': ..., ...},
        'price': {'total_price': ..., 'unit_price': ...},
        'follow_count': ...,
        'visit_count': ...,
        'publish_time': ...,
        'tags': [...],
        'link': ...
    }
    """
    # 构建 details 字典
    details = {}
    detail_keys = ['户型', '面积', '朝向', '装修情况', '层数', '建造时间', '楼型']
    detail_keys_en = ['configuration', 'area', 'towards', 'decorate', 'storey', 'period', 'categorie']

    for cn_key, en_key in zip(detail_keys, detail_keys_en):
        value = row.get(cn_key, '').strip()
        if value:
            details[en_key] = value

    # 构建 price 字典
    price = {}
    total_price = row.get('总价', '').strip()
    unit_price = row.get('每平米单价', '').strip()
    if total_price:
        price['total_price'] = total_price
    if unit_price:
        price['unit_price'] = unit_price

    # 处理 tags（从字符串转为列表）
    tags_str = row.get('标签', '').strip()
    tags = [t.strip() for t in tags_str.split('|') if t.strip()] if tags_str else []

    # 构建完整的 info 字典
    info = {
        'title': row.get('标题', '').strip() or None,
        'location': row.get('地址', '').strip() or None,
        'details': details,
        'price': price,
        'follow_count': row.get('关注人数', '').strip() or None,
        'visit_count': row.get('带看次数', '').strip() or None,
        'publish_time': row.get('发布时间', '').strip() or None,
        'tags': tags,
        'link': row.get('详情链接', '').strip() or None
    }

    return info


def convert_csv_to_json(csv_path: Path, json_path: Path) -> int:
    """
    将单个 CSV 文件转换为 JSON 文件

    Args:
        csv_path: CSV 文件路径
        json_path: 输出的 JSON 文件路径

    Returns:
        转换的记录数
    """
    infos = []

    try:
        # 使用 GBK 编码读取（与原代码保持一致）
        with open(csv_path, 'r', encoding='GBK') as f:
            reader = csv.DictReader(f)
            for row in reader:
                info = csv_row_to_info(row)
                infos.append(info)

        # 写入 JSON 文件
        if infos:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(infos, f, ensure_ascii=False, indent=2)
            print(f'✓ {csv_path.name} -> {json_path.name} ({len(infos)} 条记录)')
            return len(infos)
        else:
            print(f'⚠ {csv_path.name} 为空，跳过')
            return 0

    except Exception as e:
        print(f'✗ 处理 {csv_path.name} 时出错: {e}')
        return 0


def batch_convert(input_dir: Path, output_dir: Path = None) -> None:
    """
    批量转换目录下的所有 CSV 文件为 JSON

    Args:
        input_dir: 包含 CSV 文件的目录
        output_dir: 输出 JSON 文件的目录（默认与输入目录相同）
    """
    if output_dir is None:
        output_dir = input_dir

    output_dir.mkdir(parents=True, exist_ok=True)

    # 查找所有 CSV 文件（排除 metrics.csv）
    csv_files = [f for f in input_dir.glob('*.csv') if f.name != 'metrics.csv']

    if not csv_files:
        print(f'在 {input_dir} 中没有找到 CSV 文件')
        return

    print(f'找到 {len(csv_files)} 个 CSV 文件，开始转换...\n')

    total_records = 0
    success_count = 0

    for csv_file in sorted(csv_files):
        # 生成对应的 JSON 文件名
        json_file = output_dir / csv_file.name.replace('.csv', '.json')

        records = convert_csv_to_json(csv_file, json_file)
        if records > 0:
            total_records += records
            success_count += 1

    print(f'\n转换完成！')
    print(f'成功: {success_count}/{len(csv_files)} 个文件')
    print(f'总记录数: {total_records}')


def main():
    """主函数"""
    # 设置默认路径
    base_dir = Path(__file__).resolve().parent.parent
    guangzhou_dir = base_dir / 'result' / 'guangzhou'

    print(f'CSV 转 JSON 工具')
    print(f'=' * 60)
    print(f'输入目录: {guangzhou_dir}')
    print(f'=' * 60)
    print()

    if not guangzhou_dir.exists():
        print(f'错误: 目录 {guangzhou_dir} 不存在')
        return

    # 执行批量转换
    batch_convert(guangzhou_dir)


if __name__ == '__main__':
    main()
