#!/usr/bin/env python3
"""
性能对比测试脚本
对比串行爬虫(main.py)和并行爬虫(parallel_main.py)的实际性能
"""

import time
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
RESULT_DIR = BASE_DIR.parent / 'result'


def analyze_metrics_csv(metrics_path: Path) -> Dict:
    """分析并行爬虫的 metrics.csv 文件"""
    if not metrics_path.exists():
        return None

    print(f"\n分析文件: {metrics_path}")

    df = pd.read_csv(metrics_path)

    # 计算时间间隔
    df['duration'] = df['ts_end'] - df['ts_start']
    df['ts_start_rel'] = df['ts_start'] - df['ts_start'].min()

    total_duration = df['ts_end'].max() - df['ts_start'].min()
    total_requests = len(df)
    success_requests = len(df[df['status'] == 200])

    # 计算实际请求频率
    actual_rps = total_requests / total_duration if total_duration > 0 else 0

    # 计算平均响应时间
    avg_response_time = df['duration'].mean()

    # 按region统计
    region_stats = df.groupby('region').agg({
        'page': 'count',
        'duration': 'mean',
        'status': lambda x: (x == 200).sum()
    }).rename(columns={'page': 'total', 'duration': 'avg_duration', 'status': 'success'})

    results = {
        'total_duration': total_duration,
        'total_requests': total_requests,
        'success_requests': success_requests,
        'actual_rps': actual_rps,
        'avg_response_time': avg_response_time,
        'region_stats': region_stats
    }

    print(f"\n{'='*60}")
    print(f"总耗时: {total_duration:.2f} 秒 ({total_duration/60:.2f} 分钟)")
    print(f"总请求数: {total_requests}")
    print(
        f"成功请求数: {success_requests} ({success_requests/total_requests*100:.1f}%)")
    print(f"实际请求频率: {actual_rps:.3f} req/s")
    print(f"平均响应时间: {avg_response_time:.2f} 秒")
    print(f"\n每个区域统计:")
    print(region_stats)
    print(f"{'='*60}\n")

    return results


def run_benchmark_test(script_name: str, test_config: Dict) -> Dict:
    """运行基准测试"""
    print(f"\n{'='*60}")
    print(f"运行 {script_name} 基准测试")
    print(f"配置: {test_config}")
    print(f"{'='*60}\n")

    start_time = time.time()

    # 构造输入（页数范围）
    page_range = f"{test_config['start_page']}-{test_config['end_page']}"

    try:
        # 运行脚本
        result = subprocess.run(
            [sys.executable, script_name],
            input=page_range,
            text=True,
            capture_output=True,
            timeout=test_config.get('timeout', 600)  # 默认10分钟超时
        )

        end_time = time.time()
        duration = end_time - start_time

        print(f"\n完成! 耗时: {duration:.2f} 秒 ({duration/60:.2f} 分钟)")

        if result.returncode != 0:
            print(f"错误: {result.stderr}")

        return {
            'script': script_name,
            'duration': duration,
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr
        }

    except subprocess.TimeoutExpired:
        print(f"超时! (>{test_config.get('timeout', 600)}秒)")
        return {
            'script': script_name,
            'duration': -1,
            'success': False,
            'error': 'timeout'
        }
    except Exception as e:
        print(f"运行出错: {e}")
        return {
            'script': script_name,
            'duration': -1,
            'success': False,
            'error': str(e)
        }


def compare_performance():
    """对比串行和并行的性能"""
    print("\n" + "="*60)
    print("链家爬虫性能对比测试")
    print("="*60)

    # 选择测试模式
    print("\n请选择测试模式:")
    print("1. 分析现有的 metrics.csv 数据（并行爬虫）")
    print("2. 运行小规模基准测试（串行 vs 并行）")
    print("3. 理论计算对比")

    choice = input("\n请输入选项 (1/2/3): ").strip()

    if choice == '1':
        # 分析现有数据
        metrics_files = list(RESULT_DIR.glob('**/metrics.csv'))
        if not metrics_files:
            print("\n未找到 metrics.csv 文件!")
            print(f"搜索路径: {RESULT_DIR}")
            return

        print(f"\n找到 {len(metrics_files)} 个 metrics.csv 文件:")
        for i, f in enumerate(metrics_files, 1):
            print(f"{i}. {f.relative_to(RESULT_DIR.parent)}")

        for metrics_file in metrics_files:
            analyze_metrics_csv(metrics_file)

    elif choice == '2':
        # 运行基准测试
        print("\n⚠️  警告: 此测试将实际运行爬虫,会消耗时间和网络资源")
        print("建议配置: 1-2 页, 1-2 个区域")

        confirm = input("确认继续? (y/n): ").strip().lower()
        if confirm != 'y':
            print("测试已取消")
            return

        page_range = input("请输入页数范围 (例如 1-2): ").strip()

        test_config = {
            'start_page': int(page_range.split('-')[0]),
            'end_page': int(page_range.split('-')[1]),
            'timeout': 300  # 5分钟超时
        }

        # 运行串行版本
        serial_result = run_benchmark_test('main.py', test_config)

        # 运行并行版本
        parallel_result = run_benchmark_test('parallel_main.py', test_config)

        # 对比结果
        print("\n" + "="*60)
        print("性能对比结果")
        print("="*60)

        if serial_result['success'] and parallel_result['success']:
            serial_time = serial_result['duration']
            parallel_time = parallel_result['duration']
            speedup = serial_time / parallel_time if parallel_time > 0 else 0

            print(f"\n串行版本耗时: {serial_time:.2f} 秒")
            print(f"并行版本耗时: {parallel_time:.2f} 秒")
            print(f"加速比: {speedup:.2f}x")

            if speedup < 1.2:
                print("\n结论: 并行版本没有显著优势 (加速比 < 1.2x)")
                print("原因: 单IP + 限速导致并发收益很小")
            else:
                print(f"\n结论: 并行版本有 {(speedup-1)*100:.1f}% 的性能提升")
        else:
            print("\n测试未完全成功,无法对比")
            print(f"串行版本: {'成功' if serial_result['success'] else '失败'}")
            print(f"并行版本: {'成功' if parallel_result['success'] else '失败'}")

    elif choice == '3':
        # 理论计算
        print("\n" + "="*60)
        print("理论性能计算（基于当前配置）")
        print("="*60)

        # 当前两个版本的实际配置
        request_delay_avg = (0.8 + 1.5) / 2  # 1.15秒
        network_time_avg = 3.5  # 假设平均网络响应时间3.5秒

        print("\n串行版本 (main.py) - 当前配置:")
        print("  - REQUEST_DELAY: 0.8~1.5秒 (平均 1.15秒)")
        print("  - PAGE_DELAY: 已移除 (优化后)")
        print("  - 网络请求时间: 假设平均 3.5秒")
        serial_time_per_req = request_delay_avg + network_time_avg
        serial_rps = 1 / serial_time_per_req
        print(f"  - 平均每请求耗时: {serial_time_per_req:.2f}秒")
        print(f"  - 理论吞吐量: {serial_rps:.3f} req/s")

        print("\n并行版本 (parallel_main.py) - 当前配置:")
        print("  - 线程池: 4 workers")
        print("  - REQUEST_DELAY: 0.8~1.5秒 (平均 1.15秒)")
        print("  - PAGE_DELAY: 未使用")
        print("  - 全局限速器: 0.6 req/s")
        print("  - 子区域限速器: 0.2 req/s")
        print("  - 实际瓶颈: 全局限速器")
        parallel_rps = 0.6
        print(f"  - 理论吞吐量: {parallel_rps:.1f} req/s (受限速器控制)")

        print("\n理论加速比:")
        speedup = parallel_rps / serial_rps
        print(f"  {speedup:.2f}x (并行版本比串行版本快 {(speedup-1)*100:.1f}%)")

        print("\n性能提升来源:")
        print("  1. 并发I/O - 一个线程等待网络响应时其他线程可以工作")
        print("  2. 智能限速器 - 平滑控制请求频率,减少突发导致的延迟")
        print("  3. 专用写入线程 - 解耦网络I/O和磁盘I/O")
        print("  4. 线程池复用 - 避免频繁创建销毁线程的开销")

        print("\n注意:")
        print("  - 两个版本现在使用相同的延迟配置 (REQUEST_DELAY)")
        print("  - 串行版本已移除 PAGE_DELAY,性能显著提升")
        print("  - 理论加速比相比优化前有所降低 (因为串行版本变快了)")
        print("  - 实际加速比受网络环境、重试次数等因素影响")

        print("\n结论:")
        print("  在公平的配置对比下,并行爬虫仍能获得2-3倍性能提升")
        print("  主要归功于并发I/O和智能限速策略")


if __name__ == '__main__':
    try:
        import pandas as pd
    except ImportError:
        print("错误: 需要安装 pandas")
        print("运行: pip install pandas")
        sys.exit(1)

    compare_performance()
