#!/usr/bin/env python3
"""
为报告生成可视化图表 - 学术风格版本
基于实际测试数据生成性能对比图
"""

import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import seaborn as sns

# 设置学术风格
plt.style.use('seaborn-v0_8-paper')
sns.set_context("paper", font_scale=1.3)
sns.set_palette("deep")

# 设置字体
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = ['Times New Roman', 'DejaVu Serif']
plt.rcParams['font.sans-serif'] = ['Arial', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['axes.grid'] = True
plt.rcParams['grid.alpha'] = 0.3
plt.rcParams['grid.linestyle'] = '--'
plt.rcParams['axes.axisbelow'] = True

# 学术配色方案 - 使用更柔和的颜色
COLORS = {
    'serial': '#E74C3C',      # 柔和的红色
    'parallel': '#3498DB',    # 柔和的蓝色
    'accent': '#2ECC71',      # 柔和的绿色
    'warning': '#F39C12',     # 柔和的橙色
    'neutral': '#95A5A6',     # 灰色
    'highlight': '#9B59B6'    # 紫色
}

OUTPUT_DIR = Path(__file__).resolve().parent.parent / 'report_figures'
OUTPUT_DIR.mkdir(exist_ok=True)


def plot_performance_comparison():
    """Figure 1: 性能对比柱状图 - 学术风格"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))

    versions = ['Serial', 'Parallel']
    times = [83.95, 21.82]
    colors = [COLORS['serial'], COLORS['parallel']]

    # 子图1: 总耗时对比
    bars1 = ax1.bar(versions, times, color=colors, alpha=0.7,
                    edgecolor='black', linewidth=1.2, width=0.6)
    ax1.set_ylabel('Total Time (seconds)', fontsize=11)
    ax1.set_title('Execution Time Comparison', fontsize=12, pad=10)
    ax1.set_ylim(0, 95)
    ax1.grid(axis='y', alpha=0.3, linestyle='--')

    # 添加数值标签
    for bar, time in zip(bars1, times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 2,
                f'{time:.2f}s',
                ha='center', va='bottom', fontsize=10)

    # 添加加速比标注 - 更简洁的样式
    mid_point = 52
    ax1.plot([0, 1], [83.95, 21.82], 'k--', alpha=0.3, linewidth=1)
    ax1.annotate('3.85× Faster', xy=(0.5, mid_point),
                ha='center', fontsize=10,
                bbox=dict(boxstyle='round,pad=0.4',
                         facecolor='white', edgecolor='gray', linewidth=1))

    # 子图2: 吞吐量对比
    throughputs = [0.12, 0.46]
    bars2 = ax2.bar(versions, throughputs, color=colors, alpha=0.7,
                    edgecolor='black', linewidth=1.2, width=0.6)
    ax2.set_ylabel('Throughput (req/s)', fontsize=11)
    ax2.set_title('Throughput Comparison', fontsize=12, pad=10)
    ax2.set_ylim(0, 0.7)
    ax2.axhline(y=0.6, color=COLORS['warning'], linestyle='--',
               linewidth=1.5, alpha=0.7, label='Rate Limit (0.6 req/s)')
    ax2.grid(axis='y', alpha=0.3, linestyle='--')

    # 添加数值标签
    for bar, tp in zip(bars2, throughputs):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                f'{tp:.2f} req/s',
                ha='center', va='bottom', fontsize=10)

    ax2.legend(fontsize=9, frameon=True, fancybox=False,
              edgecolor='gray', loc='upper right')

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'fig1_performance_comparison.png',
               dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✓ Saved: fig1_performance_comparison.png")
    plt.close()


def plot_speedup_analysis():
    """Figure 2: 加速比分析 - 学术风格"""
    fig, ax = plt.subplots(figsize=(9, 5))

    categories = ['Theoretical\nSerial', 'Theoretical\nParallel',
                 'Actual\nSerial', 'Actual\nParallel']
    throughputs = [0.17, 0.60, 0.12, 0.46]
    colors = [COLORS['neutral'], COLORS['neutral'],
             COLORS['serial'], COLORS['parallel']]
    alphas = [0.5, 0.5, 0.8, 0.8]

    bars = ax.barh(categories, throughputs,
                   edgecolor='black', linewidth=1.2, height=0.6)
    for bar, color, alpha in zip(bars, colors, alphas):
        bar.set_color(color)
        bar.set_alpha(alpha)

    ax.set_xlabel('Throughput (req/s)', fontsize=11)
    ax.set_title('Theoretical vs Actual Performance', fontsize=12, pad=10)
    ax.set_xlim(0, 0.65)
    ax.grid(axis='x', alpha=0.3, linestyle='--')

    # 添加数值标签
    for bar, tp in zip(bars, throughputs):
        width = bar.get_width()
        ax.text(width + 0.01, bar.get_y() + bar.get_height()/2.,
               f'{tp:.2f}',
               ha='left', va='center', fontsize=10)

    # 添加加速比标注 - 更简洁
    # 理论加速比
    ax.annotate('', xy=(0.60, 1.15), xytext=(0.17, 0.85),
               arrowprops=dict(arrowstyle='<->', color='gray', lw=1.5))
    ax.text(0.385, 1.5, '3.53× (Theory)', ha='center', fontsize=9,
           bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                    edgecolor='gray', linewidth=1))

    # 实际加速比
    ax.annotate('', xy=(0.46, 3.15), xytext=(0.12, 2.85),
               arrowprops=dict(arrowstyle='<->', color='black', lw=1.5))
    ax.text(0.29, 3.5, '3.85× (Actual)', ha='center', fontsize=9,
           bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                    edgecolor='black', linewidth=1))

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'fig2_speedup_analysis.png',
               dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✓ Saved: fig2_speedup_analysis.png")
    plt.close()


def plot_long_running_performance():
    """Figure 3: 长时间运行性能对比 - 学术风格"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))

    # 子图1: 吞吐量随运行时间变化
    scenarios = ['Benchmark\n(10 req)', 'Huizhou\n(1320 req)', 'Guangzhou\n(2611 req)']
    throughputs = [0.46, 0.197, 0.189]
    durations = [0.36, 111.8, 230.6]
    colors_grad = [COLORS['accent'], COLORS['warning'], COLORS['serial']]

    bars1 = ax1.bar(scenarios, throughputs, color=colors_grad, alpha=0.7,
                    edgecolor='black', linewidth=1.2, width=0.6)
    ax1.set_ylabel('Throughput (req/s)', fontsize=11)
    ax1.set_title('Throughput vs. Crawling Duration', fontsize=12, pad=10)
    ax1.set_ylim(0, 0.6)
    ax1.axhline(y=0.6, color=COLORS['neutral'], linestyle='--',
               linewidth=1.5, alpha=0.5, label='Rate Limit')
    ax1.grid(axis='y', alpha=0.3, linestyle='--')

    for bar, tp in zip(bars1, throughputs):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.015,
                f'{tp:.3f}',
                ha='center', va='bottom', fontsize=9)

    ax1.legend(fontsize=9, frameon=True, fancybox=False,
              edgecolor='gray', loc='upper right')

    # 子图2: 性能下降趋势
    ax2_twin = ax2.twinx()

    # 吞吐量曲线
    line1 = ax2.plot(durations, throughputs, 'o-', color=COLORS['parallel'],
                     linewidth=2, markersize=7, label='Throughput',
                     markeredgecolor='black', markeredgewidth=0.5)
    ax2.set_xlabel('Duration (minutes)', fontsize=11)
    ax2.set_ylabel('Throughput (req/s)', fontsize=11, color=COLORS['parallel'])
    ax2.tick_params(axis='y', labelcolor=COLORS['parallel'])
    ax2.set_xscale('log')
    ax2.grid(True, alpha=0.3, linestyle='--')

    # 性能下降百分比
    degradation = [(0.46 - tp) / 0.46 * 100 for tp in throughputs]
    line2 = ax2_twin.plot(durations, degradation, 's--', color=COLORS['serial'],
                         linewidth=2, markersize=6, label='Degradation',
                         markeredgecolor='black', markeredgewidth=0.5)
    ax2_twin.set_ylabel('Performance Degradation (%)', fontsize=11,
                       color=COLORS['serial'])
    ax2_twin.tick_params(axis='y', labelcolor=COLORS['serial'])

    # 合并图例
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax2.legend(lines, labels, loc='center right', fontsize=9,
              frameon=True, fancybox=False, edgecolor='gray')

    ax2.set_title('Performance Degradation Over Time', fontsize=12, pad=10)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'fig3_long_running_performance.png',
               dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✓ Saved: fig3_long_running_performance.png")
    plt.close()


def plot_cpu_utilization():
    """Figure 4: CPU利用率对比 - 学术风格"""
    fig, ax = plt.subplots(figsize=(7, 5))

    versions = ['Serial\nCrawler', 'Parallel\nCrawler']
    cpu_avg = [10, 25]
    cpu_range = [(5, 15), (15, 35)]
    colors = [COLORS['serial'], COLORS['parallel']]

    x_pos = np.arange(len(versions))
    bars = ax.bar(x_pos, cpu_avg, color=colors, alpha=0.7,
                  edgecolor='black', linewidth=1.2, width=0.5)

    # 添加误差线表示范围
    for i, (bar, (low, high)) in enumerate(zip(bars, cpu_range)):
        ax.plot([i, i], [low, high], 'k-', linewidth=2.5, alpha=0.8)
        ax.plot([i-0.08, i+0.08], [low, low], 'k-', linewidth=2)
        ax.plot([i-0.08, i+0.08], [high, high], 'k-', linewidth=2)
        ax.text(i, high + 1.5, f'{low}-{high}%', ha='center', fontsize=9)

    # 添加平均值标签
    for i, (bar, avg) in enumerate(zip(bars, cpu_avg)):
        ax.text(i, avg/2, f'Avg: {avg}%',
               ha='center', va='center', fontsize=10,
               color='white', weight='bold')

    ax.set_ylabel('CPU Utilization (%)', fontsize=11)
    ax.set_title('CPU Utilization Comparison', fontsize=12, pad=10)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(versions, fontsize=10)
    ax.set_ylim(0, 40)
    ax.grid(axis='y', alpha=0.3, linestyle='--')

    # 添加I/O-bound标注 - 更简洁
    ax.annotate('I/O-Bound Workload',
               xy=(1, 30), xytext=(1.4, 35),
               fontsize=9, ha='center',
               bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                        edgecolor='gray', linewidth=1),
               arrowprops=dict(arrowstyle='->', color='gray', lw=1.5))

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'fig4_cpu_utilization.png',
               dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✓ Saved: fig4_cpu_utilization.png")
    plt.close()


def plot_summary_table():
    """Figure 5: 综合对比表格图 - 学术风格"""
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.axis('off')

    # 数据
    data = [
        ['Metric', 'Serial', 'Parallel', 'Improvement'],
        ['Execution Time', '83.95 s', '21.82 s', '3.85×'],
        ['Throughput', '0.12 req/s', '0.46 req/s', '3.83×'],
        ['Time per Request', '8.40 s', '2.18 s', '3.85×'],
        ['CPU Utilization', '5-15%', '15-35%', '2.3×'],
        ['Success Rate', '100%', '100%', '—'],
        ['Code Complexity', 'Low', 'Medium', '—'],
    ]

    # 创建表格
    table = ax.table(cellText=data, cellLoc='center', loc='center',
                    colWidths=[0.3, 0.2, 0.2, 0.3])

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2.2)

    # 设置学术风格
    for i in range(len(data)):
        for j in range(len(data[0])):
            cell = table[(i, j)]
            if i == 0:  # 表头
                cell.set_facecolor('#4A5568')  # 深灰色
                cell.set_text_props(weight='bold', color='white', fontsize=11)
            else:
                if j == 0:  # 第一列
                    cell.set_facecolor('#F7FAFC')
                    cell.set_text_props(weight='bold', fontsize=10)
                elif j == 3 and '×' in data[i][j]:  # 加速比列
                    cell.set_facecolor('#C6F6D5')  # 浅绿色
                    cell.set_text_props(weight='bold')
                elif j == 2:  # Parallel列
                    cell.set_facecolor('#EBF8FF')  # 浅蓝色
                else:
                    cell.set_facecolor('white')

            cell.set_edgecolor('#CBD5E0')
            cell.set_linewidth(1)

    ax.set_title('Comprehensive Performance Comparison',
                fontsize=13, weight='bold', pad=15)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'fig5_summary_table.png',
               dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✓ Saved: fig5_summary_table.png")
    plt.close()


def main():
    print("Generating academic-style figures for report section 6.1...")
    print(f"Output directory: {OUTPUT_DIR}\n")

    plot_performance_comparison()
    plot_speedup_analysis()
    plot_long_running_performance()
    plot_cpu_utilization()
    plot_summary_table()

    print(f"\n✓ All figures generated successfully in academic style!")
    print(f"✓ Output location: {OUTPUT_DIR}/")
    print("\nFigures generated:")
    print("  - fig1_performance_comparison.png  (Performance metrics)")
    print("  - fig2_speedup_analysis.png        (Theoretical vs Actual)")
    print("  - fig3_long_running_performance.png (Long-term degradation)")
    print("  - fig4_cpu_utilization.png         (CPU usage)")
    print("  - fig5_summary_table.png           (Summary table)")


if __name__ == '__main__':
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
    except ImportError as e:
        print(f"Error: Required libraries not found - {e}")
        print("Install: pip install matplotlib seaborn")
        exit(1)

    main()
