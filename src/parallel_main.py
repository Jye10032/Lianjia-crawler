import time
import random
import json
import csv
import re
import math
import queue
import threading
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import requests
from lxml import etree
from concurrent.futures import ThreadPoolExecutor, as_completed

from session_config import get_default_cookie_dict, get_default_user_agent

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / 'data'
RESULT_DIR = BASE_DIR.parent / 'result'

with open(DATA_DIR / 'USER_AGENTS.json', 'r', encoding='utf-8') as f:
    USER_AGENTS = json.load(f)
with open(DATA_DIR / 'CITY_CODE.json', 'r', encoding='utf-8') as f:
    CITY_CODE = json.load(f)

INFORMATION_PATH = RESULT_DIR / 'guangzhou'
INFORMATION_PATH.mkdir(parents=True, exist_ok=True)

PROFILE_USER_AGENT = get_default_user_agent()

REQUEST_DELAY_RANGE = (0.8, 1.5)  # seconds between consecutive HTTP requests
PAGE_DELAY_RANGE = (1.0, 2.0)      # seconds between page crawls

CSV_COLUMNS = ['title', 'location', 'configuration', 'area', 'towards', 'decorate',
               'storey', 'period', 'categorie', 'total_price', 'unit_price',
               'follow_count', 'visit_count', 'publish_time', 'tags', 'link']
CSV_HEADER_CN = ['标题', '地址', '户型', '面积', '朝向', '装修情况', '层数', '建造时间', '楼型',
                 '总价', '每平米单价', '关注人数', '带看次数', '发布时间', '标签', '详情链接']

# # SZ_DOMAIN_ROOT = 'https://sz.lianjia.com/ershoufang'
# SZ_DOMAIN_ROOT = 'https://gz.lianjia.com/ershoufang'

# REGION_SUBREGIONS: dict[str, list[str]] = {
#     'tianhe': ['cencun','changxing1','chebei','dashadi','dongfengdong','dongfengxi',
#                 'donglang','fangcun','fenshui','guanggangxincheng','hedong1',
#                 'hepingxi','huadiwan','huangsha','jiaokou1','jushu','kengkou',
#                'longxi1','liuhuazhanqian','longjin','nananlu','renminbei1','renminlu',
#                'sanyuanli','shahe1','shataibei','shatainan','shipai1','shuiyin',
#                'tangxia1','tianhegongyuan','tianhekeyunzhan','tianhenan','tianrunlu',
#                'tiyuzhongxin','wushan','wuyangxincheng','xiaobei','xihualu','xilang','ximenkou','yangji','yuexiunan']
#     # 'luohuqu': [
#     'baishida', 'buxin', 'chunfenglu', 'cuizhu', 'diwang', 'dongmen',
#     'honghu', 'huangbeiling', 'huangmugang', 'liantang', 'luohukouan',
#     'luoling', 'qingshuihe', 'sungang', 'wanxiangcheng', 'xinxiu', 'yinhu'
# ],
# 'futianqu': ['bagualing','baihua','chegongmiao','chiwei','futianbaoshuiqu',
#              'futianzhongxin','huanggan','huangmugang','huaqiangbei','huaqiangnan',
#              'jingtian','lianhua','meilin','shangbu','shangxiasha','shawei',
#              'shixia','xiangmeibei','xiangmihus','xinzhu','yinhu','yuanling','zhuzilin'],
# 'nanshanqu': ['baishizhou','daxuecheng','hongshuwan','houhai','huaqiaocheng',
#               'kechiyuan','nanshanzhongxin','nantou','qianhai','shekou',
#               'shenzhenwan','xili'],
# 'yantianqu': ['meisha','shatoujiao','yantiangang'],
# 'baoanqu':   ['baoanzhongxin','bihai','fanshen','fuyong','hangcheng','shajing',
#               'shiyan','songgang','taoyuanju','xicheng','xinan','xixiang'],
# 'longgangqu':['bantian','buji','bufendanfen','bujiguan','bujijie','bujinanling',
#               'bujishiyaling','bujishuijing','danzhutou','dayunxincheng',
#               'henggang','longgangbaohe','longgangshuanglong','longgangzhongxincheng',
#               'minzhi','pingdi','pinghu'],
# 'longhuaqu': ['bantian','guanlan','hongshan','longhuaxinqu','longhuazhongxin',
#               'meilinguan','minzhi','shangtang','shiyan'],
# 'guangmingqu': ['gongming','guangming'],
# 'pingshanqu':  ['pingshan'],
# 'dapengxinqu': ['dapengbandao']
# }

SZ_DOMAIN_ROOT = 'https://hui.lianjia.com/ershoufang/'

REGION_SUBREGIONS: dict[str, list[str]] = {
    'boluo': [  # 博罗
        'boluolongxi', 'boluoshiwan', 'boluoyuanzhou',
        'luofushan', 'xiaojinkou'
    ]
}

#   惠州区域配置 (6个区)

# SZ_DOMAIN_ROOT = 'https://hui.lianjia.com/ershoufang/'

# REGION_SUBREGIONS: dict[str, list[str]] = {
#     'huicheng': [  # 惠城
#         'chenjiang', 'dongjiangxincheng', 'dongping2', 'henanan',
#         'huihuan', 'jiangbei2', 'longfeng', 'maan',
#         'maidi', 'nantan', 'ruhu', 'shuikou',
#         'xiajiao', 'xiaojinkou', 'xiapu1'
#     ],
#     'zhongkai': [  # 仲恺
#         'chenjiang', 'huihuan', 'tongqiaolilin'
#     ],
#     'huiyang': [  # 惠阳
#         'baiyunxincheng', 'kaichengdarunfa', 'nanzhanxincheng',
#         'qiuchang', 'quzhengfu1', 'xinxu'
#     ],
#     'dayawan': [  # 大亚湾
#         'aotou', 'biyadishangquan', 'kaichengdarunfa', 'longguangcheng',
#         'wanda15', 'wuyueguangchang1', 'xiayong', 'xiquxinliao'
#     ],
#     'huidong': [  # 惠东
#         'huidongxiancheng', 'huidongyanhai', 'shiliyintan'
#     ],
#     'boluo': [  # 博罗
#         'boluolongxi', 'boluoshiwan', 'boluoyuanzhou',
#         'luofushan', 'xiaojinkou'
#     ]
# }


BASE_HEADERS_TEMPLATE = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
}

# 预编译正则
RE_FOLLOW = re.compile(r'(\d+)\s*人关注')
RE_VISIT = re.compile(r'共(\d+)\s*次带看')
RE_PUB = re.compile(r'((?:\d+天(?:以前|以内)发布)|今天发布|刚刚发布)')


def random_delay(delay_range: Tuple[float, float]) -> None:
    if delay_range[1] > 0:
        time.sleep(random.uniform(*delay_range))


def build_headers(subregion: str, page: int, ua_hint: Optional[str]) -> Dict[str, str]:
    headers = dict(BASE_HEADERS_TEMPLATE)
    headers['Referer'] = f'{SZ_DOMAIN_ROOT}/{subregion}/'
    headers['User-Agent'] = ua_hint or PROFILE_USER_AGENT or random.choice(
        USER_AGENTS)
    return headers


def get_html(url: str, session: requests.Session, headers: Dict[str, str],
             retries: int = 3) -> Tuple[str, int, int]:
    """返回 (text, status, bytes)；指数退避 + 抖动"""
    backoff = 0.6
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=headers, timeout=8)
            status = resp.status_code
            if status == 200:
                text = resp.text
                size = len(resp.content or b'')
                return text, status, size
            # 处理可重试状态码
            if status in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f'status {status}')
            # 其他非200，直接返回
            return "", status, len(resp.content or b'')
        except Exception:
            if attempt + 1 == retries:
                raise
            sleep_s = backoff * (2 ** attempt) * (1 + random.random()*0.5)
            time.sleep(sleep_s)
    raise RuntimeError('unreachable')


def categorise(details: Optional[List[str]]) -> Dict[str, str]:
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
    if not details:
        return res
    for d in details:
        for k, vals in features.items():
            if any(v in d for v in vals):
                res[k] = d
    return res


# 预编译 XPath（避免每次解析重建对象）
X_UL = '//ul[@class="sellListContent"]/li'
X_TIT = './div[@class="info clear"]/div[@class="title"]/a/text()'
X_LOC = './div[@class="info clear"]/div[@class="flood"]/div[@class="positionInfo"]/a/text()'
X_DET = './div[@class="info clear"]/div[@class="address"]/div/text()'
X_TP = './div[@class="info clear"]/div[@class="priceInfo"]/div/span/text()'
X_UP = './div[@class="info clear"]/div[@class="priceInfo"]/div[@class="unitPrice"]/@data-price'
X_LNK = './div[@class="info clear"]/div[@class="title"]/a/@href'
X_FOL = './div[@class="info clear"]/div[@class="followInfo"]/text()'
X_TAG = './div[@class="info clear"]/div[@class="tag"]//text()'


def extract(html_code: str) -> Optional[List[Dict[str, Any]]]:
    try:
        root = etree.HTML(html_code)
    except Exception:
        return None
    li_list = root.xpath(X_UL)
    print(len(li_list))
    if not li_list:
        return None
    out = []
    for li in li_list:
        title = (li.xpath(X_TIT) or [None])[0]
        location = ', '.join(li.xpath(X_LOC) or [])
        details_raw = (li.xpath(X_DET) or [None])[0]
        details = details_raw.split(' | ') if details_raw else None
        total_price = li.xpath(X_TP)
        total_price = f'{total_price[0]} 万' if total_price else None
        unit_price = li.xpath(X_UP)
        unit_price = f'{unit_price[0]} 元' if unit_price else None
        link = (li.xpath(X_LNK) or [None])[0]
        fol_parts = [p.strip() for p in li.xpath(X_FOL) or [] if p.strip()]
        fol_text = ' / '.join(fol_parts) if fol_parts else ''
        follow_count = (RE_FOLLOW.search(fol_text).group(
            1) if RE_FOLLOW.search(fol_text) else None)
        visit_count = (RE_VISIT.search(fol_text).group(
            1) if RE_VISIT.search(fol_text) else None)
        publish_time = (RE_PUB.search(fol_text).group(
            1) if RE_PUB.search(fol_text) else None)
        tags = [t.strip() for t in (li.xpath(X_TAG) or []) if t.strip()]
        out.append({
            'title': title,
            'location': location,
            'details': categorise(details),
            'price': {'total_price': total_price, 'unit_price': unit_price},
            'follow_count': follow_count, 'visit_count': visit_count,
            'publish_time': publish_time, 'tags': tags, 'follow_info': fol_text, 'link': link
        })
    # print(out)
    return out


def filtrate_gbk(data: str) -> str:
    # 如无需 GBK，可改成直接返回
    result = []
    for ch in data:
        try:
            ch.encode('GBK')
            result.append(ch)
        except UnicodeEncodeError:
            continue
    return ''.join(result)


def info_to_row(info: Dict[str, Any]) -> List[str]:
    details = info.get('details') or {}
    price = info.get('price') or {}
    row = []
    for col in CSV_COLUMNS:
        if col in ('title', 'location', 'follow_count', 'visit_count', 'publish_time', 'link'):
            v = info.get(col)
        elif col == 'tags':
            v = info.get('tags')
            v = ' | '.join(v) if isinstance(v, list) else v
        elif col in ('total_price', 'unit_price'):
            v = price.get(col)
        else:
            v = details.get(col)
        if isinstance(v, str):
            v = filtrate_gbk(v)
        elif v is None:
            v = ''
        row.append(v)
    return row

# ----------------- 并发 & 限速 -----------------


class LocalSession(threading.local):
    def __init__(self):
        self.session = None


THREAD_LOCAL = LocalSession()


def get_thread_session(base_cookies: Dict[str, str]) -> requests.Session:
    if THREAD_LOCAL.session is None:
        s = requests.Session()
        if base_cookies:
            s.cookies.update(base_cookies)
        THREAD_LOCAL.session = s
    return THREAD_LOCAL.session


class TokenBucket:
    """简单令牌桶限速：rate = 每秒令牌数；burst = 桶容量。"""

    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.capacity = burst
        self.tokens = burst
        self.timestamp = time.monotonic()
        self.lock = threading.Lock()

    def take(self, tokens=1):
        while True:
            with self.lock:
                now = time.monotonic()
                delta = now - self.timestamp
                self.timestamp = now
                self.tokens = min(
                    self.capacity, self.tokens + delta * self.rate)
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

            time.sleep(0.01)


# 全局限速（按域名共享）
GLOBAL_BUCKET = TokenBucket(rate=0.6, burst=2)
# 子区域轻量限速（避免单点猛打）
SUBREGION_BUCKETS: Dict[str, TokenBucket] = {}


def get_bucket_for_subregion(subregion: str) -> TokenBucket:
    if subregion not in SUBREGION_BUCKETS:
        SUBREGION_BUCKETS[subregion] = TokenBucket(rate=0.2, burst=1)
    return SUBREGION_BUCKETS[subregion]

# ----------------- 写入线程（单写多读） -----------------


class WriterThread(threading.Thread):
    def __init__(self, out_dir: Path, csv_header_cn: List[str]):
        super().__init__(daemon=True)
        self.out_dir = out_dir
        self.header = csv_header_cn
        self.q: "queue.Queue[Tuple[str,str,List[List[str]]]]" = queue.Queue(
            maxsize=1000)
        self.files: Dict[Tuple[str, str], csv.writer] = {}
        self.file_handles: Dict[Tuple[str, str], Any] = {}
        self.lock = threading.Lock()
        self.stop_flag = False
        self.infos_agg: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        # metrics
        self.metrics_file = open(
            self.out_dir / 'metrics.csv', 'w', encoding='utf-8', newline='')
        self.metrics_csv = csv.writer(self.metrics_file)
        self.metrics_csv.writerow(
            ['ts_start', 'ts_end', 'region', 'subregion', 'page', 'status', 'bytes', 'retries'])

    def run(self):
        while not self.stop_flag or not self.q.empty():
            try:
                msg_type, key, payload = self.q.get(timeout=0.2)
            except queue.Empty:
                continue
            if msg_type == 'rows':
                region, subregion = key.split('|', 1)
                self._ensure_writer(region, subregion)
                self.files[(region, subregion)].writerows(payload)
            elif msg_type == 'info':
                region, subregion = key.split('|', 1)
                self.infos_agg.setdefault(
                    (region, subregion), []).extend(payload)
            elif msg_type == 'metric':
                self.metrics_csv.writerow(payload)
            elif msg_type == 'flush':
                for fh in self.file_handles.values():
                    fh.flush()
                self.metrics_file.flush()
            self.q.task_done()

    def _ensure_writer(self, region: str, subregion: str):
        k = (region, subregion)
        if k in self.files:
            return
        path = self.out_dir / f'{region}_{subregion}.csv'
        fh = open(path, 'w', encoding='GBK', newline='')
        writer = csv.writer(fh)
        writer.writerow(self.header)
        self.file_handles[k] = fh
        self.files[k] = writer

    def close(self):
        self.stop_flag = True
        self.q.join()
        for fh in self.file_handles.values():
            fh.flush()
            fh.close()
        self.metrics_file.flush()
        self.metrics_file.close()

# ----------------- 任务执行 -----------------


def crawl_one(subregion: str, page: int, base_cookies: Dict[str, str], ua_hint: Optional[str],
              region_key: str) -> Tuple[List[List[str]], List[Dict[str, Any]], Dict[str, Any]]:
    """返回 (rows, infos, metric)；metric含起止时间/状态/大小/重试次数等"""
    sess = get_thread_session(base_cookies)
    url = f'{SZ_DOMAIN_ROOT}/{subregion}/pg{page}/'
    headers = build_headers(subregion, page, ua_hint)
    # 限速
    GLOBAL_BUCKET.take()
    get_bucket_for_subregion(subregion).take()
    # 抖动
    random_delay(REQUEST_DELAY_RANGE)

    ts_start = time.time()
    retries_used = 0
    status = -1
    size = 0
    rows, infos = [], []
    try:
        html, status, size = get_html(url, sess, headers, retries=4)
        if status == 200 and html:
            info_list = extract(html) or []
            if info_list:
                infos = info_list
                rows = [info_to_row(item) for item in info_list]
    except Exception:
        retries_used = 3  # 近似记录；详细可在 get_html 内透传
    ts_end = time.time()
    metric = {
        'ts_start': ts_start, 'ts_end': ts_end, 'region': region_key,
        'subregion': subregion, 'page': page, 'status': status,
        'bytes': size, 'retries': retries_used
    }
    return rows, infos, metric


def write_json_dataset(out_dir: Path, region_key: str, subregion: str, infos: List[Dict[str, Any]]):
    if not infos:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f'{region_key}_{subregion}.json'
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(infos, f, ensure_ascii=False, indent=2)


def main():
    range_input = input('请输入页数范围 (例如 1-8): ')
    m = re.search(r'^\D*(\d+)\D+(\d+)\D*$', range_input)
    if not m:
        print('输入格式不合法')
        return
    left, right = map(int, m.groups())
    if left < 1 or left > right or right > 100:
        print('页数范围不合法')
        return

    base_cookies = get_default_cookie_dict(BASE_DIR.parent) or {}
    ua_hint = PROFILE_USER_AGENT

    writer = WriterThread(INFORMATION_PATH, CSV_HEADER_CN)
    writer.start()

    max_workers = 4
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for region_key, subs in REGION_SUBREGIONS.items():
            # 如果该区域有子区域，遍历子区域；否则直接爬取区域本身
            if subs:
                # 有子区域，遍历每个子区域
                for subregion in subs:
                    for page in range(left, right+1):
                        futures.append(pool.submit(
                            crawl_one, subregion, page, base_cookies, ua_hint, region_key
                        ))
            else:
                # 没有子区域，直接爬取该区域（使用区域代码作为subregion）
                for page in range(left, right+1):
                    futures.append(pool.submit(
                        crawl_one, region_key, page, base_cookies, ua_hint, region_key
                    ))

        for fut in as_completed(futures):
            rows, infos, metric = fut.result()
            region, subregion = metric['region'], metric['subregion']
            key = f'{region}|{subregion}'
            if rows:
                writer.q.put(('rows', key, rows))
            if infos:
                writer.q.put(('info', key, infos))
            writer.q.put((
                'metric', '', [
                    metric['ts_start'], metric['ts_end'], region, subregion,
                    metric['page'], metric['status'], metric['bytes'], metric['retries']
                ]
            ))

            # 周期性flush（降低数据丢失风险）
            if random.random() < 0.02:
                writer.q.put(('flush', '', []))

    # 关闭写入线程 & 输出 JSON
    writer.close()
    # 聚合 infos 写 JSON
    for (region, subregion), infos in writer.infos_agg.items():
        write_json_dataset(INFORMATION_PATH, region, subregion, infos)
        print(f'{region}/{subregion} 完成，保存 {len(infos)} 条数据。')


if __name__ == '__main__':
    main()
