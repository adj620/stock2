"""
stock_utils.py - 주식 관련 공통 유틸리티 함수
update_holdings.py, compare_holdings.py 등에서 공유
"""

import os
import json
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd

# ============================================================
# 전역 캐시 및 마스터 데이터
# ============================================================

STOCK_MASTER = {}

def load_stock_master():
    """전역 종목 마스터 데이터 로드"""
    global STOCK_MASTER
    # src/stock_utils.py 기준 ../data/stock_master.json
    master_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "stock_master.json")
    if os.path.exists(master_path):
        try:
            with open(master_path, "r", encoding="utf-8") as f:
                STOCK_MASTER = json.load(f)
        except Exception:
            STOCK_MASTER = {}

# 모듈 로드 시 자동 실행
load_stock_master()


def _get_code_from_master(name):
    """로컬 마스터 테이블에서 종목명으로 코드 검색"""
    if not name: return None
    if name in STOCK_MASTER:
        return STOCK_MASTER[name].get("code")
    name_clean = name.replace(" ", "").upper()
    for k, v in STOCK_MASTER.items():
        if k.replace(" ", "").upper() == name_clean:
            return v.get("code")
    return None


def _get_sector_from_master(name_or_code):
    """로컬 마스터 테이블에서 업종 정보 검색"""
    if not name_or_code: return None
    if name_or_code in STOCK_MASTER:
        return STOCK_MASTER[name_or_code].get("sector")
    for k, v in STOCK_MASTER.items():
        if v.get("code") == name_or_code:
            return v.get("sector")
    return None


# ============================================================
# 종목코드 캐시 관리
# ============================================================

def _load_code_cache() -> dict:
    """로컬 종목코드 캐시 파일 로드"""
    cache_path = os.path.join(os.getcwd(), "data", "krx_code_cache.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_code_cache(cache: dict) -> None:
    """로컬 종목코드 캐시 파일 저장"""
    cache_path = os.path.join(os.getcwd(), "data", "krx_code_cache.json")
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[경고] 종목코드 캐시 저장 실패: {e}")


# ============================================================
# 네이버 금융 크롤링 함수들
# ============================================================

def _search_code_from_naver(name: str) -> str:
    """네이버 금융에서 종목명으로 종목코드 검색 (마스터 테이블 우선 탐색)"""
    if not name or len(name) < 2:
        return ""
    
    # 0. 마스터 테이블에서 먼저 찾기 (가장 빠르고 정확함)
    master_code = _get_code_from_master(name)
    if master_code: return master_code
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        # 네이버 금융 종목 페이지에서 검색 (og:url에서 코드 추출)
        search_url = f"https://finance.naver.com/search/searchList.naver?query={name}"
        res = requests.get(search_url, headers=headers, timeout=10)
        if res.status_code != 200:
            return ""
        
        soup = BeautifulSoup(res.text, "html.parser")
        
        # 검색 결과에서 정확히 일치하는 종목 찾기
        for tr in soup.select("table.tbl_search tbody tr"):
            name_td = tr.select_one("td.tit a")
            if name_td:
                found_name = name_td.get_text(strip=True)
                href = name_td.get("href", "")
                if "code=" in href:
                    code = href.split("code=")[-1].split("&")[0]
                    if code.isdigit() and len(code) == 6:
                        # 정확히 일치하거나 포함 관계
                        if found_name == name or name in found_name or found_name in name:
                            return code
        
        # 첫 번째 결과라도 반환
        first_link = soup.select_one("table.tbl_search td.tit a")
        if first_link:
            href = first_link.get("href", "")
            if "code=" in href:
                code = href.split("code=")[-1].split("&")[0]
                if code.isdigit() and len(code) == 6:
                    return code
        
        return ""
    except Exception:
        return ""


def _get_code_for_name(name: str, cache: dict) -> str:
    """종목명으로 코드 조회 (마스터 테이블 -> 캐시 -> 네이버 검색)"""
    if not name:
        return ""
    
    # 0. 마스터 테이블 우선
    master_code = _get_code_from_master(name)
    if master_code: return master_code
    
    # 1. 캐시에서 조회
    if name in cache:
        return cache[name]
    
    # 2. 네이버에서 검색
    code = _search_code_from_naver(name)
    if code:
        cache[name] = code
        _save_code_cache(cache)  # 즉시 저장
        return code
    
    return ""


# K-OTC 등 데이터 조회 불가 종목을 위한 수동 가격 설정
KOTC_MANUAL_PRICES = {
    "102600": 64100.0, # 메가젠임플란트 (2025-12-08 기준)
}


def get_naver_current_price(code: str, name: str = "") -> float | None:
    """
    네이버 금융에서 현재가 크롤링 (FDR 실패 시 폴백용)
    
    주의: 네이버는 '현재가'만 제공합니다. 과거 날짜 종가는 FDR을 사용하세요.
    
    Args:
        code: 종목코드 (6자리) 또는 종목명
        name: 종목명 (코드로 조회 실패 시 이름으로 재시도)
    
    Returns:
        현재가 (float) 또는 None
    """
    # 1. 수동 설정 확인
    if code in KOTC_MANUAL_PRICES:
        return KOTC_MANUAL_PRICES[code]
    
    def _fetch_from_naver(target_code):
        if not target_code or len(target_code) < 1: return None 
        
        # 코드가 숫자 6자리가 아니면 이름으로 간주하고 마스터 테이블 우선 검색
        url = None
        if not (isinstance(target_code, str) and target_code.isdigit() and len(target_code) == 6):
            master_code = _get_code_from_master(target_code)
            if master_code:
                target_code = master_code
            else:
                url = f"https://finance.naver.com/search/searchList.naver?query={target_code}"
        
        # 이제 target_code가 숫자 6자리로 확보되었거나, 검색 URL이 설정됨
        if url is None and isinstance(target_code, str) and target_code.isdigit() and len(target_code) == 6:
            url = f"https://finance.naver.com/item/main.naver?code={target_code}"
            
        if not url:
            return None
            
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code != 200: return None
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Method 0: 사용자가 제안한 dl.blind 방식 (가장 안정적)
            dl_blind = soup.select_one("dl.blind")
            if dl_blind:
                text = dl_blind.get_text(" ", strip=True) # 공백을 구분자로 사용하여 단어 붙음 방지
                m = re.search(r'현재가\s+([\d,]+)', text)
                if m:
                    return float(m.group(1).replace(",", ""))
                m2 = re.search(r'현재가([\d,]+)', text)
                if m2:
                    return float(m2.group(1).replace(",", ""))

            # Method 1: 투자자별 매매동향 테이블 (table.tb_type1) - 추가
            tb_type1 = soup.select_one("table.tb_type1")
            if tb_type1:
                first_row = tb_type1.select_one("tbody tr:not(.space) td em")
                if first_row:
                    price_text = first_row.get_text(strip=True).replace(",", "")
                    if price_text.isdigit():
                        return float(price_text)

            # Method 2: 실시간 가격 태그 (.no_today) - 기존 백업
            price_tag = soup.select_one(".no_today .blind")
            if price_tag:
                p_text = price_tag.get_text(strip=True).replace(",", "")
                if p_text.isdigit(): return float(p_text)
                
            # 검색 결과 페이지인 경우 첫번째 종목으로 이동 시도
            if "searchList.naver" in res.url:
                first_stock = soup.select_one("table.tbl_search tbody tr td.tit a")
                if first_stock:
                    href = first_stock.get("href", "")
                    m_code = re.search(r'code=(\d{6})', href)
                    if m_code:
                        return _fetch_from_naver(m_code.group(1))

            return None
        except Exception: return None

    # 2-1. 코드로 직접 조회
    price = _fetch_from_naver(code)
    if price: return price

    # 2-2. 코드 조회 실패 시 이름으로 검색 시도 (종목코드가 잘못되었을 가능성)
    if name:
        # _get_code_for_name 함수 활용 (캐시 및 네이버 검색)
        name_to_code = _load_code_cache()
        found_code = _get_code_for_name(name, name_to_code)
        if found_code and found_code != code:
            print(f"[Fallback/Search] {name}의 새로운 코드 발견: {found_code}")
            return _fetch_from_naver(found_code)
            
    return None


# ============================================================
# KRX API 직접 조회 (FDR 실패 시 복구용)
# ============================================================

KRX_COOKIE = ""  # 필요 시 KRX 쿠키 설정

def fetch_krx_listings_custom(cookie_str: str = None) -> pd.DataFrame:
    """KRX의 새로운 API 경로를 직접 사용하여 상장사 목록 획득"""
    url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    payload = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01901', 
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'share': '1',
        'csvxls_isNo': 'false',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020201',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://data.krx.co.kr',
    }
    if cookie_str or KRX_COOKIE:
        headers['Cookie'] = cookie_str or KRX_COOKIE

    try:
        resp = requests.post(url, data=payload, headers=headers, timeout=10)
        if resp.status_code == 200:
            json_data = resp.json()
            if 'OutBlock_1' in json_data:
                df = pd.DataFrame(json_data['OutBlock_1'])
                df = df.rename(columns={'ISU_SRT_CD': 'Code', 'ISU_ABBRV': 'Name'})
                return df
        return pd.DataFrame()
    except Exception as e:
        print(f"[KRX 복구 경고] 커스텀 페처 실행 중 오류: {e}")
        return pd.DataFrame()


# ============================================================
# 통합 현재가 조회 함수 (핵심)
# ============================================================

def get_stock_price(
    code: str,
    name: str = "",
    target_date: str = None,
    use_fdr: bool = True,
    silent: bool = False
) -> dict:
    """
    종목 가격 조회 통합 함수
    
    우선순위:
    1. FDR (FinanceDataReader) - 과거 날짜 종가 지원
    2. 네이버 크롤링 폴백 - 현재가만 지원 (과거 날짜 지정 시 경고)
    
    Args:
        code: 종목코드 (6자리) 또는 심볼
        name: 종목명 (코드가 없을 때 코드 탐색용)
        target_date: 조회할 날짜 (YYYYMMDD 또는 YYYY-MM-DD 형식, None이면 오늘)
        use_fdr: FDR 사용 여부 (False면 네이버만 사용)
        silent: True면 로그 출력 안함
    
    Returns:
        dict: {
            "price": float | None,       # 조회된 가격
            "source": str,               # "fdr", "naver", "manual", "none"
            "date": str,                 # 실제 조회된 날짜 (YYYY-MM-DD)
            "df": pd.DataFrame | None,   # FDR 데이터 (1년치, 52주 지표 계산용)
        }
    """
    result = {"price": None, "source": "none", "date": None, "df": None}
    
    # 날짜 처리
    if target_date:
        end_date = target_date
        end_dt = datetime.strptime(end_date, "%Y-%m-%d" if "-" in end_date else "%Y%m%d")
    else:
        end_dt = datetime.now()
        end_date = end_dt.strftime("%Y%m%d")
    
    # 과거 날짜 여부 판단
    today_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    is_past_date = end_dt < today_dt
    
    # 심볼 결정 (code 또는 name으로 코드 탐색)
    sym = code
    if not sym and name:
        cache = _load_code_cache()
        sym = _get_code_for_name(name, cache)
    
    if not sym:
        if not silent:
            print(f"[경고] 종목코드를 찾을 수 없음: {name or code}")
        return result
    
    # 1. FDR 조회 시도
    if use_fdr:
        try:
            import FinanceDataReader as fdr
            
            start_dt = end_dt - timedelta(days=370)  # 1년+ 데이터
            start_date = start_dt.strftime("%Y-%m-%d")
            end_date_str = end_dt.strftime("%Y-%m-%d")
            
            df_px = None
            try:
                df_px = fdr.DataReader(sym, start_date, end_date_str)
            except Exception:
                df_px = None
            
            # 주말/휴일 대비 재시도
            if df_px is None or df_px.empty:
                try:
                    retry_start = (end_dt - timedelta(days=7)).strftime("%Y-%m-%d")
                    df_px = fdr.DataReader(sym, retry_start, end_date_str)
                except Exception:
                    df_px = None
            
            if df_px is not None and not df_px.empty and "Close" in df_px.columns:
                # target_date에 해당하는 종가 찾기
                target_date_str = end_dt.strftime("%Y-%m-%d")
                
                if hasattr(df_px.index, 'strftime'):
                    date_match = df_px.index.strftime("%Y-%m-%d") == target_date_str
                    if date_match.any():
                        last_row = df_px[date_match].iloc[-1]
                        actual_date = target_date_str
                    else:
                        last_row = df_px.iloc[-1]
                        actual_date = df_px.index[-1].strftime("%Y-%m-%d") if hasattr(df_px.index[-1], 'strftime') else str(df_px.index[-1])
                        if not silent:
                            print(f"[FDR] {name or sym} 지정일({target_date_str}) 데이터 없음 → 최근 거래일({actual_date}) 종가 사용")
                else:
                    last_row = df_px.iloc[-1]
                    actual_date = end_date_str
                
                result["price"] = float(last_row["Close"])
                result["source"] = "fdr"
                result["date"] = actual_date
                result["df"] = df_px
                
                # 정상 조회 - 로그 생략
                
                return result
                
        except ImportError:
            if not silent:
                print("[FDR 경고] FinanceDataReader 모듈을 찾을 수 없습니다.")
        except Exception as e:
            if not silent:
                print(f"[FDR 경고] {name or sym}({sym}) 데이터 조회 실패: {str(e).split(chr(10))[0]}")
    
    # 2. 네이버 크롤링 폴백
    if not silent:
        print(f"\n⚠️  [FDR 실패] {name or sym}({sym}) FDR 데이터 조회 실패 - 네이버 크롤링으로 폴백")
        if is_past_date:
            print(f"   ⚠️  주의: 과거 날짜({end_date}) 지정됨 - 네이버는 '현재가'만 제공하므로 정확하지 않을 수 있습니다.")
    
    naver_price = get_naver_current_price(sym, name)
    if naver_price:
        result["price"] = naver_price
        result["source"] = "naver"
        result["date"] = datetime.now().strftime("%Y-%m-%d")
        
        if not silent:
            print(f"   ✅ [Fallback/Naver] {name or sym}({sym}) 네이버 현재가 사용: {naver_price:,.0f}원")
    else:
        if not silent:
            print(f"   ❌ [FDR/Naver 최종 실패] {name or sym}({sym}) 현재가 업데이트 불가")
    
    return result
