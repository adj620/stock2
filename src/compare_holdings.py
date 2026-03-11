import sys
import os
import pandas as pd
import argparse
import gspread
from google.oauth2.service_account import Credentials
import json
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta

def _normalize_num(val):
    if pd.isna(val) or val == "":
        return 0.0
    if isinstance(val, str):
        val = val.replace(",", "").replace("%", "").strip()
        try:
            return float(val)
        except ValueError:
            return 0.0
    return float(val)

def _load_code_cache():
    cache_path = os.path.join(os.getcwd(), "data", "krx_code_cache.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

# 전역 종목 마스터 데이터 캐시
STOCK_MASTER = {}
def load_stock_master():
    global STOCK_MASTER
    # src/compare_holdings.py 기준 ../data/stock_master.json
    master_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "stock_master.json")
    if os.path.exists(master_path):
        try:
            with open(master_path, "r", encoding="utf-8") as f:
                STOCK_MASTER = json.load(f)
        except Exception:
            STOCK_MASTER = {}

load_stock_master()

def _get_code_from_master(name):
    """로컬 마스터 테이블에서 종목명으로 코드 검색"""
    if not name: return None
    # 1. 정확한 매칭
    if name in STOCK_MASTER:
        return STOCK_MASTER[name].get("code")
    # 2. 공백 제거 후 매칭
    name_clean = name.replace(" ", "").upper()
    for k, v in STOCK_MASTER.items():
        if k.replace(" ", "").upper() == name_clean:
            return v.get("code")
    return None

def _get_sector_from_master(name_or_code):
    """로컬 마스터 테이블에서 업종 정보 검색"""
    if not name_or_code: return None
    # 이름으로 찾기
    if name_or_code in STOCK_MASTER:
        return STOCK_MASTER[name_or_code].get("sector")
    # 코드로 찾기
    for k, v in STOCK_MASTER.items():
        if v.get("code") == name_or_code:
            return v.get("sector")
    return None

def _save_code_cache(cache):
    cache_path = os.path.join(os.getcwd(), "data", "krx_code_cache.json")
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# --- [KRX API Recovery] ---
# KRX 서버의 강화된 보안 정책으로 인해 FDR 라이브러리가 실패할 경우 사용하는 수동 복구 로직
# 브라우저에서 'http://data.krx.co.kr' 접속 후 'getJsonData.cmd' 요청의 Cookie 값을 아래에 붙여넣으면 성능이 향상됩니다.
KRX_COOKIE = "__smVisitorID=-OpIBIgkS-e; JSESSIONID=SafYIfSrbKlOjdgLoEbSNWqdmkeMeZDjMhChjFQePaXve0nVtjXY2t5k6BWVgeY8.bWRjX2RvbWFpbi9tZGNvd2FwMS1tZGNhcHAxMQ==; lang=ko_KR; npPfsHost=127.0.0.1; npPfsPort=14440; successJoinId=q4config; successJoinName=%EA%B9%80%EC%98%81%EB%B0%95; successJoinEmail=q4config%40naver.com; mdc.client_session=true"

def _fetch_krx_listings_custom(cookie_str: str = None) -> pd.DataFrame:
    """KRX의 새로운 API 경로(dbms/MDC/STAT/standard/MDCSTAT01901)를 직접 사용하여 상장사 목록 획득"""
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
                # FDR과 컬럼명 호환성 유지 (Symbol, Name)
                df = df.rename(columns={'ISU_SRT_CD': 'Code', 'ISU_ABBRV': 'Name'})
                return df
        # 실패 시 조용히 빈 DataFrame 반환 (FDR fallback 사용)
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def _search_code_from_naver(name):
    if not name or len(name) < 2:
        return ""
    
    # 0. 마스터 테이블에서 먼저 찾기 (가장 빠르고 정확함)
    code = _get_code_from_master(name)
    if code: return code
    try:
        import urllib.parse
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        # 1. UTF-8 검색 (requests 자동 인코딩)
        try:
            res = requests.get("https://finance.naver.com/search/searchList.naver", params={"query": name}, headers=headers, timeout=10)
            if "code=" in res.url:
                m = re.search(r'code=(\d{6})', res.url)
                if m: return m.group(1)
            
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, "html.parser")
                # 검색 결과 목록에서 정확히 일치하는 이름 찾기
                name_clean = name.replace(" ", "").upper()
                for tr in soup.select("table.tbl_search tbody tr"):
                    name_td = tr.select_one("td.tit a")
                    if name_td:
                        found_name = name_td.get_text(strip=True).replace(" ", "").upper()
                        href = name_td.get("href", "")
                        if "code=" in href:
                            code = href.split("code=")[-1].split("&")[0]
                            if code.isdigit() and len(code) == 6:
                                if found_name == name_clean:
                                    return code
        except Exception: pass

        # 2. EUC-KR 검색 (수동 인코딩) - 네이버 서버 호환성
        try:
            encoded_val = name.encode('euc-kr')
            res = requests.get(f"https://finance.naver.com/search/searchList.naver?query={urllib.parse.quote(encoded_val)}", headers=headers, timeout=10)
            if "code=" in res.url:
                m = re.search(r'code=(\d{6})', res.url)
                if m: return m.group(1)
            
            if res.status_code == 200:
                content = res.content.decode('euc-kr', 'ignore')
                soup = BeautifulSoup(content, "html.parser")
                name_clean = name.replace(" ", "").upper()
                for tr in soup.select("table.tbl_search tbody tr"):
                    name_td = tr.select_one("td.tit a")
                    if name_td:
                        found_name = name_td.get_text(strip=True).replace(" ", "").upper()
                        href = name_td.get("href", "")
                        if "code=" in href:
                            code = href.split("code=")[-1].split("&")[0]
                            if code.isdigit() and len(code) == 6:
                                if found_name == name_clean:
                                    return code
        except Exception: pass
        
        return ""
    except Exception:
        return ""
    except Exception:
        return ""

def _get_code_for_name(name, cache):
    """종목명으로 코드 조회 - 우선순위: 1) stock_master.json, 2) 캐시, 3) 네이버 검색"""
    if not name:
        return ""
    
    # 1. stock_master.json에서 먼저 찾기 (가장 정확함)
    master_code = _get_code_from_master(name)
    if master_code:
        # 캐시와 다르면 캐시 업데이트
        if name in cache and cache[name] != master_code:
            print(f"[정보] 캐시 수정: {name} {cache[name]} -> {master_code} (stock_master 기준)")
            cache[name] = master_code
            _save_code_cache(cache)
        return master_code
    
    # 2. 캐시에서 찾기
    if name in cache:
        return cache[name]
    
    # 3. 네이버 검색 (마스터/캐시에 없는 경우)
    code = _search_code_from_naver(name)
    if code:
        cache[name] = code
        _save_code_cache(cache)
        return code
    return ""

def _get_naver_price(code):
    """현재가 크롤링 (FDR 실패 시 fallback) - 다중 방법 시도"""
    if not code: return None
    
    # 코드가 숫자 6자리가 아니면 이름으로 간주하고 마스터 테이블/검색 처리
    if not (isinstance(code, str) and code.isdigit() and len(code) == 6):
        master_code = _get_code_from_master(code)
        if master_code:
            code = master_code
        else:
            url = f"https://finance.naver.com/search/searchList.naver?query={code}"
    
    # 이제 code가 확보되었거나, url이 설정됨
    if isinstance(code, str) and code.isdigit() and len(code) == 6:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code != 200: return None
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # === Method 1: dl.blind 방식 (가장 안정적) ===
        dl_blind = soup.select_one("dl.blind")
        if dl_blind:
            text = dl_blind.get_text(" ", strip=True)
            # 패턴 1: "현재가 128,500"
            m = re.search(r'현재가\s+([\d,]+)', text)
            if m:
                return float(m.group(1).replace(",", ""))
            # 패턴 2: "현재가128,500" (공백 없음)
            m2 = re.search(r'현재가([\d,]+)', text)
            if m2:
                return float(m2.group(1).replace(",", ""))

        # === Method 2: 투자자별 매매동향 테이블 (table.tb_type1) ===
        # 첫 번째 종가 데이터 (가장 최근 거래일)
        tb_type1 = soup.select_one("table.tb_type1")
        if tb_type1:
            first_row = tb_type1.select_one("tbody tr:not(.space) td em")
            if first_row:
                price_text = first_row.get_text(strip=True).replace(",", "")
                if price_text.isdigit():
                    return float(price_text)

        # === Method 3: 실시간 가격 태그 (.no_today) ===
        price_tag = soup.select_one(".no_today .blind")
        if price_tag:
            p_text = price_tag.get_text(strip=True).replace(",", "")
            if p_text.isdigit(): 
                return float(p_text)
        
        # === Method 4: sise_left의 현재가 영역 ===
        today_tag = soup.select_one("#chart_area .rate_info .blind")
        if today_tag:
            text = today_tag.get_text(strip=True)
            m = re.search(r'([\d,]+)', text)
            if m:
                return float(m.group(1).replace(",", ""))
            
        # 검색 결과 페이지인 경우 첫번째 종목으로 이동 시도
        if "searchList.naver" in res.url:
            first_stock = soup.select_one("table.tbl_search tbody tr td.tit a")
            if first_stock:
                href = first_stock.get("href", "")
                m = re.search(r'code=(\d{6})', href)
                if m:
                    return _get_naver_price(m.group(1))

        return None
    except Exception as e:
        # print(f"[디버그] 크롤링 에러: {e}")
        return None

def read_sheet_ranges(spreadsheet_key, sheet_title, cred_path):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if not os.path.exists(cred_path):
        # 상대 경로 대응
        cred_path = os.path.join(os.getcwd(), cred_path)
    
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_key)
    
    try:
        ws = sh.worksheet(sheet_title)
    except Exception:
        print(f"[경고] '{sheet_title}' 탭을 찾을 수 없습니다.")
        return None, None

    all_values = ws.get_all_values()
    if not all_values:
        return None, None

    df_full = pd.DataFrame(all_values)
    
    # 컬럼 수가 부족한 경우 예외 처리
    if df_full.shape[1] < 19:
        print(f"[경고] 시트의 열 개수가 부족합니다. (현재: {df_full.shape[1]}열, 필요: 19열 이상)")
        # 최소한 A-I열(9열)이라도 있으면 왼쪽은 가져옴
        df_left_raw = df_full.iloc[:, 0:min(9, df_full.shape[1])]
        df_right_raw = pd.DataFrame() if df_full.shape[1] <= 10 else df_full.iloc[:, 10:min(19, df_full.shape[1])]
    else:
        df_left_raw = df_full.iloc[:, 0:9]
        df_right_raw = df_full.iloc[:, 10:19]

    def process_table(df):
        if df.empty: return pd.DataFrame()
        # 헤더 찾기 (종목명이 포함된 행)
        header_row_idx = None
        for i, row in df.iterrows():
            if any("종목명" in str(v) for v in row.values):
                header_row_idx = i
                break
        
        if header_row_idx is None:
            return pd.DataFrame()
            
        new_df = df.iloc[header_row_idx+1:].copy()
        new_df.columns = df.iloc[header_row_idx].values
        # 종목명이 비어있는 행 제거
        new_df = new_df[new_df.iloc[:, 0].astype(str).str.strip() != ""]
        return new_df.reset_index(drop=True)

    left_holdings = process_table(df_left_raw)
    right_holdings = process_table(df_right_raw)
    
    return left_holdings, right_holdings

def compare_tables(left, right, cred_path):
    # 컬럼명 유연 매핑
    def find_col(df, keywords):
        for col in df.columns:
            if any(k in str(col) for k in keywords):
                return col
        return None

    col_map = {
        "name": find_col(left, ["종목명"]),
        "qty": find_col(left, ["잔고수량", "수량"]),
        "avg": find_col(left, ["손익단가", "단가"]),
        "eval": find_col(left, ["평가금액"]),
        "weight_eval": find_col(left, ["평가비중"])
    }
    
    # 필수 컬럼(명칭) 확인
    if not col_map["name"]: 
        print("[오류] '종목명' 컬럼을 찾을 수 없습니다.")
        return pd.DataFrame()

    # 내부 변수용 이름으로 리네임
    left_sub = left.rename(columns={col_map[k]: k for k in col_map if col_map[k] is not None})
    
    r_cols = {
        "name": find_col(right, ["종목명"]),
        "qty": find_col(right, ["잔고수량", "수량"]),
        "avg": find_col(right, ["손익단가", "단가"]),
        "eval": find_col(right, ["평가금액"]),
        "weight_eval": find_col(right, ["평가비중"])
    }
    right_sub = right.rename(columns={r_cols[k]: k for k in r_cols if r_cols[k] is not None})
    
    # 수치 데이터 정규화
    num_cols = ["qty", "avg", "eval", "weight_eval"]
    for col in num_cols:
        if col in left_sub.columns: left_sub[col] = left_sub[col].apply(_normalize_num)
        if col in right_sub.columns: right_sub[col] = right_sub[col].apply(_normalize_num)

    # 종목명 기준 병합
    merged = pd.merge(left_sub[["name", "qty", "avg", "eval", "weight_eval"]], 
                      right_sub[["name", "qty", "avg", "eval", "weight_eval"]], 
                      on="name", how="outer", suffixes=("_L", "_R")).fillna(0)
    
    # 현재가 조회
    current_prices = {}
    try:
        import FinanceDataReader as fdr
        
        # 1단계: 캐시 로드
        print("[1/3] 종목코드 캐시 로드 중...")
        name_to_code = _load_code_cache()
        
        # 2단계: FDR 상장사 목록으로 캐시 보강
        print("[2/3] 상장사 목록 업데이트 중...")
        updated_cache = False
        try:
            listings = []
            try:
                df_krx = fdr.StockListing("KRX")
                if df_krx is not None and not df_krx.empty: 
                    listings.append(df_krx)
                    print(f"      └ KRX 상장사 {len(df_krx)}개 로드 완료")
                else:
                    raise ValueError("FDR returned empty KRX listing")
            except Exception:
                # FDR 실패 시 커스텀 페처 시도
                df_custom = _fetch_krx_listings_custom()
                if not df_custom.empty:
                    print(f"      └ KRX 서버 직접 조회로 {len(df_custom)}개 로드")
                    listings.append(df_custom)
                else:
                    print("      └ KRX 조회 실패, 개별 시장 조회 시도...")

            if not listings:
                for mkt in ["KOSPI", "KOSDAQ"]:
                    try:
                        df_mkt = fdr.StockListing(mkt)
                        if not df_mkt.empty: 
                            listings.append(df_mkt)
                            print(f"      └ {mkt} {len(df_mkt)}개 로드")
                    except Exception: continue
            
            if listings:
                listing = pd.concat(listings).drop_duplicates(subset=["Code"])
                for _, row in listing.iterrows():
                    n, c = str(row["Name"]).strip(), str(row["Code"]).strip()
                    if n and c:
                        if n not in name_to_code:
                            name_to_code[n] = c
                            updated_cache = True
                        # 공백 제거 버전도 추가하여 매칭율 향상
                        n_clean = n.replace(" ", "")
                        if n_clean not in name_to_code:
                            name_to_code[n_clean] = c
                            updated_cache = True
        except Exception:
            pass
        
        if updated_cache:
            _save_code_cache(name_to_code)
            print(f"      └ 캐시 업데이트 완료: {len(name_to_code)}개 종목")
        
        # 3단계: 현재가 조회 (공통 모듈 사용)
        from lib.stock_utils import get_stock_price
        
        target_names = merged["name"].unique()
        total_count = len(target_names)
        print(f"[3/3] 현재가 조회 시작 (총 {total_count}개 종목)")
        
        for idx, name in enumerate(target_names, 1):
            # 진행현황 표시 (매 1개마다 + 퍼센트)
            pct = idx * 100 // total_count
            print(f"\r[진행] 현재가 조회 중... {idx}/{total_count} ({pct}%)", end="", flush=True)
            
            clean_name = str(name).strip().replace(" ", "")
            code = _get_code_for_name(clean_name, name_to_code)
            if not code:
                # 원본 이름으로 한 번 더 시도
                code = _get_code_for_name(str(name).strip(), name_to_code)
            
            if code:
                # K-OTC 종목은 FDR/네이버에서 조회 불가 - 스킵
                SKIP_CODES = {"102600"}  # K-OTC 종목 리스트
                if code in SKIP_CODES:
                    continue
                
                # 공통 모듈로 가격 조회 (FDR 우선, 실패 시 네이버 폴백)
                price_result = get_stock_price(code, name, silent=True)
                if price_result["price"]:
                    current_prices[name] = price_result["price"]
            else:
                # 코드 자체를 못 찾은 경우 (이름으로 직접 검색 시도)
                price_result = get_stock_price("", name, silent=True)
                if price_result["price"]:
                    current_prices[name] = price_result["price"]
        
        print()  # 진행현황 줄바꿈
        print(f"[정보] 현재가 조회 완료: {len(current_prices)}개 종목")
    except Exception as e:
        print(f"\n[경고] 현재가 조회 실패: {e}")

    diff_results = []
    for _, row in merged.iterrows():
        name = row["name"]
        qty_L, qty_R = row["qty_L"], row["qty_R"]  # L=기준, R=내꺼
        avg_L, avg_R = row["avg_L"], row["avg_R"]
        
        qty_diff = qty_R - qty_L  # 내 수량 - 기준 수량 (양수=내가 더 많음)
        avg_gap = avg_R - avg_L   # 내 평단 - 기준 평단 (양수=불리, 음수=유리)
        
        # 현재가 조회
        cur_price = current_prices.get(name, 0)
        has_cur_price = cur_price > 0
        
        # === 기본 지표 계산 ===
        # 손익률 = (현재가 - 평단) / 평단 (분수 형태, 0.1 = 10%)
        pl_rate = ((cur_price - avg_R) / avg_R) if avg_R > 0 and has_cur_price else 0.0
        base_pl_rate = ((cur_price - avg_L) / avg_L) if avg_L > 0 and has_cur_price else 0.0
        
        is_profit = pl_rate > 0
        is_favorable = avg_R < avg_L  # 내 평단이 기준보다 낮음 (유리)
        
        # 손익률 차이 = 내 손익률 - 기준 손익률 (양수 = 내가 우세)
        pl_rate_diff = pl_rate - base_pl_rate if (avg_L > 0 and avg_R > 0) else None
        is_better_than_base = (pl_rate_diff is not None and pl_rate_diff > 0)
        
        # === 카테고리 분류 (재설계) ===
        category = "Z_미분류"
        
        # E: 기준에만 존재 (내가 미보유)
        if qty_R == 0 and qty_L > 0:
            category = "E_기준만_존재"
            pl_rate_diff = None
            avg_gap = None
        
        # D: 나만 보유 (기준에 없음)
        elif qty_L == 0 and qty_R > 0:
            category = "D_나만_보유"
            pl_rate_diff = None
            avg_gap = None
        
        # A: 유리함 - 손익률차 > 0 (손실중이더라도 기준보다 우세하면 유리)
        elif is_better_than_base:
            category = "A_유리함"
        
        # B: 평단맞춤 가능 - 내 평단 > 기준 평단이고 현재가 < 기준평단
        elif avg_R > avg_L and avg_L > 0 and has_cur_price and cur_price < avg_L:
            category = "B_평단맞춤_가능"
        
        # C: 평단맞춤 불가 - 내 평단 > 기준 평단이지만 현재가 >= 기준평단
        elif avg_R > avg_L and avg_L > 0:
            category = "C_평단맞춤_불가"
        
        # G: 손실중 - 나머지 (기준보다도 불리한 상황)
        else:
            category = "G_손실중"
        
        # === 미세차이 판정 (3% 이내) ===
        # 평단 차이가 기준 평단의 3% 이내면 "거의 일치"로 간주
        is_minor_diff = False
        if avg_L > 0 and category not in ["E_기준만_존재", "D_나만_보유"]:
            avg_diff_pct = abs(avg_R - avg_L) / avg_L
            if avg_diff_pct <= 0.03:  # 3% 이내
                is_minor_diff = True
        
        # === 평단 맞춤 계산 ===
        avg_match_shares = 0  # 평단 맞추기 위한 추가 매수 주수
        avg_match_cost = 0    # 필요 금액
        
        if category == "B_평단맞춤_가능" and has_cur_price and qty_R > 0 and not is_minor_diff:
            # 공식: (현재보유 × (기준평단 - 내평단)) / (현재가 - 기준평단)
            # 단, 현재가 < 기준평단 이어야 함
            if cur_price < avg_L:
                needed_x = (qty_R * (avg_L - avg_R)) / (cur_price - avg_L)
                if needed_x > 0:
                    avg_match_shares = int(needed_x)
                    avg_match_cost = int(avg_match_shares * cur_price)
        
        # === 수량 맞춤 계산 ===
        # A_유리함, D_나만_보유 카테고리는 수량맞춤 불필요
        qty_match_shares = 0
        qty_match_cost = 0
        
        if category not in ["A_유리함", "D_나만_보유"] and not is_minor_diff:
            qty_match_shares = int(qty_L - qty_R)  # 양수=매수필요, 음수=매도가능
            qty_match_cost = int(abs(qty_match_shares) * cur_price) if has_cur_price else 0
        
        # === 현금화 계산 (수정됨) ===
        # 현금화 가능금액 = 현재가 × 수량차 (내가 더 많이 가진 수량)
        # 조건: 수익 상태 + 내 수량 > 기준 수량
        cash_sell_shares = 0
        cash_amount = 0
        
        if qty_R > qty_L and is_profit and has_cur_price:
            cash_sell_shares = int(qty_R - qty_L)
            cash_amount = int(cur_price * cash_sell_shares)
        
        # === 우선순위 계산 ===
        if category == "B_평단맞춤_가능" and avg_match_cost > 0:
            priority = avg_match_cost
        elif category == "A_유리함" and cash_amount > 0:
            priority = -cash_amount  # 현금화 가능 금액이 클수록 우선 (음수로)
        elif category == "E_기준만_존재":
            # 기준손익률이 마이너스면 더 우선 (현재가 < 기준평단 = 지금 진입해도 유리)
            if base_pl_rate < 0:
                priority = int(base_pl_rate * 10000)  # 마이너스가 클수록 상단
            else:
                priority = qty_match_cost if qty_match_cost > 0 else 999999999
        elif category == "D_나만_보유":
            # 수익 중이면 상단, 손실 중이면 하단
            if is_profit:
                priority = -cash_amount if cash_amount > 0 else 0
            else:
                priority = 999999999
        else:
            priority = 999999999  # 우선순위 없음
        
        # === 미세차이는 별도 카테고리로 분류 ===
        if is_minor_diff and category not in ["E_기준만_존재", "D_나만_보유"]:
            category = "F_거의일치"
        
        diff_results.append({
            "카테고리": category,
            "종목명": name,
            "손익률차": round(pl_rate_diff, 4) if pl_rate_diff is not None else None,
            "기준손익률": round(base_pl_rate, 4),
            "내손익률": round(pl_rate, 4),
            "평단갭": int(avg_gap) if avg_gap is not None else None,
            "현재가": int(cur_price),
            "기준_단가": int(avg_L),
            "내_단가": int(avg_R),
            "수량차": int(qty_diff),
            "기준_수량": int(qty_L),
            "내_수량": int(qty_R),
            "수량맞춤_필요주수": qty_match_shares,
            "수량맞춤_필요금액": qty_match_cost,
            "평단맞춤_필요주수": avg_match_shares,
            "평단맞춤_필요금액": avg_match_cost,
            "현금화_주수": cash_sell_shares,
            "현금화_금액": cash_amount,
            
            # 비중(%) = (내 평가금액) / (전체 평가금액) - 나중에 계산
            "내_평가금액": int(qty_R * cur_price) if has_cur_price else 0,
        })
    
    # DataFrame 생성
    result_df = pd.DataFrame(diff_results)
    
    # 비중(%) 계산: 내 평가금액 / 전체 합계
    total_eval = result_df["내_평가금액"].sum()
    if total_eval > 0:
        result_df["비중(%)"] = (result_df["내_평가금액"] / total_eval * 100).round(2)
    else:
        result_df["비중(%)"] = 0.0
    
    # 내_평가금액 컬럼 제거 (비중 계산용이었음)
    result_df = result_df.drop(columns=["내_평가금액"])
    
    # === 카테고리별 커스텀 정렬 ===
    sorted_dfs = []
    
    # A: 내손익률 내림차순
    df_a = result_df[result_df["카테고리"] == "A_유리함"].sort_values(by="내손익률", ascending=False)
    sorted_dfs.append(df_a)
    
    # B: 수량차 오름차순 (마이너스가 위로)
    df_b = result_df[result_df["카테고리"] == "B_평단맞춤_가능"].sort_values(by="수량차", ascending=True)
    sorted_dfs.append(df_b)
    
    # C: 내손익률 내림차순 (수익 중인 것이 상단)
    df_c = result_df[result_df["카테고리"] == "C_평단맞춤_불가"].sort_values(by="내손익률", ascending=False)
    sorted_dfs.append(df_c)
    
    # D: 나만_보유 - 비중 내림차순
    df_d = result_df[result_df["카테고리"] == "D_나만_보유"].sort_values(by="비중(%)", ascending=False)
    sorted_dfs.append(df_d)
    
    # E: 기준손익률 오름차순 (마이너스가 위로 = 진입 유리)
    df_e = result_df[result_df["카테고리"] == "E_기준만_존재"].sort_values(by="기준손익률", ascending=True)
    sorted_dfs.append(df_e)
    
    # F: 내손익률 내림차순
    df_f = result_df[result_df["카테고리"] == "F_거의일치"].sort_values(by="내손익률", ascending=False)
    sorted_dfs.append(df_f)
    
    # G: 손실중 - 비중 내림차순
    df_g = result_df[result_df["카테고리"] == "G_손실중"].sort_values(by="비중(%)", ascending=False)
    sorted_dfs.append(df_g)
    
    # Z: 기타 (있다면)
    df_z = result_df[~result_df["카테고리"].isin(["A_유리함", "B_평단맞춤_가능", "C_평단맞춤_불가", "D_나만_보유", "E_기준만_존재", "F_거의일치", "G_손실중"])]
    sorted_dfs.append(df_z)
    
    # 합치기
    result_df = pd.concat(sorted_dfs, ignore_index=True)
    
    return result_df

def write_analysis_sheet(spreadsheet_key, title, df, cred_path):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_key)
    
    try:
        ws = sh.worksheet(title)
        sh.del_worksheet(ws)
    except Exception:
        pass
        
    rows, cols = (len(df) + 1), max(1, len(df.columns))
    ws = sh.add_worksheet(title=title, rows=str(max(100, rows)), cols=str(max(20, cols)))
    
    # 출력용 DataFrame (현재 모든 컬럼 출력)
    export_df = df.copy()
    
    # NaT 또는 NaN을 빈 문자열로 처리하되 숫자는 유지
    values = [list(export_df.columns)] + export_df.fillna("").values.tolist()
    # value_input_option='USER_ENTERED'를 사용하여 숫자 데이터 유지
    ws.update(values=values, range_name="A1", value_input_option='USER_ENTERED')
    
    requests = []
    
    # 1. 헤더 서식 및 틀 고정
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": len(export_df.columns)},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2}, "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}, "bold": True}, "horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
        }
    })
    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 2}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
        }
    })
    
    # 2. 카테고리별 행 색상 적용
    category_colors = {
        "A_유리함": {"red": 0.85, "green": 0.95, "blue": 0.85},      # 연두색
        "B_평단맞춤_가능": {"red": 0.85, "green": 0.92, "blue": 1.0},  # 연한 파랑
        "C_평단맞춤_불가": {"red": 1.0, "green": 0.95, "blue": 0.8},   # 연한 노랑
        "D_나만_보유": {"red": 0.9, "green": 0.95, "blue": 1.0},      # 연한 하늘색
        "E_기준만_존재": {"red": 1.0, "green": 0.92, "blue": 0.85},    # 연한 주황
        "F_거의일치": {"red": 0.95, "green": 0.95, "blue": 0.95},     # 연한 회색
        "G_손실중": {"red": 1.0, "green": 0.88, "blue": 0.88},        # 연한 빨강
    }
    
    # 카테고리별 정렬 기준 컬럼 매핑
    sort_column_map = {
        "A_유리함": "내손익률",
        "B_평단맞춤_가능": "수량차",
        "C_평단맞춤_불가": "내손익률",
        "D_나만_보유": "비중(%)",
        "E_기준만_존재": "기준손익률",
        "F_거의일치": "내손익률",
        "G_손실중": "비중(%)",
    }
    
    header_list = list(export_df.columns)
    
    if "카테고리" in df.columns:
        for idx, row in df.iterrows():
            category = row["카테고리"]
            if category in category_colors:
                color = category_colors[category]
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": ws.id, "startRowIndex": idx + 1, "endRowIndex": idx + 2, "startColumnIndex": 0, "endColumnIndex": len(export_df.columns)},
                        "cell": {"userEnteredFormat": {"backgroundColor": color}},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })
            
            # 정렬 기준 컬럼 강조 (진한 파란색)
            if category in sort_column_map:
                sort_col = sort_column_map[category]
                if sort_col in header_list:
                    sort_col_idx = header_list.index(sort_col)
                    requests.append({
                        "repeatCell": {
                            "range": {"sheetId": ws.id, "startRowIndex": idx + 1, "endRowIndex": idx + 2, "startColumnIndex": sort_col_idx, "endColumnIndex": sort_col_idx + 1},
                            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": 0.1, "green": 0.3, "blue": 0.6}, "bold": True}}},
                            "fields": "userEnteredFormat.textFormat"
                        }
                    })
            
            # A_유리함, C_평단맞춤_불가에서 내손익률 양수인 경우 종목명 연한 빨강 색상
            if category in ["A_유리함", "C_평단맞춤_불가"]:
                my_pl = row.get("내손익률", 0)
                if pd.notna(my_pl) and my_pl > 0 and "종목명" in export_df.columns:
                    name_col_idx = header_list.index("종목명")
                    requests.append({
                        "repeatCell": {
                            "range": {"sheetId": ws.id, "startRowIndex": idx + 1, "endRowIndex": idx + 2, "startColumnIndex": name_col_idx, "endColumnIndex": name_col_idx + 1},
                            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0.4, "blue": 0.4}, "bold": True}}},
                            "fields": "userEnteredFormat.textFormat"
                        }
                    })
    
    # 3. 손익률 색상 적용 (양수=빨강, 음수=파랑) - 손익률차 컬럼에 적용
    for pl_col_name in ["손익률차", "내손익률", "기준손익률"]:
        if pl_col_name in export_df.columns:
            pl_col_idx = list(export_df.columns).index(pl_col_name)
            for idx, row in df.iterrows():
                pl_rate = row.get(pl_col_name, 0)
                if pd.isna(pl_rate):
                    continue
                if pl_rate > 0:
                    color = {"red": 0.8, "green": 0.2, "blue": 0.2}  # 빨강 (수익)
                elif pl_rate < 0:
                    color = {"red": 0.2, "green": 0.2, "blue": 0.8}  # 파랑 (손실)
                else:
                    continue
                
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": ws.id, "startRowIndex": idx + 1, "endRowIndex": idx + 2, "startColumnIndex": pl_col_idx, "endColumnIndex": pl_col_idx + 1},
                        "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": color, "bold": True}}},
                        "fields": "userEnteredFormat.textFormat"
                    }
                })
    
    # 4. 서브 데이터 컬럼 흐림 처리 (차이 컬럼 뒤의 기준/내 데이터)
    # 기준손익률, 내손익률 (손익률차 뒤), 기준_단가, 내_단가 (평단갭 뒤), 기준_수량, 내_수량 (수량차 뒤)
    dim_columns = ["기준손익률", "내손익률", "기준_단가", "내_단가", "기준_수량", "내_수량"]
    header_list = list(export_df.columns)
    
    for col_name in dim_columns:
        if col_name in header_list:
            col_idx = header_list.index(col_name)
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(df) + 1, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
                    "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": 0.6, "green": 0.6, "blue": 0.6}}}},
                    "fields": "userEnteredFormat.textFormat.foregroundColor"
                }
            })
    
    # 5. 열 너비 설정 (새 컬럼 구조에 맞게)
    # 카테고리, 종목명, 손익률차, 기준손익률, 내손익률, 평단갭, 현재가, 기준_단가, 내_단가, 수량차, 기준_수량, 내_수량, ...
    col_widths = [110, 120, 75, 75, 70, 70, 75, 70, 65, 60, 60, 55, 85, 95, 85, 95, 70, 90]
    
    for i, w in enumerate(col_widths):
        if i >= len(export_df.columns): break
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                "properties": {"pixelSize": w},
                "fields": "pixelSize"
            }
        })
    
    # 5. 숫자 서식 적용
    header_list = list(export_df.columns)
    number_format_map = {
        "기준손익률": "0.00%",
        "내손익률": "0.00%",
        "손익률차": "0.00%",
        "비중(%)": "0.00",
        "기준_수량": "#,##0",
        "내_수량": "#,##0",
        "수량차": "#,##0",
        "수량맞춤_필요주수": "#,##0",
        "수량맞춤_필요금액": "#,##0",
        "기준_단가": "#,##0",
        "내_단가": "#,##0",
        "평단갭": "#,##0",
        "평단맞춤_필요주수": "#,##0",
        "평단맞춤_필요금액": "#,##0",
        "현재가": "#,##0",
        "현금화_주수": "#,##0",
        "현금화_금액": "#,##0",
    }
    
    for col_name, fmt in number_format_map.items():
        if col_name in header_list:
            col_idx = header_list.index(col_name)
            # 손익률은 실제로 퍼센트가 아닌 숫자이므로 NUMBER 타입 사용
            num_type = "PERCENT" if "%" in col_name and "%" in fmt else "NUMBER"
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(df) + 1, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": num_type, "pattern": fmt}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

    if requests:
        sh.batch_update({"requests": requests})
    
    print(f"[정보] 분석 결과가 '{title}' 탭에 저장되었습니다.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gs-key", required=True, help="스프레드시트 키")
    parser.add_argument("--gs-cred", default="config/stock-holding-log-db46e6d87dd6.json")
    args = parser.parse_args()

    source_tab = "비교데이터"
    print(f"[정보] '{source_tab}' 탭의 좌우 테이블 비교를 시작합니다.")
    left, right = read_sheet_ranges(args.gs_key, source_tab, args.gs_cred)
    
    if left is None or left.empty:
        print("[오류] 비교할 데이터를 가져오지 못했습니다. '비교데이터' 탭이 존재하는지 확인하세요.")
        return

    diff_df = compare_tables(left, right, args.gs_cred)
    
    if diff_df.empty:
        print("[확인] 두 테이블이 완벽하게 일치합니다!")
        return

    print(f"[정보] {len(diff_df)}개 종목에서 차이가 발견되었습니다.")
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    analysis_title = f"분석_비교데이터_{timestamp}"
    write_analysis_sheet(args.gs_key, analysis_title, diff_df, args.gs_cred)

if __name__ == "__main__":
    main()
