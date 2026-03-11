"""
compare_three_way.py - [기준] vs [대신] vs [키움] 3자 잔고 통합 비교
기준계좌를 중심으로 대신(내계좌)과 키움(맘계좌)의 수량 차이를 분석합니다.
"""

import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import re

# 공통 모듈 사용
from lib.stock_utils import (
    get_stock_price,
    _get_code_for_name,
)

# 설정
SPREADSHEET_KEY = "1OluYqwosyYzWLXYh_iGfoMKsbaM6WjVlAiXImDfGWdA"
CRED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "stock-holding-log-db46e6d87dd6.json")
MINE_TAB = "내계좌"
MOM_TAB = "맘계좌"

def _normalize_num(val, is_rate=False):
    if pd.isna(val) or val == "": return 0.0
    if isinstance(val, str):
        val = val.replace(",", "").replace("%", "").strip()
        try: 
            num = float(val)
            if is_rate: num /= 100.0 # -42.75 -> -0.4275
            return num
        except: return 0.0
    num = float(val)
    if is_rate: num /= 100.0
    return num

def find_col(df, keywords):
    for col in df.columns:
        if any(k in str(col) for k in keywords): return col
    return None

def get_latest_base_tab(sh):
    """숫자 6자리(YYMMDD) 형식 중 가장 큰(최신) 탭 찾기"""
    tabs = [ws.title for ws in sh.worksheets()]
    date_tabs = [t for t in tabs if re.match(r'^\d{6}$', t)]
    if not date_tabs: return None
    return sorted(date_tabs, reverse=True)[0]

def get_tab_df(sh, title, mapping):
    # 기본 결과 구조 (mapping의 모든 키가 포함된 빈 DF)
    fallback_df = pd.DataFrame(columns=mapping.keys())
    try:
        ws = sh.worksheet(title)
        values = ws.get_all_values()
        if not values: return fallback_df
        df_raw = pd.DataFrame(values)
        
        header_idx = None
        for i, row in df_raw.iterrows():
            if any("종목명" in str(v) for v in row.values):
                header_idx = i
                break
        
        if header_idx is None: return fallback_df
        
        df = df_raw.iloc[header_idx+1:].copy()
        df.columns = df_raw.iloc[header_idx].values
        df = df[df.iloc[:,0].astype(str).str.strip() != ""].reset_index(drop=True)
        
        # 필요한 열만 추출 및 정규화
        res = pd.DataFrame()
        for internal_key, keywords in mapping.items():
            col = find_col(df, keywords)
            if col:
                if internal_key == "name": 
                    res[internal_key] = df[col].astype(str).str.strip()
                else: 
                    is_rate = (internal_key == "rate")
                    res[internal_key] = df[col].apply(lambda x: _normalize_num(x, is_rate=is_rate))
            else:
                res[internal_key] = "" if internal_key == "name" else 0.0
        
        # 누락된 컬럼이 있으면 채우기
        for k in mapping.keys():
            if k not in res.columns:
                res[k] = "" if k == "name" else 0.0
                
        return res
    except Exception as e:
        print(f"[경고] '{title}' 탭 로드 실패: {e}")
        return fallback_df

def main():
    print("🚀 3자(기준 vs 대신 vs 키움) 비교 분석 시작...")
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CRED_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_KEY)
    
    BASE_TAB = get_latest_base_tab(sh)
    if not BASE_TAB:
        print("[오류] 기준 날짜 탭을 찾을 수 없습니다.")
        return

    print(f"[정보] 기준 탭: {BASE_TAB} / 내계좌: {MINE_TAB} / 맘계좌: {MOM_TAB}")

    # 매핑 정의
    base_map = {"name": ["종목명"], "qty": ["잔고수량"], "avg": ["평균단가"], "eval": ["평가금액"], "rate": ["손익률"]}
    mine_map = {"name": ["종목명"], "qty": ["잔고수량"], "avg": ["손익단가"], "eval": ["평가금액"], "rate": ["수익률(%)"]}
    mom_map = {"name": ["종목명"], "qty": ["보유수량"], "avg": ["손익분기매입가"], "eval": ["평가금액"], "rate": ["수익률"]}

    df_base_raw = get_tab_df(sh, BASE_TAB, base_map)
    df_base = df_base_raw.rename(columns={"qty":"qty_base", "avg":"avg_base", "eval":"eval_base", "rate":"rate_base"})
    
    df_mine_raw = get_tab_df(sh, MINE_TAB, mine_map)
    df_mine = df_mine_raw.rename(columns={"qty":"qty_mine", "avg":"avg_mine", "eval":"eval_mine", "rate":"rate_mine"})
    
    df_mom_raw = get_tab_df(sh, MOM_TAB, mom_map)
    df_mom = df_mom_raw.rename(columns={"qty":"qty_mom", "avg":"avg_mom", "eval":"eval_mom", "rate":"rate_mom"})

    # 데이터 병합 (Outer Join)
    merged = pd.merge(df_base, df_mine, on="name", how="outer")
    merged = pd.merge(merged, df_mom, on="name", how="outer").fillna(0)

    # 차이 계산 (기준 대비)
    merged["대신_수량차"] = merged["qty_mine"] - merged["qty_base"]
    merged["키움_수량차"] = merged["qty_mom"] - merged["qty_base"]
    
    # 카테고리 결정
    def get_cat(row):
        if row["qty_base"] == 0: return "NEW_신규종목"
        if row["대신_수량차"] == 0 and row["키움_수량차"] == 0: return "E_거의일치"
        return "A_수량불일치"

    merged["카테고리"] = merged.apply(get_cat, axis=1)

    # 컬럼 정리
    final_cols = [
        "카테고리", "name", "qty_base", "qty_mine", "qty_mom", "대신_수량차", "키움_수량차",
        "rate_base", "rate_mine", "rate_mom", "avg_base", "avg_mine", "avg_mom",
        "eval_base", "eval_mine", "eval_mom"
    ]
    res_df = merged[final_cols].copy()
    
    # 한글 헤더 변경
    korean_headers = {
        "name": "종목명",
        "qty_base": "기준_수량", "qty_mine": "대신_수량", "qty_mom": "키움_수량",
        "rate_base": "기준_수익률", "rate_mine": "대신_수익률", "rate_mom": "키움_수익률",
        "avg_base": "기준_단가", "avg_mine": "대신_단가", "avg_mom": "키움_단가",
        "eval_base": "기준_평가금", "eval_mine": "대신_평가금", "eval_mom": "키움_평가금"
    }
    res_df = res_df.rename(columns=korean_headers)

    # 정렬: 카테고리별, 그 다음 기준_수익률 오름차순
    res_df = res_df.sort_values(["카테고리", "기준_수익률", "종목명"], ascending=[True, True, True])

    # 구글 시트 쓰기
    write_to_sheet(sh, res_df)

def write_to_sheet(sh, df):
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    title = f"기준_대신_키움_{timestamp}"
    
    ws = sh.add_worksheet(title=title, rows=len(df)+100, cols=len(df.columns)+2)
    
    header = df.columns.tolist()
    data_list = [header] + df.values.tolist()
    ws.update("A1", data_list)
    
    # 서식 적용
    fmt_range = f"A1:{chr(64+len(header))}{len(data_list)}"
    ws.format(fmt_range, {"textFormat": {"fontSize": 9}, "verticalAlignment": "MIDDLE"})
    ws.format(f"A1:{chr(64+len(header))}1", {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}, "textFormat": {"bold": True}})
    
    # 숫자/퍼센트 서식
    header_list = header
    requests = []
    
    # 컬러 정의
    COLOR_RED = {"red": 0.8, "green": 0.1, "blue": 0.1}
    COLOR_BLUE = {"red": 0.1, "green": 0.1, "blue": 0.8}
    COLOR_YELLOW = {"red": 1.0, "green": 1.0, "blue": 0.8} # 배경색

    for idx, col in enumerate(header_list):
        # 퍼센트 설정
        if "수익률" in col:
            fmt = "0.00%"
            range_req = {"sheetId": ws.id, "startColumnIndex": idx, "endColumnIndex": idx+1, "startRowIndex": 1}
            requests.append({"repeatCell": {"range": range_req, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": fmt}}}, "fields": "userEnteredFormat.numberFormat"}})
        # 숫자 설정
        elif "수량" in col or "단가" in col or "평가금" in col:
            fmt = "#,##0"
            range_req = {"sheetId": ws.id, "startColumnIndex": idx, "endColumnIndex": idx+1, "startRowIndex": 1}
            requests.append({"repeatCell": {"range": range_req, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": fmt}}}, "fields": "userEnteredFormat.numberFormat"}})

    # 행별 조건부 서식 (수량차 강조 등)
    for r_idx, row_data in enumerate(df.values):
        curr_row = r_idx + 1
        # 수량차 강조
        for c_name in ["대신_수량차", "키움_수량차"]:
            if c_name in header_list:
                c_idx = header_list.index(c_name)
                val = row_data[c_idx]
                if val != 0:
                    requests.append({
                        "repeatCell": {
                            "range": {"sheetId": ws.id, "startRowIndex": curr_row, "endRowIndex": curr_row+1, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1},
                            "cell": {"userEnteredFormat": {"backgroundColor": COLOR_YELLOW, "textFormat": {"bold": True}}},
                            "fields": "userEnteredFormat(backgroundColor,textFormat)"
                        }
                    })
        
        # 수익률 색상
        for c_name in ["기준_수익률", "대신_수익률", "키움_수익률"]:
            if c_name in header_list:
                c_idx = header_list.index(c_name)
                val = row_data[c_idx]
                if val != 0:
                    color = COLOR_RED if val > 0 else COLOR_BLUE
                    requests.append({
                        "repeatCell": {
                            "range": {"sheetId": ws.id, "startRowIndex": curr_row, "endRowIndex": curr_row+1, "startColumnIndex": c_idx, "endColumnIndex": c_idx+1},
                            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": color, "bold": True}}},
                            "fields": "userEnteredFormat.textFormat"
                        }
                    })

    if requests:
        sh.batch_update({"requests": requests})
    
    print(f"✅ 3자 비교 결과가 '{title}' 탭에 저장되었습니다.")

if __name__ == "__main__":
    main()
