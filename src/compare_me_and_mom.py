"""
compare_me_and_mom.py - 내 계좌와 맘 계좌의 잔고를 비교하여 차이를 분석합니다.
다른 컬럼 명칭(잔고수량 vs 보유수량 등)을 매핑하여 비교 수행.
"""

import os
import sys
import pandas as pd
import argparse
import gspread
from google.oauth2.service_account import Credentials
import json
from datetime import datetime

# 공통 모듈 사용
from lib.stock_utils import (
    get_stock_price,
    _get_code_for_name,
    _load_code_cache,
)

# 설정
SPREADSHEET_KEY = "1OluYqwosyYzWLXYh_iGfoMKsbaM6WjVlAiXImDfGWdA"
CRED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "stock-holding-log-db46e6d87dd6.json")
MINE_TAB = "내계좌"
MOM_TAB = "맘계좌"

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

def find_col(df, keywords):
    for col in df.columns:
        if any(k in str(col) for k in keywords):
            return col
    return None

def read_sheets(cred_path):
    """구글 시트에서 내 계좌와 맘 계좌 탭 읽기"""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if not os.path.exists(cred_path):
        cred_path = os.path.join(os.getcwd(), cred_path)
    
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)
    
    sh = gc.open_by_key(SPREADSHEET_KEY)
    
    def get_tab_df(title):
        try:
            ws = sh.worksheet(title)
            values = ws.get_all_values()
            df = pd.DataFrame(values)
            
            # 헤더 찾기
            header_idx = None
            for i, row in df.iterrows():
                if any("종목명" in str(v) for v in row.values):
                    header_idx = i
                    break
            
            if header_idx is not None:
                new_df = df.iloc[header_idx+1:].copy()
                new_df.columns = df.iloc[header_idx].values
                new_df = new_df[new_df.iloc[:, 0].astype(str).str.strip() != ""]
                return new_df.reset_index(drop=True)
            return pd.DataFrame()
        except:
            print(f"[경고] '{title}' 탭을 읽을 수 없습니다.")
            return pd.DataFrame()

    df_mine = get_tab_df(MINE_TAB)
    df_mom = get_tab_df(MOM_TAB)
    
    return df_mine, df_mom

def compare_data(mine, mom):
    """내 계좌와 맘 계좌 비교 (동기화 및 손실 분석 중심)"""
    mine_map = {
        "name": ["종목명"], "qty": ["잔고수량"], "avg": ["손익단가"],
        "purchase": ["장부금액"], "rate": ["수익률(%)"], "weight": ["매입비중"]
    }
    mom_map = {
        "name": ["종목명"], "qty": ["보유수량"], "avg": ["손익분기매입가"],
        "purchase": ["매입금액"], "rate": ["수익률"], "weight": ["보유비중"]
    }

    def extract(df, mapping):
        res = pd.DataFrame()
        for key, keywords in mapping.items():
            col = find_col(df, keywords)
            if col: res[key] = df[col]
            else: res[key] = 0.0 if key != "name" else ""
        
        for col in ["qty", "avg", "purchase", "rate", "weight"]:
            if col in res.columns: res[col] = res[col].apply(_normalize_num)
        return res

    df_mine_sub = extract(mine, mine_map)
    df_mom_sub = extract(mom, mom_map)
    
    merged = pd.merge(df_mine_sub, df_mom_sub, on="name", how="outer", suffixes=("_Mine", "_Mom")).fillna(0)
    
    current_prices = {}
    try:
        name_to_code = _load_code_cache()
        target_names = merged["name"].unique()
        for name in target_names:
            code = _get_code_for_name(str(name).strip(), name_to_code)
            if code:
                p = get_stock_price(code, name, silent=True)["price"]
                if p: current_prices[name] = p
    except: pass

    def normalize_percent(val):
        if pd.isna(val) or val == 0: return 0.0
        return round(val / 100, 6)

    diff_results = []
    for _, row in merged.iterrows():
        name = row["name"]
        qty_mine, qty_mom = row["qty_Mine"], row["qty_Mom"]
        avg_mine, avg_mom = row["avg_Mine"], row["avg_Mom"]
        rate_mine = normalize_percent(row["rate_Mine"])
        rate_mom = normalize_percent(row["rate_Mom"])
        cur_price = current_prices.get(name, 0)
        
        qty_diff = qty_mine - qty_mom 
        avg_gap = avg_mine - avg_mom 
        
        # 카테고리 우선 결정
        if qty_mine > 0 and qty_mom == 0: category = "C_나만_보유"
        elif qty_mine == 0 and qty_mom > 0: category = "D_맘만_보유"
        elif qty_diff != 0:
            if rate_mine < 0 or rate_mom < 0:
                category = "A1_수량불일치_손실"
            else:
                category = "A2_수량불일치_수익"
        elif abs(avg_gap) / max(1, avg_mom) > 0.01: category = "B_평단불일치"
        else:
            if rate_mine < 0 or rate_mom < 0:
                category = "E1_거의일치_손실"
            else:
                category = "E2_거의일치_수익"

        # 시나리오 계산 (거의일치/수량불일치_수익 제외)
        if category in ["E1_거의일치_손실", "E2_거의일치_수익", "A2_수량불일치_수익"]:
            needed_qty, needed_amt, expected_avg, expected_pl_rate = 0, 0, 0, 0
        else:
            needed_qty = qty_mom - qty_mine
            needed_amt = needed_qty * cur_price if cur_price > 0 else needed_qty * avg_mom
            if qty_mom > 0:
                p_buy = cur_price if cur_price > 0 else avg_mom
                total_cost = (qty_mine * avg_mine) + (max(0, needed_qty) * p_buy)
                new_qty = max(qty_mine, qty_mom)
                expected_avg = total_cost / new_qty if new_qty > 0 else 0
                expected_pl_rate = (cur_price - expected_avg) / expected_avg if expected_avg > 0 and cur_price > 0 else 0
            else:
                expected_avg, expected_pl_rate = 0, 0

        # 평가금 계산 (각 계좌의 단가와 수익률 기반으로 각각 계산하여 차이 반영)
        eval_mine = qty_mine * avg_mine * (1 + rate_mine)
        eval_mom = qty_mom * avg_mom * (1 + rate_mom)
        raw_eval_diff = eval_mine - eval_mom
        rate_diff = rate_mine - rate_mom

        # 사용자 요청: A1(손실)은 모두 마이너스, A2(수익)는 모두 플러스 처리
        if category == "A1_수량불일치_손실":
            eval_diff_val = -abs(raw_eval_diff)
        elif category == "A2_수량불일치_수익":
            eval_diff_val = abs(raw_eval_diff)
        else:
            eval_diff_val = raw_eval_diff

        diff_results.append({
            "카테고리": category,
            "종목명": name,
            "수량차이": int(qty_diff),
            "평단차이": int(avg_gap),
            "수익률차이": round(rate_diff, 4),
            "평가금차이": int(eval_diff_val),
            "내_수익률": rate_mine,
            "맘_수익률": rate_mom,
            "맞춤_필요수량": int(needed_qty) if needed_qty != 0 else 0,
            "맞춤_필요금액": int(needed_amt) if needed_amt != 0 else 0,
            "예상평단": int(expected_avg) if expected_avg != 0 else 0,
            "예상수익률": round(expected_pl_rate, 4) if expected_pl_rate != 0 else 0,
            "내_수량": int(qty_mine),
            "맘_수량": int(qty_mom),
            "내_단가": int(avg_mine),
            "맘_단가": int(avg_mom),
            "내_평가금": int(eval_mine),
            "맘_평가금": int(eval_mom),
        })
        
    res_df = pd.DataFrame(diff_results)
    
    # 정렬 키 생성
    def get_sort_key(row):
        cat_map = {
            "A1_수량불일치_손실": 1, 
            "B_평단불일치": 2, 
            "C_나만_보유": 3, 
            "D_맘만_보유": 4, 
            "E1_거의일치_손실": 5,
            "A2_수량불일치_수익": 6,
            "E2_거의일치_수익": 7
        }
        return cat_map.get(row["카테고리"], 9)

    res_df["_sort_key"] = res_df.apply(get_sort_key, axis=1)
    
    # 카테고리 우선, 그 다음 내_수익률 오름차순 (손실이 큰 순서)
    res_df = res_df.sort_values(["_sort_key", "내_수익률", "종목명"], ascending=[True, True, True]).drop(columns=["_sort_key"])
    
    return res_df

def write_to_sheet(df, cred_path):
    """결과를 새 탭에 저장 및 요약/고급 서식 적용"""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_KEY)
    
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    # 내계좌(대신) vs 맘계좌(키움)
    title = f"대신_키움_{timestamp}"
    
    # 총계 계산
    sum_mine = df["내_평가금"].sum()
    sum_mom = df["맘_평가금"].sum()
    sum_diff = sum_mine - sum_mom
    
    summary_data = [
        ["[전체 요약]", "", "", "", "", ""],
        ["내 총 보유금액", f"{sum_mine:,.0f}", "원", "", "", ""],
        ["맘 총 보유금액", f"{sum_mom:,.0f}", "원", "", "", ""],
        ["보유금액 차이", f"{sum_diff:,.0f}", "원", f"({(sum_mine/sum_mom - 1)*100:.1f}% 차이)" if sum_mom > 0 else "", "", ""],
        ["", "", "", "", "", ""],
    ]
    summary_len = len(summary_data)
    
    ws = sh.add_worksheet(title=title, rows=str(max(100, len(df) + summary_len + 5)), cols="20")
    
    # 시트 데이터 작성 전 인덱스 초기화 (작도 시 idx가 sequential하게 유지되도록)
    df = df.reset_index(drop=True)
    
    # 데이터 준비: 요약 + 헤더 + 데이터
    all_values = summary_data + [list(df.columns)] + df.fillna("").values.tolist()
    ws.update(values=all_values, range_name="A1", value_input_option='USER_ENTERED')
    
    requests = []
    header_list = list(df.columns)
    data_start_row = summary_len # 0-indexed, so row summary_len+1 in sheet if including header

    # 1. 요약 섹션 서식
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": summary_len-1, "startColumnIndex": 1, "endColumnIndex": 2},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": 11}}},
            "fields": "userEnteredFormat(textFormat)"
        }
    })
    # 금액 차이 색상
    diff_color = {"red": 0.8, "green": 0.1, "blue": 0.1} if sum_diff > 0 else {"red": 0.1, "green": 0.1, "blue": 0.8}
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": summary_len-2, "endRowIndex": summary_len-1, "startColumnIndex": 1, "endColumnIndex": 2},
            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": diff_color}}},
            "fields": "userEnteredFormat.textFormat.foregroundColor"
        }
    })

    # 2. 데이터 헤더 서식 (진한 배경 + 흰색 굵은 글씨)
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": data_start_row, "endRowIndex": data_start_row + 1, "startColumnIndex": 0, "endColumnIndex": len(header_list)},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}, "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2}, "horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
        }
    })

    # 3. 틀 고정 (헤더 이후부터)
    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": data_start_row + 1, "frozenColumnCount": 2}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
        }
    })

    # 4. 열 너비 자동 조정
    for i, col in enumerate(header_list):
        pixel_size = 120 if col in ["카테고리", "종목명"] else 85
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                "properties": {"pixelSize": pixel_size},
                "fields": "pixelSize"
            }
        })

    # 5. 카테고리별 배경색
    category_colors = {
        "A1_수량불일치_손실": {"red": 1.0, "green": 0.9, "blue": 0.8}, # 살구색
        "A2_수량불일치_수익": {"red": 0.95, "green": 0.95, "blue": 0.95}, # 연회색
        "B_평단불일치": {"red": 0.9, "green": 0.9, "blue": 1.0}, # 연보라
        "C_나만_보유": {"red": 0.9, "green": 1.0, "blue": 1.0}, # 연청색
        "D_맘만_보유": {"red": 1.0, "green": 0.9, "blue": 0.95}, # 연분홍
        "E1_거의일치_손실": {"red": 1.0, "green": 1.0, "blue": 1.0}, # 흰색
        "E2_거의일치_수익": {"red": 1.0, "green": 1.0, "blue": 1.0}, # 흰색
    }
    
    for idx, row in df.iterrows():
        cat = row.get("카테고리")
        curr_row = data_start_row + 1 + idx
        if cat in category_colors:
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": curr_row, "endRowIndex": curr_row + 1, "startColumnIndex": 0, "endColumnIndex": len(header_list)},
                    "cell": {"userEnteredFormat": {"backgroundColor": category_colors[cat]}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

    # 6. 숫자 서식 
    fmt_map = {
        "수량차이": "#,##0", "평단차이": "#,##0", "수익률차이": "0.00%", "평가금차이": "#,##0",
        "맞춤_필요수량": "#,##0", "맞춤_필요금액": "#,##0", "예상평단": "#,##0",
        "예상수익률": "0.00%", "내_수량": "#,##0", "맘_수량": "#,##0", 
        "내_단가": "#,##0", "맘_단가": "#,##0",
        "내_수익률": "0.00%", "맘_수익률": "0.00%", "내_평가금": "#,##0", "맘_평가금": "#,##0"
    }
    for col, pattern in fmt_map.items():
        if col in header_list:
            c_idx = header_list.index(col)
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": data_start_row + 1, "endRowIndex": data_start_row + 1 + len(df), "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT" if "%" in pattern else "NUMBER", "pattern": pattern}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

    # 공통 색상 정의
    COLOR_RED = {"red": 0.8, "green": 0.1, "blue": 0.1}   # 수익 (진한 빨강)
    COLOR_BLUE = {"red": 0.1, "green": 0.1, "blue": 0.8}  # 손실 (진한 파랑)
    COLOR_YELLOW = {"red": 1.0, "green": 1.0, "blue": 0.0} # 강조 (노랑)
    COLOR_MINE_MORE = {"red": 1.0, "green": 0.9, "blue": 0.8} # 내가 많음 (오렌지/피치)
    COLOR_MOM_MORE = {"red": 0.8, "green": 0.9, "blue": 1.0}  # 맘이 많음 (하늘)

    # 7. 유의미한 값 강조
    for idx, r in df.iterrows():
        curr_row = data_start_row + 1 + idx
        cat = r.get("카테고리")
        
        # 수익률 색상 (+빨강, -파랑)
        for col in ["내_수익률", "맘_수익률", "예상수익률", "수익률차이"]:
            if col in header_list:
                c_idx = header_list.index(col)
                val = r.get(col, 0)
                if val == 0: continue
                color = COLOR_RED if val > 0 else COLOR_BLUE
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": ws.id, "startRowIndex": curr_row, "endRowIndex": curr_row + 1, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1},
                        "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": color, "bold": True}}},
                        "fields": "userEnteredFormat.textFormat"
                    }
                })
        
        # 평가금차이 색상 (+빨강, -파랑)
        if "평가금차이" in header_list:
            c_idx = header_list.index("평가금차이")
            val = r.get("평가금차이", 0)
            if val != 0:
                color = COLOR_RED if val > 0 else COLOR_BLUE
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": ws.id, "startRowIndex": curr_row, "endRowIndex": curr_row + 1, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1},
                        "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": color, "bold": True}}},
                        "fields": "userEnteredFormat.textFormat"
                    }
                })

        # 수량차이 셀 색상 강조 (내가 많으면 오렌지, 맘이 많으면 하늘색)
        qty_diff_val = r.get("수량차이", 0)
        if qty_diff_val != 0:
            c_idx = header_list.index("수량차이")
            color = COLOR_MINE_MORE if qty_diff_val > 0 else COLOR_MOM_MORE
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": curr_row, "endRowIndex": curr_row + 1, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1},
                    "cell": {"userEnteredFormat": {"backgroundColor": color, "textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)"
                }
            })

        # 맞춤 필요 수량 강조 (값이 있을 때만 노란색, A2/E 제외)
        n_qty = r.get("맞춤_필요수량", 0)
        if n_qty != 0 and cat not in ["E1_거의일치_손실", "E2_거의일치_수익", "A2_수량불일치_수익"]:
            c_idx = header_list.index("맞춤_필요수량")
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": curr_row, "endRowIndex": curr_row + 1, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1},
                    "cell": {"userEnteredFormat": {"backgroundColor": COLOR_YELLOW, "textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)"
                }
            })

    if requests: sh.batch_update({"requests": requests})
    print(f"✅ 비교 결과가 '{title}' 탭에 저장되었습니다.")

def main():
    print("🚀 내 계좌 vs 맘 계좌 비교 시작...")
    df_mine, df_mom = read_sheets(CRED_PATH)
    
    if df_mine.empty or df_mom.empty:
        print("❌ 데이터를 읽어오지 못했습니다.")
        return
        
    diff_df = compare_data(df_mine, df_mom)
    print(f"📊 비교 완료 ({len(diff_df)}개 종목)")
    
    write_to_sheet(diff_df, CRED_PATH)

if __name__ == "__main__":
    main()
