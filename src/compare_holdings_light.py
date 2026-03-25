"""
compare_holdings_light.py - 비중 관련 로직 제거된 Light 버전
동적 탭 참조 (--base-key, --base-tab, --mine-key, --mine-tab) 및 헤더 매핑 지원
서로 다른 스프레드시트에 위치한 기준계좌와 내 계좌 비교 가능
"""

import sys
import os
import pandas as pd
import argparse
import gspread
from google.oauth2.service_account import Credentials
import json
from datetime import datetime, timedelta

# 공통 모듈 사용
from lib.stock_utils import (
    get_stock_price,
    _get_code_from_master,
    _get_code_for_name,
    _load_code_cache,
    _save_code_cache,
)

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


def read_sheet_ranges(base_key, base_tab, mine_key, mine_tab, cred_path):
    """구글 시트에서 기준계좌와 내 계좌 테이블 읽기 (다른 스프레드시트 지원)"""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if not os.path.exists(cred_path):
        cred_path = os.path.join(os.getcwd(), cred_path)
    
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)
    
    # 1. 기준계좌 (Base) 읽기 - update_holdings.py가 사용하는 스프레드시트
    try:
        sh_base = gc.open_by_key(base_key)
        ws_base = sh_base.worksheet(base_tab)
        base_values = ws_base.get_all_values()
        df_base_raw = pd.DataFrame(base_values)
        print(f"[정보] 기준계좌 로드 성공: '{base_tab}' ({len(df_base_raw)}행)")
    except Exception as e:
        print(f"[오류] 기준계좌 탭 '{base_tab}'을 찾을 수 없습니다: {e}")
        return None, None

    # 2. 내 계좌 (Mine) 읽기 - compare_holdings.py가 사용하는 스프레드시트
    try:
        sh_mine = gc.open_by_key(mine_key)
        ws_mine = sh_mine.worksheet(mine_tab)
        mine_all_values = ws_mine.get_all_values()
        df_mine_full = pd.DataFrame(mine_all_values)
        # A-I열 추출 (0-8)
        df_mine_raw = df_mine_full.iloc[:, 0:9] if df_mine_full.shape[1] >= 9 else df_mine_full
        print(f"[정보] 내 계좌 로드 성공: '{mine_tab}' ({len(df_mine_raw)}행)")
    except Exception as e:
        print(f"[오류] 내 계좌 탭 '{mine_tab}'을 찾을 수 없습니다: {e}")
        df_mine_raw = pd.DataFrame()

    def process_table(df):
        if df.empty: return pd.DataFrame()
        header_row_idx = None
        for i, row in df.iterrows():
            if any("종목명" in str(v) for v in row.values):
                header_row_idx = i
                break
        
        if header_row_idx is None:
            return pd.DataFrame()
            
        new_df = df.iloc[header_row_idx+1:].copy()
        new_df.columns = df.iloc[header_row_idx].values
        new_df = new_df[new_df.iloc[:, 0].astype(str).str.strip() != ""]
        return new_df.reset_index(drop=True)

    left_holdings = process_table(df_base_raw)
    right_holdings = process_table(df_mine_raw)
    
    return left_holdings, right_holdings


def compare_tables(left, right, target_type="mine"):
    """기준계좌와 내 계좌 테이블 비교 (헤더 매핑 반영)"""
    
    # 헤더 매핑 정의
    base_map = {
        "name": ["종목명"],
        "qty": ["잔고수량"],
        "avg": ["평균단가"],
        "purchase": ["매입금액"],
        "eval": ["평가금액"],
        "pl": ["미실현손익"],
        "rate": ["손익률"],
        "weight": ["보유비중"]
    }
    
    mine_map = {
        "name": ["종목명"],
        "qty": ["잔고수량"],
        "avg": ["손익단가"],
        "purchase": ["장부금액"],
        "eval": ["평가금액"],
        "pl": ["평가손익"],
        "rate": ["수익률(%)"],
        "weight": ["매입비중"]
    }

    # 맘계좌 헤더 매핑
    mom_map = {
        "name": ["종목명"],
        "qty": ["보유수량"],
        "avg": ["손익분기매입가"],
        "purchase": ["매입금액"],
        "eval": ["평가금액"],
        "pl": ["평가손익"],
        "rate": ["수익률"],
        "weight": ["보유비중"]
    }

    # 대상 계좌 타입에 따라 매핑 선택
    target_map = mom_map if target_type == "mom" else mine_map

    def extract_sub(df, mapping):
        res = pd.DataFrame()
        for internal_key, keywords in mapping.items():
            col = find_col(df, keywords)
            if col:
                res[internal_key] = df[col]
            else:
                res[internal_key] = 0.0 if internal_key != "name" else ""
        return res

    left_sub = extract_sub(left, base_map)
    right_sub = extract_sub(right, target_map)
    
    if left_sub["name"].replace("", pd.NA).dropna().empty:
        print("[오류] 기준계좌에서 종목명을 찾을 수 없습니다.")
        return pd.DataFrame()

    # 데이터 정규화
    for df in [left_sub, right_sub]:
        for col in ["qty", "avg", "purchase", "eval", "pl", "rate", "weight"]:
            df[col] = df[col].apply(_normalize_num)

    # 종목명 기준 병합
    merged = pd.merge(left_sub, right_sub, on="name", how="outer", suffixes=("_L", "_R")).fillna(0)
    
    current_prices = {}
    try:
        name_to_code = _load_code_cache()
        target_names = merged["name"].unique()
        total_count = len(target_names)
        print(f"[정보] 현재가 조회 시작 (총 {total_count}개 종목)")
        
        for idx, name in enumerate(target_names, 1):
            pct = idx * 100 // total_count
            print(f"\r[진행] 현재가 조회 중... {idx}/{total_count} ({pct}%)", end="", flush=True)
            
            clean_name = str(name).strip().replace(" ", "")
            code = _get_code_for_name(clean_name, name_to_code)
            if not code:
                code = _get_code_for_name(str(name).strip(), name_to_code)
            
            if code:
                SKIP_CODES = {"102600"}
                if code in SKIP_CODES: continue
                price_result = get_stock_price(code, name, silent=True)
                if price_result["price"]: current_prices[name] = price_result["price"]
            else:
                price_result = get_stock_price("", name, silent=True)
                if price_result["price"]: current_prices[name] = price_result["price"]
        print()
    except Exception as e:
        print(f"\n[경고] 현재가 조회 실패: {e}")

    diff_results = []
    for _, row in merged.iterrows():
        name = row["name"]
        qty_L, qty_R = row["qty_L"], row["qty_R"]
        avg_L, avg_R = row["avg_L"], row["avg_R"]
        qty_diff = qty_R - qty_L
        avg_gap = avg_R - avg_L
        cur_price = current_prices.get(name, 0)
        has_cur_price = cur_price > 0
        
        pl_rate = ((cur_price - avg_R) / avg_R) if avg_R > 0 and has_cur_price else 0.0
        base_pl_rate = ((cur_price - avg_L) / avg_L) if avg_L > 0 and has_cur_price else 0.0
        is_profit = pl_rate > 0
        pl_rate_diff = pl_rate - base_pl_rate if (avg_L > 0 and avg_R > 0) else None
        
        category = "Z_미분류"
        if qty_R == 0 and qty_L > 0: category = "E_기준만_존재"; pl_rate_diff, avg_gap = None, None
        elif qty_L == 0 and qty_R > 0: category = "D_나만_보유"; pl_rate_diff, avg_gap = None, None
        elif (pl_rate_diff is not None and pl_rate_diff > 0): category = "A_유리함"
        elif avg_R > avg_L and avg_L > 0 and has_cur_price and cur_price < avg_L: category = "B_평단맞춤_가능"
        elif avg_R > avg_L and avg_L > 0: category = "C_평단맞춤_불가"
        else: category = "G_손실중"
        
        is_minor_diff = False
        if avg_L > 0 and category not in ["E_기준만_존재", "D_나만_보유"]:
            if abs(avg_R - avg_L) / avg_L <= 0.03: is_minor_diff = True
        
        if is_minor_diff and category not in ["E_기준만_존재", "D_나만_보유"]: category = "F_거의일치"

        # 1. 수량맞춤_필요주수 / 필요금액 (계산 가능하면 무조건 구함)
        qty_match_shares, qty_match_cost = 0, 0
        qty_match_shares = int(qty_L - qty_R)
        if has_cur_price:
            qty_match_cost = int(abs(qty_match_shares) * cur_price)
        
        # 2. 평단맞춤_필요주수 / 필요금액 (계산 가능하면 무조건 구함)
        avg_match_shares, avg_match_cost = 0, 0
        if has_cur_price and qty_R > 0 and cur_price != avg_L:
            needed_x = (qty_R * (avg_L - avg_R)) / (cur_price - avg_L)
            if needed_x > 0:
                avg_match_shares = int(needed_x)
                avg_match_cost = int(avg_match_shares * cur_price)
        
        # 3. 추가매수 시 예상 지표들 계산 (수량차 마이너스이고 현재가 있으면 무조건 계산)
        need_cost = 0  # 필요 금액
        exp_avg = 0    # 예상 평단
        exp_avg_gap = 0  # 예상 평단갭
        exp_pl_rate = 0  # 예상 손익률
        exp_pl_diff = 0  # 예상 손익률차
        pl_improve = 0   # 손익률개선
        
        if qty_diff < 0 and has_cur_price and qty_R > 0:
            buy_qty = abs(qty_diff)
            buy_cost = buy_qty * cur_price
            current_cost = qty_R * avg_R
            new_qty = qty_R + buy_qty
            new_cost = current_cost + buy_cost
            expected_avg = new_cost / new_qty if new_qty > 0 else 0
            expected_pl_rate = ((cur_price - expected_avg) / expected_avg) if expected_avg > 0 else 0
            expected_avg_gap = expected_avg - avg_L
            expected_pl_diff = expected_pl_rate - base_pl_rate
            
            need_cost = int(buy_cost)
            exp_avg = int(expected_avg)
            exp_avg_gap = int(expected_avg_gap)
            exp_pl_rate = round(expected_pl_rate, 4)
            exp_pl_diff = round(expected_pl_diff, 4)
            pl_improve = round(expected_pl_rate - pl_rate, 4)
            
        # 현금화_주수, 현금화_금액
        cash_sell_shares, cash_amount = 0, 0
        if qty_R > qty_L and is_profit and has_cur_price:
            cash_sell_shares = int(qty_R - qty_L)
            cash_amount = int(cur_price * cash_sell_shares)

        
        diff_results.append({
            "카테고리": category, "종목명": name,
            "손익률차": round(pl_rate_diff, 4) if pl_rate_diff is not None else None,
            "기준손익률": round(base_pl_rate, 4), "내손익률": round(pl_rate, 4),
            "평단갭": int(avg_gap) if avg_gap is not None else None,
            "현재가": int(cur_price), "기준_단가": int(avg_L), "내_단가": int(avg_R),
            "기준_평가금액": int(row["eval_L"]), "내_평가금액": int(row["eval_R"]),
            "기준_비중": round(row["weight_L"] / 100.0, 4) if row["weight_L"] else 0.0,
            "내_비중": round(row["weight_R"] / 100.0, 4) if row["weight_R"] else 0.0,
            "수량차": int(qty_diff), "기준_수량": int(qty_L), "내_수량": int(qty_R),
            "수량맞춤_필요주수": qty_match_shares, "수량맞춤_필요금액": qty_match_cost,
            "평단맞춤_필요주수": avg_match_shares, "평단맞춤_필요금액": avg_match_cost,
            "필요금액": need_cost, "예상평단": exp_avg, "예상평단갭": exp_avg_gap,
            "예상손익률": exp_pl_rate, "예상손익률차": exp_pl_diff, "손익률개선": pl_improve,
            "현금화_주수": cash_sell_shares, "현금화_금액": cash_amount,
        })


    
    result_df = pd.DataFrame(diff_results)
    sorted_dfs = []
    cats = ["A_유리함", "B_평단맞춤_가능", "C_평단맞춤_불가", "D_나만_보유", "E_기준만_존재", "F_거의일치", "G_손실중"]
    for c in cats:
        sub = result_df[result_df["카테고리"] == c]
        if c in ["A_유리함", "C_평단맞춤_불가", "D_나만_보유", "G_손실중"]: sub = sub.sort_values(by="내손익률", ascending=False)
        elif c == "B_평단맞춤_가능": sub = sub.sort_values(by="수량차", ascending=True)
        elif c == "E_기준만_존재": sub = sub.sort_values(by="기준손익률", ascending=True)
        elif c == "F_거의일치": sub = sub.sort_values(by="수량차", ascending=False)
        sorted_dfs.append(sub)
    
    sorted_dfs.append(result_df[~result_df["카테고리"].isin(cats)])
    return pd.concat(sorted_dfs, ignore_index=True)


def write_analysis_sheet(spreadsheet_key, title, df, cred_path):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_key)
    try:
        ws = sh.worksheet(title); sh.del_worksheet(ws)
    except Exception: pass
    rows, cols = (len(df) + 1), max(1, len(df.columns))
    ws = sh.add_worksheet(title=title, rows=str(max(100, rows)), cols=str(max(20, cols)))
    values = [list(df.columns)] + df.fillna("").values.tolist()
    ws.update(values=values, range_name="A1", value_input_option='USER_ENTERED')
    
    requests = []
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": len(df.columns)},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2}, "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}, "bold": True}, "horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
        }
    })
    requests.append({"updateSheetProperties": {"properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 5}}, "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}})
    
    # 1. 헤더 정리 및 컬럼-인덱스 매핑 생성
    header_list = list(df.columns)
    col_idx_map = {name: i for i, name in enumerate(header_list)}

    # 열 너비 조정: 카테고리/종목명 = 100, 나머지 = 80
    for col_name, col_idx in col_idx_map.items():
        pixel_width = 100 if col_name in ["카테고리", "종목명"] else 80
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": col_idx, "endIndex": col_idx + 1},
                "properties": {"pixelSize": pixel_width},
                "fields": "pixelSize"
            }
        })

    category_colors = {
        "A_유리함": {"red": 0.85, "green": 0.95, "blue": 0.85}, 
        "B_평단맞춤_가능": {"red": 0.85, "green": 0.92, "blue": 1.0}, 
        "C_평단맞춤_불가": {"red": 1.0, "green": 0.95, "blue": 0.8}, 
        "D_나만_보유": {"red": 0.9, "green": 0.95, "blue": 1.0}, 
        "E_기준만_존재": {"red": 1.0, "green": 0.92, "blue": 0.85}, 
        "F_거의일치": {"red": 0.95, "green": 0.95, "blue": 0.95}, 
        "G_손실중": {"red": 1.0, "green": 0.88, "blue": 0.88}
    }

    # 조건부 포맷팅을 위한 루프
    yellow_bg = {"red": 1.0, "green": 1.0, "blue": 0.0}
    
    # 미리 인덱스 추출 (존재 여부 확인 포함)
    c_idx_qty_diff = col_idx_map.get("수량차")
    c_idx_base_pl = col_idx_map.get("기준손익률")
    c_idx_need_cost_match = col_idx_map.get("수량맞춤_필요금액")
    c_idx_my_pl = col_idx_map.get("내손익률")
    c_idx_avg_match_shares = col_idx_map.get("평단맞춤_필요주수")
    c_idx_pl_improve = col_idx_map.get("손익률개선")

    for idx, row in df.iterrows():
        cat = row.get("카테고리")
        s_row, e_row = idx + 1, idx + 2
        
        if cat in category_colors:
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": s_row, "endRowIndex": e_row, "startColumnIndex": 0, "endColumnIndex": len(header_list)}, 
                    "cell": {"userEnteredFormat": {"backgroundColor": category_colors[cat]}}, 
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
            
        # 1. F_거의일치: 수량차 양수인 셀 노란색
        if cat == "F_거의일치" and isinstance(c_idx_qty_diff, int):
            if row.get("수량차", 0) > 0:
                c_start, c_end = c_idx_qty_diff, c_idx_qty_diff + 1
                requests.append({"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": s_row, "endRowIndex": e_row, "startColumnIndex": c_start, "endColumnIndex": c_end}, "cell": {"userEnteredFormat": {"backgroundColor": yellow_bg}}, "fields": "userEnteredFormat.backgroundColor"}})
        
        # 3. E_기준만_존재: 기준손익률 마이너스인 것의 수량맞춤_필요금액 노란색
        if cat == "E_기준만_존재" and isinstance(c_idx_base_pl, int) and isinstance(c_idx_need_cost_match, int):
            if row.get("기준손익률", 0) < 0:
                c_start, c_end = c_idx_need_cost_match, c_idx_need_cost_match + 1
                requests.append({"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": s_row, "endRowIndex": e_row, "startColumnIndex": c_start, "endColumnIndex": c_end}, "cell": {"userEnteredFormat": {"backgroundColor": yellow_bg}}, "fields": "userEnteredFormat.backgroundColor"}})
        
        # 4. A_유리함: 내손익률 음수인 경우 수량차 노란색
        if cat == "A_유리함" and isinstance(c_idx_my_pl, int) and isinstance(c_idx_qty_diff, int):
            if row.get("내손익률", 0) < 0:
                c_start, c_end = c_idx_qty_diff, c_idx_qty_diff + 1
                requests.append({"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": s_row, "endRowIndex": e_row, "startColumnIndex": c_start, "endColumnIndex": c_end}, "cell": {"userEnteredFormat": {"backgroundColor": yellow_bg}}, "fields": "userEnteredFormat.backgroundColor"}})
        
        # 5. B_평단맞춤_가능: 수량차 음수이고 평단맞춤_필요주수+수량차 여전히 음수면 평단맞춤_필요주수 노란색
        if cat == "B_평단맞춤_가능" and isinstance(c_idx_qty_diff, int) and isinstance(c_idx_avg_match_shares, int):
            qty_diff = row.get("수량차", 0)
            avg_match = row.get("평단맞춤_필요주수", 0)
            if qty_diff < 0 and (avg_match + qty_diff) < 0:
                c_start, c_end = c_idx_avg_match_shares, c_idx_avg_match_shares + 1
                requests.append({"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": s_row, "endRowIndex": e_row, "startColumnIndex": c_start, "endColumnIndex": c_end}, "cell": {"userEnteredFormat": {"backgroundColor": yellow_bg}}, "fields": "userEnteredFormat.backgroundColor"}})
        
        # 6. 손익률개선이 양수인 경우 손익률개선 셀 노란색 (모든 카테고리)
        if isinstance(c_idx_pl_improve, int):
            if row.get("손익률개선", 0) > 0:
                c_start, c_end = c_idx_pl_improve, c_idx_pl_improve + 1
                requests.append({"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": s_row, "endRowIndex": e_row, "startColumnIndex": c_start, "endColumnIndex": c_end}, "cell": {"userEnteredFormat": {"backgroundColor": yellow_bg}}, "fields": "userEnteredFormat.backgroundColor"}})

    # 숫자 포맷 및 색상 지정 루프 (루프 외부에서 컬럼별로 일괄 처리 가능한 것들은 미리 처리됨)
    fmt_map = {"기준손익률": "0.00%", "내손익률": "0.00%", "손익률차": "0.00%", "예상손익률": "0.00%", "예상손익률차": "0.00%", "손익률개선": "0.00%", "기준_수량": "#,##0", "내_수량": "#,##0", "수량차": "#,##0", "기준_단가": "#,##0", "내_단가": "#,##0", "기준_평가금액": "#,##0", "내_평가금액": "#,##0", "기준_비중": "0.00%", "내_비중": "0.00%", "평단갭": "#,##0", "현재가": "#,##0", "수량맞춤_필요주수": "#,##0", "수량맞춤_필요금액": "#,##0", "평단맞춤_필요주수": "#,##0", "평단맞춤_필요금액": "#,##0", "필요금액": "#,##0", "예상평단": "#,##0", "예상평단갭": "#,##0", "현금화_주수": "#,##0", "현금화_금액": "#,##0"}

    for col, fmt in fmt_map.items():
        if col in col_idx_map:
            c_idx = col_idx_map[col]
            requests.append({"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(df) + 1, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT" if "%" in fmt else "NUMBER", "pattern": fmt}}}, "fields": "userEnteredFormat.numberFormat"}})

    # 개별 셀 색상 (양수/음수) 처리
    color_cols = ["손익률차", "내손익률", "기준손익률", "예상손익률", "예상손익률차"]
    for col in color_cols:
        if col in col_idx_map:
            c_idx = col_idx_map[col]
            for idx, r in df.iterrows():
                val = r.get(col, 0)
                if pd.isna(val) or val == 0: continue
                color = {"red": 0.8, "green": 0.2, "blue": 0.2} if val > 0 else {"red": 0.2, "green": 0.2, "blue": 0.8}
                requests.append({"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": idx + 1, "endRowIndex": idx + 2, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1}, "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": color, "bold": True}}}, "fields": "userEnteredFormat.textFormat"}})

    # 기준_수량 bold 처리
    if "기준_수량" in col_idx_map:
        c_idx = col_idx_map["기준_수량"]
        requests.append({"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": len(df) + 1, "startColumnIndex": c_idx, "endColumnIndex": c_idx + 1}, "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}})

    if requests: sh.batch_update({"requests": requests})

    print(f"[정보] 분석 결과가 '{title}' 탭에 저장되었습니다.")


def main():
    parser = argparse.ArgumentParser(description="잔고 비교 (Light 버전 - 다중 스프레드시트 지원)")
    parser.add_argument("--gs-cred", default="config/stock-holding-log-db46e6d87dd6.json")
    parser.add_argument("--base-key", required=True, help="기준계좌 스프레드시트 키 (update_holdings.py 파일)")
    parser.add_argument("--base-tab", required=True, help="기준계좌 탭 이름 (예: 잔고_20260109)")
    parser.add_argument("--mine-key", required=True, help="내 계좌 스프레드시트 키 (compare_holdings.py 파일)")
    parser.add_argument("--mine-tab", default="내계좌", help="내 계좌 탭 이름 (기본: 내계좌)")
    parser.add_argument("--target-type", choices=["mine", "mom"], default="mine", help="비교 대상 계좌 타입 (mine: 내계좌, mom: 맘계좌)")
    parser.add_argument("--output-key", default=None, help="결과 저장할 스프레드시트 키 (기본: mine-key)")
    args = parser.parse_args()

    target_label = "내 계좌" if args.target_type == "mine" else "맘 계좌"
    print(f"[정보] 기준: {args.base_tab} / 대상: {args.mine_tab} ({target_label})")
    left, right = read_sheet_ranges(args.base_key, args.base_tab, args.mine_key, args.mine_tab, args.gs_cred)
    
    if left is None or left.empty:
        print("[오류] 기준계좌 데이터를 가져오지 못했습니다.")
        return
    
    diff_df = compare_tables(left, right, args.target_type)
    if diff_df.empty:
        print("[확인] 두 테이블이 완벽하게 일치하거나 비교할 데이터가 없습니다.")
        return
    
    print(f"[정보] {len(diff_df)}개 종목 분석 완료.")
    
    target_label_tab = "대신" if args.target_type == "mine" else "키움"
    analysis_title = f"기준_{target_label_tab}_{datetime.now().strftime('%y%m%d_%H%M%S')}"
    
    output_key = args.output_key if args.output_key else args.mine_key
    write_analysis_sheet(output_key, analysis_title, diff_df, args.gs_cred)


if __name__ == "__main__":
    main()
