#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import time

# src 디렉토리를 경로에 추가
sys.path.append(os.path.join(os.getcwd(), 'src'))

from update_holdings_us import run_daily_update, get_previous_business_day
import gspread
from google.oauth2.service_account import Credentials

def main():
    parser = argparse.ArgumentParser(description="미국 주식 잔고 일괄 업데이트 (밀린 일자 처리용)");
    parser.add_argument("--start", required=True, help="시작 날짜 (YYYYMMDD)");
    parser.add_argument("--end", default=datetime.now().strftime("%Y%m%d"), help="종료 날짜 (YYYYMMDD, 기본값: 오늘)");
    parser.add_argument("--gs-holdings-key", required=True, help="잔고 스프레드시트 키");
    parser.add_argument("--gs-trades-key", required=True, help="매매일지 스프레드시트 키");
    parser.add_argument("--gs-trades-gid", type=int, default=None, help="매매일지 탭 gid");
    parser.add_argument("--gs-cred", default=None, help="인증키 파일 경로");
    parser.add_argument("--allow-empty-prev", action="store_true", help="전일 데이터가 없어도 무시하고 빈 잔고에서 시작");
    parser.add_argument("--all-days", action="store_true", help="매매 내역 여부와 상관없이 모든 영업일 처리");
    parser.add_argument("--dry-run", action="store_true", help="실제 쓰기 없이 로그만 출력");

    args = parser.parse_args();

    start_date = datetime.strptime(args.start, "%Y%m%d");
    end_date = datetime.strptime(args.end, "%Y%m%d");

    target_dates = [];
    if args.all_days:
        # 날짜 범위 생성 (주말 제외 모든 영업일)
        date_range = pd.date_range(start=start_date, end=end_date);
        target_dates = [d.strftime("%Y%m%d") for d in date_range if d.weekday() < 5];
        print(f"\n🚀 일괄 업데이트 시작 (모든 영업일 모드): {args.start} ~ {args.end} (총 {len(target_dates)}일)")
    else:
        # 매매일지에서 실제 날짜들 추출
        print(f"\n📂 매매일지에서 업데이트할 날짜를 추출하는 중...")
        try:
            scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            cred_path = args.gs_cred or os.path.join(os.getcwd(), "config", "stock-holding-log-db46e6d87dd6.json")
            if not os.path.exists(cred_path):
                 cred_path = os.path.join(os.getcwd(), "stock-holding-log-db46e6d87dd6.json")
            creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(args.gs_trades_key)
            if args.gs_trades_gid:
                ws = next((w for w in sh.worksheets() if str(w.id) == str(args.gs_trades_gid)), None)
            else:
                ws = sh.get_worksheet(0)
            
            if ws is None:
                raise ValueError("매매일지 워크시트를 찾을 수 없습니다.")
            
            data = ws.get_all_values()
            if not data:
                raise ValueError("매매일지에 데이터가 없습니다.")
            
            df_trades = pd.DataFrame(data[1:], columns=data[0])
            # 날짜 컬럼 찾기 (내부 로직과 동일하게 탐색)
            date_col = next((c for c in df_trades.columns if any(x in str(c) for x in ["날짜", "date", "Date"])), None)
            if not date_col:
                raise ValueError("매매일지에서 '날짜' 컬럼을 찾을 수 없습니다.")
            
            # 날짜 정규화 및 필터링
            all_trade_dates = []
            for d in df_trades[date_col].unique():
                try:
                    # "2025. 10. 29" 형식을 "20251029"로 변환
                    clean_d = str(d).replace(".", "").replace(" ", "").strip()
                    if len(clean_d) == 8 and clean_d.isdigit():
                        dt = datetime.strptime(clean_d, "%Y%m%d")
                        if start_date <= dt <= end_date:
                            all_trade_dates.append(clean_d)
                except:
                    continue
            
            target_dates = sorted(list(set(all_trade_dates)))
            print(f"✅ 매매 내역이 발견된 날짜들: {', '.join(target_dates)}")
            print(f"🚀 일괄 업데이트 시작 (매매일지 모드): 총 {len(target_dates)}개 일자")
        except Exception as e:
            print(f"❌ 날짜 추출 중 오류 발생: {e}")
            print("모든 영업일 모드로 전환합니다.")
            date_range = pd.date_range(start=start_date, end=end_date);
            target_dates = [d.strftime("%Y%m%d") for d in date_range if d.weekday() < 5];

    print("="*60)

    for current_date_str in target_dates:
        print(f"\n▶ [{current_date_str}] 업데이트 진행 중...");
        
        # 부모 args 객체 모방
        daily_args = argparse.Namespace(
            date=current_date_str,
            gs_holdings_key=args.gs_holdings_key,
            gs_trades_key=args.gs_trades_key,
            gs_trades_gid=args.gs_trades_gid,
            gs_trades_title=None,
            gs_holdings_tab=None,
            gs_out_tab=None,
            gs_cred=args.gs_cred,
            gs_kotc_key=None,
            gs_kotc_tab=None,
            override_price=None,
            override_json=None,
            no_fdr_update=False,
            allow_empty_prev=args.allow_empty_prev,
            dry_run=args.dry_run,
            debug_one_name=None,
            debug_like=None,
            holidays=None,
            bal_code=None, bal_name=None, bal_qty=None, bal_avg=None,
            tr_code=None, tr_name=None, tr_side=None, tr_qty=None, tr_price=None, tr_sign=None
        );

        try:
            run_daily_update(daily_args);
            print(f"✅ [{current_date_str}] 업데이트 완료");
            # 구글 API 할당량 제한을 고려하여 약간의 휴식
            time.sleep(2);
        except Exception as e:
            print(f"❌ [{current_date_str}] 업데이트 중 오류 발생: {e}");
            user_input = input("계속 진행하시겠습니까? (y/n): ").lower();
            if user_input != 'y':
                print("일괄 업데이트를 중단합니다.");
                break;

    print("\n" + "="*60)
    print("🏁 모든 일괄 업데이트 프로세스가 완료되었습니다.")

if __name__ == "__main__":
    main();
