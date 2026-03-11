import pandas as pd
import requests
import json
import os

def fetch_all_krx_stocks():
    """KRX API를 사용하여 전체 상장 종목 목록(이름, 코드, 시장)을 가져와 저장"""
    url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    payload = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01901', 
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'share': '1',
        'csvxls_isNo': 'false',
    }
    # 사용자 제공 쿠키 (최신 확인값)
    cookie_str = "JSESSIONID=89F7C2B2A340C7F57C085533175BC7FB.6be05533175bc7fb; __smVisitorID=Gv-KshK0l_x; KRX_UTL=https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020201"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020201',
        'Cookie': cookie_str
    }
    
    try:
        print("KRX에서 전체 종목 목록 요청 중...")
        res = requests.post(url, data=payload, headers=headers, timeout=15)
        if res.status_code != 200:
            print(f"오류: HTTP {res.status_code}")
            return
            
        data = res.json()
        if 'OutBlock_1' not in data:
            print(f"오류: 데이터 형식이 올바르지 않음 ({data.get('RTN_MSG', 'No message')})")
            return
            
        df = pd.DataFrame(data['OutBlock_1'])
        # ISU_ABBRV(이름), ISU_SRT_CD(코드), MKT_NM(시장)
        stock_master = {}
        for _, row in df.iterrows():
            name = row['ISU_ABBRV'].strip()
            code = row['ISU_SRT_CD'].strip()
            market = row['MKT_NM'].strip()
            stock_master[name] = {
                "code": code,
                "market": market
            }
            
        # JSON 저장
        os.makedirs("data", exist_ok=True)
        with open("data/stock_master.json", "w", encoding="utf-8") as f:
            json.dump(stock_master, f, ensure_ascii=False, indent=2)
            
        print(f"성공: {len(stock_master)}개 종목이 data/stock_master.json에 저장되었습니다.")
        
        # AJ네트웍스 확인
        if "AJ네트웍스" in stock_master:
            print(f"확인: AJ네트웍스 ({stock_master['AJ네트웍스']['code']}) 발견됨.")
        else:
            print("경고: AJ네트웍스를 찾을 수 없습니다.")
            
    except Exception as e:
        print(f"예외 발생: {e}")

if __name__ == "__main__":
    fetch_all_krx_stocks()
