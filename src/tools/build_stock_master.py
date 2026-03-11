import requests
from bs4 import BeautifulSoup
import json
import os
import time
import re

def build_master():
    """네이버 금융 시세 페이지를 크롤링하여 전체 상장 종목(KOSPI, KOSDAQ)의 이름-코드 매핑 생성"""
    base_url = "https://finance.naver.com/sise/sise_market_sum.naver"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    master_data = {}
    
    # sosok=0 (KOSPI), sosok=1 (KOSDAQ)
    for sosok in [0, 1]:
        market_name = "KOSPI" if sosok == 0 else "KOSDAQ"
        print(f"--- {market_name} 크롤링 시작 ---")
        
        # 첫 페이지에서 마지막 페이지 번호 찾기
        res = requests.get(f"{base_url}?sosok={sosok}&page=1", headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        last_page_tag = soup.select_one("td.pgRR a")
        if last_page_tag:
            m = re.search(r'page=(\d+)', last_page_tag.get("href", ""))
            last_page = int(m.group(1)) if m else 1
        else:
            # 페이지 번호가 없는 경우는 1페이지만 있는 것
            last_page = 1
            
        print(f"총 {last_page} 페이지 발견.")
        
        for page in range(1, last_page + 1):
            print(f"페이지 {page}/{last_page} 처리 중...", end="\r")
            url = f"{base_url}?sosok={sosok}&page={page}"
            res = requests.get(url, headers=headers)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            table = soup.select_one("table.type_2")
            if not table: continue
            
            rows = table.select("tr")
            for row in rows:
                link = row.select_one("a.tltle")
                if link:
                    name = link.get_text(strip=True)
                    href = link.get("href", "")
                    m_code = re.search(r'code=(\d{6})', href)
                    if m_code:
                        code = m_code.group(1)
                        master_data[name] = {
                            "code": code,
                            "market": market_name
                        }
            time.sleep(0.1) # 서버 부하 방지
        print(f"\n{market_name} 완료. 현재까지 총 {len(master_data)}개 종목 확보.")

    # 기존 섹터 정보와 병합 시도 (섹터 매핑 파일이 있다면)
    sector_path = "data/sector_mapping.json"
    if os.path.exists(sector_path):
        try:
            with open(sector_path, "r", encoding="utf-8") as f:
                sectors = json.load(f)
            for name, info in master_data.items():
                code = info["code"]
                if code in sectors:
                    master_data[name]["sector"] = sectors[code]
        except Exception as e:
            print(f"섹터 정보 병합 중 오류: {e}")

    # 최종 저장
    os.makedirs("data", exist_ok=True)
    with open("data/stock_master.json", "w", encoding="utf-8") as f:
        json.dump(master_data, f, ensure_ascii=False, indent=2)
    
    print(f"최종 완료: {len(master_data)}개 종목이 data/stock_master.json에 저장되었습니다.")
    
    # AJ네트웍스 확인
    if "AJ네트웍스" in master_data:
        print(f"검증: AJ네트웍스 -> {master_data['AJ네트웍스']}")
    else:
        print("경고: AJ네트웍스를 찾을 수 없습니다.")

if __name__ == "__main__":
    build_master()
