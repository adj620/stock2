"""
krx_code_cache.json을 stock_master.json 기준으로 정리하는 스크립트
- stock_master에 있는 종목은 stock_master의 코드로 덮어씀
- 중복되거나 잘못된 매핑 수정
"""
import json
import os

def main():
    # 파일 경로
    cache_path = os.path.join("data", "krx_code_cache.json")
    master_path = os.path.join("data", "stock_master.json")
    
    # 데이터 로드
    with open(cache_path, "r", encoding="utf-8") as f:
        cache = json.load(f)
    
    with open(master_path, "r", encoding="utf-8") as f:
        master = json.load(f)
    
    print(f"캐시 종목 수: {len(cache)}")
    print(f"마스터 종목 수: {len(master)}")
    print()
    
    # stock_master 기준으로 캐시 검증 및 수정
    fixed_count = 0
    for name, master_info in master.items():
        master_code = master_info.get("code")
        if not master_code:
            continue
        
        # 캐시에 있는 경우
        if name in cache:
            cache_code = cache[name]
            if cache_code != master_code:
                print(f"수정: {name} - 캐시({cache_code}) -> 마스터({master_code})")
                cache[name] = master_code
                fixed_count += 1
        else:
            # 캐시에 없으면 추가
            cache[name] = master_code
    
    # 결과 저장
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    
    print()
    print(f"수정된 종목 수: {fixed_count}")
    print(f"최종 캐시 종목 수: {len(cache)}")
    print("캐시 동기화 완료!")

if __name__ == "__main__":
    main()
