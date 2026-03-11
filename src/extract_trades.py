"""
extract_trades.py - 매매일지 자동 작성 스크립트

캡쳐 이미지에서 매매 내역을 추출하여 구글 시트 '매매일지' 탭에 추가합니다.
멀티 OCR 엔진 (Gemini 2.5 Flash, VARCO-VISION) 결과를 비교하여 보여줍니다.
"""

import os
import sys
import re
import argparse
from datetime import datetime
from collections import Counter

# ============================================================
# 터미널 색상 (ANSI)
# ============================================================
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


def normalize_bg_color(bg_color: str) -> str:
    """
    다양한 색상 표현을 GREEN 또는 WHITE로 정규화
    
    실제 매도 배경색: #C4F0CD~#C7EECF (연한 녹색/민트색)
    실제 매도 글자색: #4C8356 (진한 녹색)
    매수: 검은 글씨 + 흰 배경
    """
    if not bg_color:
        return "UNKNOWN"
    
    c = bg_color.strip().upper()
    
    # HEX 코드 직접 분석 (#RRGGBB 형식)
    if c.startswith("#") and len(c) == 7:
        try:
            r = int(c[1:3], 16)
            g = int(c[3:5], 16)
            b = int(c[5:7], 16)
            
            # 실제 매도 배경색 범위: R=192~199, G=238~240, B=205~215 (연녹색)
            # 녹색 성분이 높고, 빨강/파랑보다 녹색이 월등히 높으면 GREEN
            if g > r and g > b and g > 200:
                return "GREEN"
            # 거의 흰색 (모든 채널이 240 이상)
            if r > 240 and g > 240 and b > 240:
                return "WHITE"
            # 연녹색 범위 체크 (R: 180-210, G: 230-250, B: 190-220)
            if 180 <= r <= 210 and 230 <= g <= 250 and 190 <= b <= 220:
                return "GREEN"
            # 기본: 밝기가 높으면 WHITE, 녹색 톤이면 GREEN
            brightness = (r + g + b) / 3
            green_ratio = g / max(1, (r + b) / 2)
            if green_ratio > 1.1 and g > 200:
                return "GREEN"
            if brightness > 250:
                return "WHITE"
        except ValueError:
            pass
    
    # 문자열 기반 판정
    green_keywords = [
        "GREEN", "MINT", "LIME", "EMERALD", "SAGE", "OLIVE",
        "연녹", "녹색", "초록", "민트", "파스텔", "연두",
        "LIGHT GREEN", "PALE GREEN", "SOFT GREEN", "PASTEL"
    ]
    white_keywords = [
        "WHITE", "BLANK", "NONE", "CLEAR", "EMPTY",
        "흰색", "하양", "기본", "없음"
    ]
    
    for kw in green_keywords:
        if kw in c:
            return "GREEN"
    
    for kw in white_keywords:
        if kw in c:
            return "WHITE"
    
    # 기본값: 판별 불가시 원본 유지하되 경고 표시용
    return "UNKNOWN"

# ============================================================
# 설정
# ============================================================
SPREADSHEET_KEY = "1oCLjuqYxGc-RG4ArhF3GeXvQhN_sQDC1qMkh0YBk_lc"
TRADES_TAB_TITLE = "매매일지"
CRED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "stock-holding-log-db46e6d87dd6.json")
GEMINI_KEY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "gemini_key.txt")
STOCK_MASTER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "stock_master.json")
KRX_CACHE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "krx_code_cache.json")
IMAGE_BASE_DIR = r"D:\텔레"
OCR_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "ocr_cache")

# 캐시 디렉토리 생성
if not os.path.exists(OCR_CACHE_DIR):
    os.makedirs(OCR_CACHE_DIR)


# ============================================================
# OCR 결과 캐싱 시스템
# ============================================================
def get_cache_path(date_str: str) -> str:
    """날짜별 캐시 파일 경로 반환"""
    return os.path.join(OCR_CACHE_DIR, f"ocr_cache_{date_str}.json")


def load_ocr_cache(date_str: str) -> dict:
    """OCR 캐시 로드"""
    cache_path = get_cache_path(date_str)
    if os.path.exists(cache_path):
        import json
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_ocr_cache(date_str: str, engine_name: str, results: list):
    """OCR 결과를 캐시에 저장"""
    import json
    cache_path = get_cache_path(date_str)
    cache_data = load_ocr_cache(date_str)
    cache_data[engine_name] = results
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"   ⚠️ 캐시 저장 실패: {e}")



# ============================================================
# 스마트 종목 매칭 시스템
# ============================================================
_stock_master_cache = None

def load_stock_master() -> dict:
    """stock_master.json 및 krx_code_cache.json 로드 (캐싱)"""
    global _stock_master_cache
    if _stock_master_cache is not None:
        return _stock_master_cache
    
    import json
    combined = {}
    
    # 1. krx_code_cache.json (최신 KRX 공식 데이터)
    if os.path.exists(KRX_CACHE_PATH):
        try:
            with open(KRX_CACHE_PATH, "r", encoding="utf-8") as f:
                combined.update(json.load(f))
        except Exception:
            pass
            
    # 2. stock_master.json (사용자 정의 또는 기존 데이터)
    if os.path.exists(STOCK_MASTER_PATH):
        try:
            with open(STOCK_MASTER_PATH, "r", encoding="utf-8") as f:
                combined.update(json.load(f))
        except Exception:
            pass
            
    _stock_master_cache = combined
    return _stock_master_cache


def match_stock_name(ocr_name: str, ocr_code: str = None) -> tuple[str, bool]:
    """
    OCR로 추출된 종목명/코드를 기반으로 정규 종목명 매칭
    우선순위: 1.코드일치(최우선) -> 2.정확일치 -> 3.공백제거일치 -> 4.유사도일치
    
    코드 매칭을 최우선으로 하는 이유:
    OCR에서 엑셀 열 너비가 좁아 종목명이 잘리는 경우가 있음
    (예: "아모레퍼시픽홀딩스3우C" → "아모레퍼시픽홀딩스")
    이 경우 잘린 이름이 다른 종목과 정확일치하여 잘못 매칭될 수 있으므로
    종목코드가 있으면 코드 기반 매칭을 우선 수행
    """
    # 아모레퍼시픽홀딩스 오판독 강제 고정
    if ocr_name.replace(" ", "") == "아모레퍼시픽홀딩스":
        ocr_name = "아모레퍼시픽홀딩스3우C"
        if not ocr_code:
            ocr_code = "00279K"

    stock_master = load_stock_master()
    if not stock_master:
        return ocr_name, False
    
    # 1차: 종목코드로 검색 (가장 강력한 근거 - 코드는 고유하므로 최우선)
    if ocr_code:
        clean_code = str(ocr_code).strip()
        # 순수 숫자 코드는 6자리로 패딩 (예: '5930' -> '005930')
        # 우선주 코드(알파벳 포함, 예: '00279K')는 그대로 유지
        if clean_code.isdigit():
            clean_code = clean_code.zfill(6)
        for name, info in stock_master.items():
            if isinstance(info, dict) and info.get("code") == clean_code:
                return name, True
            elif isinstance(info, str) and info == clean_code:
                return name, True
        
    # 2차: 정확히 일치하는 종목명 찾기
    if ocr_name in stock_master:
        return ocr_name, True
    
    # 3차: 공백 제거 후 비교 (OCR은 공백 오차가 잦음)
    normalized_ocr = ocr_name.replace(" ", "")
    for name in stock_master.keys():
        if name.replace(" ", "") == normalized_ocr:
            return name, True
    
    # 4차: OCR 오타 보정 (Fuzzy Matching)
    best_match = None
    best_score = 0
    for name in stock_master.keys():
        common = sum(1 for c in normalized_ocr if c in name)
        # 최소 2글자 이상 일치하고, (원본 길이 - 1) 이상의 글자가 포함되어야 함
        if common > best_score and common >= max(2, len(normalized_ocr) - 1):
            best_score = common
            best_match = name
    
    # 유사도가 전체 이름의 70% 이상인 경우에만 확정
    if best_match and best_score >= len(normalized_ocr) * 0.7:
        return best_match, True
    
    # 매칭 실패
    return ocr_name, False


def load_gemini_key() -> str:
    """Gemini API 키 로드"""
    if os.path.exists(GEMINI_KEY_PATH):
        with open(GEMINI_KEY_PATH, "r", encoding="utf-8") as f:
            return f.read().strip()
    return None


def get_weekday_korean(date_obj: datetime) -> str:
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    return weekdays[date_obj.weekday()]


def build_image_path(date_str: str) -> str:
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    year, month, day = date_obj.year, date_obj.month, date_obj.day
    weekday = get_weekday_korean(date_obj)
    filename = f"{year}년{month}월{day}일({weekday}).jpg"
    return os.path.join(IMAGE_BASE_DIR, filename)


# ============================================================
# OCR 엔진 1: Gemini API (1.5 Flash / 2.0 Flash)
# ============================================================

# 이미지 분할 임계값 (이 높이 이상이면 분할)
IMAGE_CHUNK_HEIGHT = 1200  # 픽셀
IMAGE_OVERLAP = 100  # 청크 간 겹침 (누락 방지)


def split_image_vertically(image_path: str) -> list:
    """세로로 긴 이미지를 여러 청크로 분할 (PIL 사용)"""
    try:
        from PIL import Image
        import io
    except ImportError:
        return [image_path]  # PIL 없으면 원본 반환
    
    img = Image.open(image_path)
    width, height = img.size
    
    # 분할 필요 없으면 원본 반환
    if height <= IMAGE_CHUNK_HEIGHT:
        return [image_path]
    
    chunks = []
    y = 0
    chunk_idx = 0
    
    while y < height:
        # 청크 영역 계산 (겹침 포함)
        y_end = min(y + IMAGE_CHUNK_HEIGHT, height)
        
        # 이미지 자르기
        chunk = img.crop((0, y, width, y_end))
        
        # 임시 바이트 스트림으로 저장 (파일 생성 없이 메모리에서 처리)
        buffer = io.BytesIO()
        chunk.save(buffer, format="JPEG", quality=95)
        buffer.seek(0)
        chunks.append(buffer.getvalue())
        
        chunk_idx += 1
        
        # 다음 청크 시작점 (겹침 적용)
        y = y_end - IMAGE_OVERLAP
        
        # 마지막에 도달했으면 종료
        if y_end >= height:
            break
    
    print(f"   📐 이미지 분할: {height}px → {len(chunks)}개 청크")
    return chunks


def extract_with_gemini(image_path: str, model_name: str = "gemini-2.5-flash") -> list[dict]:
    """Gemini API를 사용하여 이미지에서 매매 내역 추출 (이미지 분할 지원)"""
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        return []
    
    api_key = load_gemini_key()
    if not api_key:
        return []
    
    # 이미지 분할 시도
    chunks = split_image_vertically(image_path)
    
    # 분할되지 않은 경우 (원본 파일 경로)
    if isinstance(chunks[0], str):
        with open(image_path, "rb") as f:
            chunks = [f.read()]
    
    prompt = """이 이미지는 주식 매매 내역 테이블의 일부입니다.

⚠️ 중요 규칙:
- '매도' 행은 연녹색(민트색) 배경 → bg_color: Green
- '매수' 행은 흰색 배경 → bg_color: White
- 글자색이 초록색이면 해당 행은 '매도'입니다
- 배경에 조금이라도 녹색 음영이 있으면 반드시 'Green'
- 💡 통계적 힌트: '매도'는 주문수량이 2개 이상인 경우가 95% 이상입니다. 수량이 1개인 경우는 '매수'일 가능성이 높으니 배경색과 함께 신중히 판단하세요.

테이블에서 추출할 정보:
- 매매구분: "매수" 또는 "매도" (현금매수/코스매수/K-OTC매수 → 매수, 현금매도/코스매도/K-OTC매도 → 매도)
- 종목코드: 6자리 숫자 (A 제외)
- 종목명
- 수량: 숫자만
- 단가: 숫자만 (소수점 이하 버림)
- 배경색: 'Green' 또는 'White'

📌 출력 형식 (CSV, 헤더 없음):
매수,종목코드,종목명,수량,단가,배경색

예시:
매수,005930,삼성전자,10,55000,White
매도,000660,SK하이닉스,5,120000,Green

모든 행을 빠짐없이 정확하게 추출하세요."""

    try:
        client = genai.Client(api_key=api_key)
        all_trades = []
        
        for i, chunk_data in enumerate(chunks):
            if len(chunks) > 1:
                print(f"      청크 {i+1}/{len(chunks)} 처리 중...", end=" ", flush=True)
            
            response = client.models.generate_content(
                model=model_name,
                contents=[
                    types.Part.from_bytes(data=chunk_data, mime_type="image/jpeg"),
                    prompt
                ]
            )
            
            result_text = response.text.strip()
            chunk_trades = parse_gemini_response(result_text)
            
            if len(chunks) > 1:
                print(f"✅ {len(chunk_trades)}건")
            
            all_trades.extend(chunk_trades)
        
        # 중복 제거 (동일 종목+단가+수량 기준)
        if len(chunks) > 1:
            all_trades = deduplicate_trades(all_trades)
            print(f"   📊 중복 제거 후: {len(all_trades)}건")
        
        return all_trades
        
    except Exception as e:
        err_msg = str(e)
        print(f"   ❌ Gemini 오류: {e}")
        if "quota" in err_msg.lower() or "rate" in err_msg.lower():
            return []  # 할당량 초과
        return []


def parse_gemini_response(result_text: str) -> list[dict]:
    """Gemini 응답 텍스트를 파싱하여 거래 목록 반환"""
    trades = []
    for line in result_text.split("\n"):
        line = line.strip()
        if not line or line.startswith("#") or "," not in line:
            continue
        parts = line.split(",")
        # 최소 5개(기존) 또는 6개(배경색 포함)
        if len(parts) >= 5:
            # 새 형식: 매수/매도,종목코드,종목명,수량,단가,배경색(옵션)
            side = "매수" if "매수" in parts[0] else ("매도" if "매도" in parts[0] else None)
            if not side:
                continue
            code = parts[1].strip()
            name = parts[2].strip()
            try:
                qty = int(parts[3].strip().replace(",", ""))
            except:
                qty = 1
            try:
                price = int(float(parts[4].strip().replace(",", "")))
            except:
                continue
            
            # 배경색 파싱 (있으면 사용, 없으면 None)
            bg_color = None
            if len(parts) >= 6:
                bg_color = parts[5].strip()

            if name and price > 0:
                trades.append({
                    "side": side, 
                    "code": code, 
                    "name": name, 
                    "qty": qty, 
                    "price": price,
                    "bg_color": bg_color
                })
        elif len(parts) >= 4:
            # 구 형식 호환: 매수/매도,종목명,수량,단가
            side = "매수" if "매수" in parts[0] else ("매도" if "매도" in parts[0] else None)
            if not side:
                continue
            name = parts[1].strip()
            try:
                qty = int(parts[2].strip().replace(",", ""))
            except:
                qty = 1
            try:
                price = int(float(parts[3].strip().replace(",", "")))
            except:
                continue
            if name and price > 0:
                trades.append({
                    "side": side, 
                    "code": None, 
                    "name": name, 
                    "qty": qty, 
                    "price": price,
                    "bg_color": None
                })
    
    return trades


def deduplicate_trades(trades: list[dict]) -> list[dict]:
    """중복 거래 제거 (이미지 청킹 겹침으로 인한 동일 행 중복 포함)
    
    이미지 분할 시 겹침 구간에서 같은 행이 두 번 추출될 수 있음.
    이때 종목코드가 다르게 인식될 수 있으므로 (예: 002790 vs 00279K)
    이름+단가+수량+구분이 동일하면 중복으로 판단.
    """
    seen_by_code = set()   # (코드, 단가, 수량, 구분) 기준
    seen_by_name = {}      # (이름, 단가, 수량, 구분) → 인덱스 기준
    unique = []
    for t in trades:
        code_key = (t.get("code") or t["name"], t["price"], t["qty"], t["side"])
        name_normalized = t["name"].replace(" ", "")
        name_key = (name_normalized, t["price"], t["qty"], t["side"])
        
        if code_key in seen_by_code:
            continue  # 코드+단가+수량+구분 완전 동일 → 명확한 중복
        
        if name_key in seen_by_name:
            # 이름+단가+수량+구분 동일하지만 코드가 다름
            # → 청킹 겹침으로 인한 중복 (예: 002790 vs 00279K)
            # 더 정보가 풍부한 코드(알파벳 포함 = 우선주 등)를 보존
            existing_idx = seen_by_name[name_key]
            existing = unique[existing_idx]
            existing_code = existing.get("code") or ""
            new_code = t.get("code") or ""
            
            # 알파벳 포함 코드가 더 구체적 (예: 00279K > 002790)
            if not existing_code.isdigit() or new_code.isdigit():
                pass  # 기존 것 유지
            else:
                unique[existing_idx] = t  # 새 것(알파벳 코드)으로 교체
                seen_by_code.add(code_key)
            continue
        
        seen_by_code.add(code_key)
        seen_by_name[name_key] = len(unique)
        unique.append(t)
    return unique


# ============================================================
# OCR 엔진 2: Naver Clova Donut (OCR-Free Document Understanding)
# ============================================================
def extract_with_donut(image_path: str) -> list[dict]:
    """Naver Clova Donut 모델을 사용하여 이미지에서 매매 내역 추출"""
    try:
        from transformers import DonutProcessor, VisionEncoderDecoderModel
        from PIL import Image
        import torch
        import re
    except ImportError as e:
        print(f"\n   Donut 모듈 오류: {e}")
        return []
    
    try:
        print(f"\n   [Donut] 이미지 로드 중...", end="", flush=True)
        image = Image.open(image_path).convert("RGB")
        print(" 완료")
        
        # 모델 로드 (CUDA 사용)
        print(f"   [Donut] 모델 로드 중...", end="", flush=True)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        if device == "cpu":
            print(f"\n   ⚠️ 경고: CUDA 사용 불가! CPU로 실행됩니다")
        
        model_name = "naver-clova-ix/donut-base"
        processor = DonutProcessor.from_pretrained(model_name)
        model = VisionEncoderDecoderModel.from_pretrained(
            model_name,
            torch_dtype=torch.float16 if device == "cuda" else torch.float32
        ).to(device)
        print(f" 완료 ({device.upper()} 사용)")
        
        # 이미지 전처리
        print(f"   [Donut] 텍스트 추출 중...", end="", flush=True)
        pixel_values = processor(image, return_tensors="pt").pixel_values.to(device)
        if device == "cuda":
            pixel_values = pixel_values.half()
        
        # 텍스트 생성
        task_prompt = "<s_cord-v2>"
        decoder_input_ids = processor.tokenizer(task_prompt, add_special_tokens=False, return_tensors="pt").input_ids.to(device)
        
        with torch.no_grad():
            outputs = model.generate(
                pixel_values,
                decoder_input_ids=decoder_input_ids,
                max_length=512,
                early_stopping=True,
                pad_token_id=processor.tokenizer.pad_token_id,
                eos_token_id=processor.tokenizer.eos_token_id,
                use_cache=True,
                num_beams=1,
                do_sample=False,
            )
        
        result_text = processor.batch_decode(outputs, skip_special_tokens=True)[0]
        print(" 완료")
        
        # 결과 파싱 - 매매 내역 추출
        trades = []
        lines = result_text.split("\n")
        
        for line in lines:
            line = line.strip()
            if "매수" in line or "매도" in line:
                side = "매수" if "매수" in line else "매도"
                numbers = re.findall(r'[\d,]+', line)
                if len(numbers) >= 1:
                    try:
                        price = int(numbers[-1].replace(",", ""))
                        qty = int(numbers[-2].replace(",", "")) if len(numbers) >= 2 else 1
                        name_match = re.search(r'(?:매수|매도)\s*([가-힣A-Za-z0-9]+)', line)
                        if name_match:
                            name = name_match.group(1)
                            if price > 0:
                                trades.append({"side": side, "name": name, "qty": qty, "price": price, "bg_color": None})
                    except:
                        pass
        
        return trades
    except Exception as e:
        print(f"\n   Donut 오류: {e}")
        return []



# ============================================================
# 멀티 OCR 결과 비교 및 병합
# ============================================================
def run_all_ocr_engines(image_path: str, date_str: str) -> dict:
    """두 가지 OCR 엔진 실행 및 결과 수집 (캐싱 적용)"""
    # 기존 캐시 로드
    results = load_ocr_cache(date_str)
    
    if results:
        cached_engines = ", ".join(results.keys())
        print(f"📦 캐시된 OCR 데이터 발견: {cached_engines}")
    
    print("🔍 OCR 엔진 실행 중 (Gemini 2.5 Flash)...")
    
    # Gemini 2.5 Flash
    if "gemini-2.5-flash" in results:
        print("   Gemini 2.5 Flash... ✅ (캐시 사용)")
    else:
        print("   Gemini 2.5 Flash...", end=" ", flush=True)
        try:
            r1 = extract_with_gemini(image_path, "gemini-2.5-flash")
            if r1:
                results["gemini-2.5-flash"] = r1
                save_ocr_cache(date_str, "gemini-2.5-flash", r1)
                print(f"✅ {len(r1)}건")
            else:
                print("⏭️ SKIP (할당량 초과 또는 오류)")
        except Exception as e:
            print(f"❌ 오류: {e}")
    
    # Gemini 단독 모드 - 로컬 모델 없이 빠르고 안정적
    # (캐싱 시스템으로 토큰 절약)
    
    return results


def normalize_trade_key(trade: dict) -> str:
    """비교를 위한 정규화된 키 생성 (종목코드 포함하여 동명이종 구분)"""
    name = trade["name"].replace(" ", "").lower()
    code = trade.get("code") or ""
    return f"{trade['side']}_{code}_{name}_{trade['price']}"


def merge_and_compare_results(all_results: dict) -> list[dict]:
    """여러 OCR 결과를 병합하고 일치도 계산 (이미지 원본 순서 유지)"""
    if not all_results:
        return []
    
    # 첫 번째 엔진 결과를 기준 순서로 사용 (이미지 원본 순서)
    first_engine_results = list(all_results.values())[0] if all_results else []
    
    # 모든 거래를 수집하고 일치 횟수 카운트
    trade_votes = {}  # key -> {trade: dict, sources: list, count: int, order: int}
    
    order_counter = 0
    for source, trades in all_results.items():
        for trade in trades:
            key = normalize_trade_key(trade)
            if key not in trade_votes:
                trade_votes[key] = {
                    "trade": trade,
                    "sources": [],
                    "count": 0,
                    "order": order_counter  # 최초 등장 순서 기록
                }
                order_counter += 1
            trade_votes[key]["sources"].append(source)
            trade_votes[key]["count"] += 1
    
    # 결과 정리 (이미지 원본 순서 유지 - order 기준)
    merged = []
    for key, data in sorted(trade_votes.items(), key=lambda x: x[1]["order"]):
        trade = data["trade"].copy()
        trade["_sources"] = data["sources"]
        trade["_match_count"] = data["count"]
        merged.append(trade)
    
    return merged


def display_comparison_results(all_results: dict, merged: list[dict]):
    """비교 결과를 터미널에 표시"""
    print(f"\n{'='*70}")
    print(f"📊 OCR 결과 비교 (총 {len(all_results)}개 엔진)")
    print(f"{'='*70}")
    
    # 엔진별 결과 수
    for engine, trades in all_results.items():
        print(f"   {engine}: {len(trades)}건")
    
    print(f"\n{'='*70}")
    print(f"📋 병합된 결과 (총 {len(merged)}건)")
    print(f"{'='*70}")
    print(f"{'일치':^6} | {'매매':^4} | {'종목명':^18} | {'수량':^6} | {'단가':^12} | 소스")
    print(f"{'-'*70}")
    
    total_engines = len(all_results)
    
    for i, trade in enumerate(merged, 1):
        match_count = trade["_match_count"]
        sources = trade["_sources"]
        
        # 색상 결정
        if match_count == total_engines and total_engines > 1:
            color = Colors.GREEN  # 모든 엔진 일치 - 녹색
            match_icon = "✓✓✓"
        elif match_count >= 2:
            color = Colors.YELLOW  # 2개 이상 일치 - 노란색
            match_icon = "✓✓ "
        else:
            color = Colors.RESET  # 1개만 - 기본
            match_icon = "✓  "
        
        source_abbr = ",".join([s.split("-")[0][:3] for s in sources])
        
        line = f"{color}{match_icon:^6} | {trade['side']:^4} | {trade['name']:<18} | {trade['qty']:>6} | {trade['price']:>10,}원 | {source_abbr}{Colors.RESET}"
        print(line)
    
    print(f"{'='*70}")
    print(f"범례: {Colors.GREEN}✓✓✓ = 모든 엔진 일치{Colors.RESET}, {Colors.YELLOW}✓✓ = 2개 일치{Colors.RESET}, ✓ = 1개만")
    
    # 매도/매수/의심 요약
    sell_count = sum(1 for t in merged if t["side"] == "매도")
    buy_count_raw = sum(1 for t in merged if t["side"] == "매수")
    
    # 매도 의심 판별 (매도 영역 내 매수 qty>=2)
    sell_indices = [i for i, t in enumerate(merged) if t["side"] == "매도"]
    sell_suspect_names = []
    if sell_indices:
        region_start, region_end = sell_indices[0], sell_indices[-1]
        for i in range(region_start, region_end + 1):
            t = merged[i]
            if t["side"] == "매수" and t["qty"] >= 2:
                sell_suspect_names.append(t["name"])
    
    # 매수 의심 판별 (매수 영역 내 매도 qty==1)
    buy_indices = [i for i, t in enumerate(merged) if t["side"] == "매수"]
    buy_suspect_names = []
    if buy_indices:
        region_start, region_end = buy_indices[0], buy_indices[-1]
        for i in range(region_start, region_end + 1):
            t = merged[i]
            if t["side"] == "매도" and t["qty"] == 1:
                buy_suspect_names.append(t["name"])
    
    sell_suspect_count = len(sell_suspect_names)
    buy_suspect_count = len(buy_suspect_names)
    pure_buy = buy_count_raw - sell_suspect_count
    pure_sell = sell_count - buy_suspect_count
    
    total = pure_sell + sell_suspect_count + buy_suspect_count + pure_buy
    print(f"\n   📊 매도: {pure_sell}건 | 매도의심: {sell_suspect_count}건 | 매수의심: {buy_suspect_count}건 | 매수: {pure_buy}건 (총 {total}건)")
    if sell_suspect_names:
        print(f"   🔍 매도의심 종목: {Colors.YELLOW}{', '.join(sell_suspect_names)}{Colors.RESET}")
    if buy_suspect_names:
        print(f"   🔍 매수의심 종목: {Colors.CYAN}{', '.join(buy_suspect_names)}{Colors.RESET}")


def confirm_trades_interactively(merged: list[dict], date_str: str) -> list[dict]:
    """사용자 대화형 컨펌 (스마트 종목 매칭 적용, 매도→매수 순서, 매도의심 포함)"""
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    formatted_date = f"{date_obj.year}. {date_obj.month}. {date_obj.day}"
    
    # ── 매도 의심 판별 ──
    # 원본 순서에서 매도 영역(연속된 매도 구간) 내에 끼어있는 매수(qty>=2)를 매도 의심으로 분류
    # 예: [매도, 매도, 매수(qty=3), 매도] → 매수(qty=3)는 매도 의심
    for t in merged:
        t["_sell_suspect"] = False
    
    # 1) 매도 거래의 인덱스 범위를 파악하여 매도 영역 결정
    sell_indices = [i for i, t in enumerate(merged) if t["side"] == "매도"]
    
    if sell_indices:
        # 매도 영역: 첫 번째 매도부터 마지막 매도까지의 범위
        sell_region_start = sell_indices[0]
        sell_region_end = sell_indices[-1]
        
        # 매도 영역 내에 있는 매수(qty>=2) 거래를 매도 의심으로 표시
        for i in range(sell_region_start, sell_region_end + 1):
            t = merged[i]
            if t["side"] == "매수" and t["qty"] >= 2:
                t["_sell_suspect"] = True
    
    # 매도 먼저, 그 다음 매수 순서로 정렬 (매도 의심은 매도 그룹에 포함)
    def sort_key(x):
        idx = merged.index(x)
        if x["side"] == "매도" or x.get("_sell_suspect"):
            return (0, idx)  # 매도 그룹 (원본 순서 유지)
        else:
            return (1, idx)  # 매수 그룹 (원본 순서 유지)
    
    sorted_merged = sorted(merged, key=sort_key)
    
    print(f"\n{'='*70}")
    print(f"📝 매매 내역 확인 (날짜: {formatted_date}) - 매도→매수 순")
    print(f"{'='*70}")
    
    print(f"{'-'*70}")

    print("  Enter: 수락 | 숫자: 수량수정 | 매수/매도: 구분수정")
    print("  매수/매도,종목,수량,단가: 전체수정 | d: 삭제 | q: 취소 | all: 일괄수락")
    print(f"{'='*70}\n")
    
    confirmed = []
    for i, trade in enumerate(sorted_merged, 1):
        match_count = trade.get("_match_count", 1)
        bg_color = trade.get("bg_color", None)
        
        # 스마트 종목 매칭 적용 (종목코드도 함께 사용)
        ocr_code = trade.get("code", None)
        matched_name, is_matched = match_stock_name(trade["name"], ocr_code)
        trade["name"] = matched_name  # 정제된 종목명으로 업데이트
        
        # 색상 결정
        color_code = Colors.RESET
        match_icon = "✓"
        if is_matched:
            color_code = Colors.GREEN  # 자동 매칭 성공 - 녹색
            match_icon = "✓"
        elif match_count >= 2:
            color_code = Colors.YELLOW
            match_icon = "?"
        else:
            color_code = Colors.RED  # 매칭 실패 - 빨간색
            match_icon = "✗"
            
        # [신규 기능] 배경색 및 수량 검증 (더블 체크)
        # 매수 -> White, 매도 -> Green이어야 함
        # 매도 -> 수량 >= 2 (95%)
        warnings = []
        
        # 1. 색상 검증
        if bg_color:
            normalized_color = normalize_bg_color(bg_color)
            if trade['side'] == "매수" and normalized_color == "GREEN":
                warnings.append(f"색상불일치: {bg_color}→{normalized_color}")
            elif trade['side'] == "매도" and normalized_color == "WHITE":
                warnings.append(f"색상불일치: {bg_color}→{normalized_color}")
        
        # 2. 수량 규칙 검증 (매도는 2개 이상이 95%)
        if trade['side'] == "매도" and trade['qty'] == 1:
            warnings.append("수량주의: 매도인데 1주")
        elif trade['side'] == "매수" and trade['qty'] >= 2:
            # 매수인데 수량이 많으면 혹시 매도를 매수로 인식했는지 확인 유도
            if bg_color and normalize_bg_color(bg_color) == "GREEN":
                pass # 위에서 색상불일치로 잡힘
            else:
                warnings.append("수량확인: 매수 2주 이상")

        # 3. 매도 의심 표시
        is_sell_suspect = trade.get("_sell_suspect", False)
        if is_sell_suspect:
            warnings.append("매도의심: 매도 영역 내 매수(qty≥2)")

        color_warning = ""
        if warnings:
            color_warning = f"{Colors.RED}[⚠️ {', '.join(warnings)}]{Colors.RESET}"

        # 매도 의심 태그
        suspect_tag = f"{Colors.YELLOW}[매도의심]{Colors.RESET} " if is_sell_suspect else ""
        display = f"{color_code}[{i:3d}/{len(sorted_merged)}] {match_icon} {suspect_tag}{trade['side']:4s} | {trade['name']:<18} | {trade['qty']:>4}주 | {trade['price']:>10,}원{Colors.RESET} {color_warning}"
        print(display)
        
        # 의심 거래: 1=매수, 2=매도 선택
        if is_sell_suspect:
            print(f"       💡 매도의심 → {Colors.YELLOW}1: 매수 | 2: 매도{Colors.RESET}")
            user_input = input(f"       → [1=매수, 2=매도]: ").strip()
        # 자동 매칭된 종목은 수량/단가만 간단히 확인
        elif is_matched and not warnings:
            user_input = input(f"       → 수량({trade['qty']}) 단가({trade['price']:,}) [Enter=OK]: ").strip()
        else:
            if not is_matched:
                print(f"       ⚠️ 종목 미매칭! 직접 확인 필요")
            if warnings:
                print(f"       ⚠️ 경고 발생: {', '.join(warnings)}")
                
            user_input = input("       → ").strip()
        
        if user_input.lower() in ["q", "quit"]:
            print("\n❌ 전체 취소")
            return []
        elif user_input.lower() in ["done", "all"]:
            # 나머지 모두 자동 수락 (검증 로직 적용)
            current_trade_fixed = trade.copy()
            # 현재 거래가 매도의심이면 매도로 변환
            if current_trade_fixed.get("_sell_suspect"):
                current_trade_fixed["side"] = "매도"
            confirmed.append({k: v for k, v in current_trade_fixed.items() if not k.startswith("_")})
            
            # 남은 항목들에 대해서도 검증 및 보정 수행
            for remaining in sorted_merged[i:]:
                rem_trade = remaining.copy()
                ocr_code_r = rem_trade.get("code", None)
                matched_name_r, is_matched_r = match_stock_name(rem_trade["name"], ocr_code_r)
                rem_trade["name"] = matched_name_r
                
                # 매도의심 거래는 매도로 자동 변환
                if rem_trade.get("_sell_suspect"):
                    rem_trade["side"] = "매도"
                
                confirmed.append({k: v for k, v in rem_trade.items() if not k.startswith("_")})
            
            print(f"       ✅ 나머지 {len(sorted_merged) - i + 1}건 일괄 검증 및 수락 완료")
            break
        elif user_input.lower() in ["d", "delete"]:
            print("       ⏭️ 삭제")
            continue
        elif user_input == "" and not is_sell_suspect:
            confirmed.append({k: v for k, v in trade.items() if not k.startswith("_")})
            print("       ✅ 수락")
        elif is_sell_suspect and user_input in ["1", "2", ""]:
            # 의심 거래 숫자 선택 (빈 입력 시 반복 요청)
            while user_input not in ["1", "2"]:
                user_input = input(f"       → 1 또는 2를 입력하세요 [1=매수, 2=매도]: ").strip()
            trade["side"] = "매수" if user_input == "1" else "매도"
            confirmed.append({k: v for k, v in trade.items() if not k.startswith("_")})
            print(f"       ✅ → {trade['side']}")
        elif user_input.isdigit():
            trade["qty"] = int(user_input)
            confirmed.append({k: v for k, v in trade.items() if not k.startswith("_")})
            print(f"       ✏️ 수량→{trade['qty']}주")
        # [신규] 매수/매도만 입력하면 매매구분만 변경
        elif user_input in ["매수", "매도"]:
            old_side = trade["side"]
            trade["side"] = user_input
            confirmed.append({k: v for k, v in trade.items() if not k.startswith("_")})
            print(f"       ✏️ 구분 {old_side}→{trade['side']}")
        else:
            parts = user_input.split(",")
            if len(parts) >= 4:
                try:
                    side = "매수" if "매수" in parts[0] else "매도"
                    name = parts[1].strip()
                    qty = int(parts[2].strip().replace(",", ""))
                    price = int(float(parts[3].strip().replace(",", "")))
                    confirmed.append({"side": side, "name": name, "qty": qty, "price": price})
                    print(f"       ✏️ 수정완료")
                except Exception as e:
                    print(f"       ⚠️ 오류. 원본유지")
                    confirmed.append({k: v for k, v in trade.items() if not k.startswith("_")})
            else:
                print("       ⚠️ 형식오류. 원본유지")
                confirmed.append({k: v for k, v in trade.items() if not k.startswith("_")})
    
    # 추가 입력
    print(f"\n{'='*70}")
    print("➕ 추가 입력 (완료: Enter만) - 형식: 매수/매도,종목명,수량,단가")
    print(f"{'='*70}")
    
    while True:
        user_input = input("추가 → ").strip()
        if not user_input:
            break
        parts = user_input.split(",")
        if len(parts) >= 4:
            try:
                side = "매수" if "매수" in parts[0] else "매도"
                name = parts[1].strip()
                qty = int(parts[2].strip().replace(",", ""))
                price = int(float(parts[3].strip().replace(",", "")))
                confirmed.append({"side": side, "name": name, "qty": qty, "price": price})
                print(f"       ✅ 추가됨")
            except:
                print(f"       ⚠️ 형식 오류")
        else:
            print(f"       ⚠️ 형식: 매수/매도,종목명,수량,단가")
    
    print(f"\n✅ 확인 완료: {len(confirmed)}건")
    return confirmed


def append_trades_to_sheet(trades: list[dict], date_str: str) -> None:
    """구글 시트 저장"""
    if not trades:
        print("⚠️ 저장할 내역 없음")
        return
    
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("❌ gspread 설치 필요")
        sys.exit(1)
    
    import time
    
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    formatted_date = f"{date_obj.year}. {date_obj.month}. {date_obj.day}"
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CRED_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    
    # 503 오류 대비 재시도 로직 (최대 5회)
    max_retries = 5
    retry_delay = 2
    
    ws = None
    for attempt in range(max_retries):
        try:
            sh = gc.open_by_key(SPREADSHEET_KEY)
            
            for w in sh.worksheets():
                if w.title == TRADES_TAB_TITLE:
                    ws = w
                    break
            
            if ws is None:
                print(f"❌ '{TRADES_TAB_TITLE}' 탭 없음")
                sys.exit(1)
            
            all_values = ws.get_all_values()
            last_row = len(all_values)
            
            print(f"\n📊 현재 마지막 행: {last_row}")
            
            rows_to_add = []
            for index, trade in enumerate(trades):
                current_row = last_row + 1 + index
                
                # 수식 생성 (G~K열)
                formula_g = f"=E{current_row}*F{current_row}"                # 매수금액
                formula_h = f'=if(B{current_row}="매수",E{current_row},-E{current_row})' # 수량변동
                formula_i = f"=F{current_row}*H{current_row}"                # 실제손익
                formula_j = f'=TEXT(A{current_row}, "ddd")'                   # 요일
                formula_k = f"=WEEKNUM(A{current_row}, 2)"                    # 주차

                rows_to_add.append([
                    formatted_date,     # A: 날짜
                    trade["side"],      # B: 매매구분
                    "",                 # C: 종목번호 (빈칸)
                    trade["name"],      # D: 종목명
                    trade["qty"],       # E: 수량
                    trade["price"],     # F: 단가
                    formula_g,          # G: 매수금액
                    formula_h,          # H: 수량변동
                    formula_i,          # I: 실제손익
                    formula_j,          # J: 요일
                    formula_k           # K: 주차
                ])
            
            start_cell = f"A{last_row + 1}"
            ws.update(values=rows_to_add, range_name=start_cell, value_input_option="USER_ENTERED")
            
            print(f"✅ {len(trades)}건 저장 완료! (행 {last_row + 1}~{last_row + len(trades)})")
            return  # 성공 시 종료

        except Exception as e:
            # gspread APIError 중 503 혹은 기타 네트워크 오류일 경우 재시도
            error_msg = str(e)
            if "503" in error_msg or "unavailable" in error_msg.lower():
                if attempt < max_retries - 1:
                    print(f"   ⚠️ 서버 일시적 오류(503) 발생. {retry_delay}초 후 재시도합니다... ({attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
            
            # 그 외 오류는 즉시 발생
            print(f"❌ 저장 중 오류 발생: {e}")
            break


def main():
    # ANSI 색상 활성화 (Windows)
    os.system("")
    
    parser = argparse.ArgumentParser(description="멀티 OCR 매매일지 자동화")
    today_default = datetime.now().strftime("%Y%m%d")
    parser.add_argument("--date", default=today_default, help=f"날짜 (기본: {today_default})")
    parser.add_argument("--image", default=None, help="이미지 경로")
    parser.add_argument("--dry-run", action="store_true", help="저장 없이 확인만")
    args = parser.parse_args()
    
    date_str = args.date.replace("-", "").replace("/", "")
    if not date_str.isdigit() or len(date_str) != 8:
        print(f"❌ 날짜 형식 오류: {args.date}")
        sys.exit(1)
    
    image_path = args.image if args.image else build_image_path(date_str)
    
    if not os.path.exists(image_path):
        print(f"❌ 이미지 파일 없음: {image_path}")
        sys.exit(1)
    
    print(f"\n{'='*70}")
    print(f"📅 날짜: {date_str}")
    print(f"🖼️ 이미지: {image_path}")
    print(f"{'='*70}\n")
    
    # 1. 모든 OCR 엔진 실행
    all_results = run_all_ocr_engines(image_path, date_str)
    
    if not all_results:
        print("❌ 모든 OCR 엔진 실패")
        sys.exit(1)
    
    # 2. 결과 병합 및 비교
    merged = merge_and_compare_results(all_results)
    
    # 3. 비교 결과 표시
    display_comparison_results(all_results, merged)
    
    # 4. 사용자 확인
    confirmed = confirm_trades_interactively(merged, date_str)
    
    if not confirmed:
        print("⚠️ 저장할 내역이 없거나 취소되었습니다.")
        sys.exit(2)  # Exit code 2: 취소/데이터 없음 (배치 파일에서 다음 단계 진행 안 함)
    
    # 5. 저장
    if args.dry_run:
        print("\n🔍 [Dry Run] 저장 건너뜀")
        for t in confirmed:
            print(f"  {t['side']} | {t['name']} | {t['qty']}주 | {t['price']:,}원")
        sys.exit(2)  # Dry run도 다음 단계(업데이트) 진행 안 함
    else:
        append_trades_to_sheet(confirmed, date_str)
        print("\n✅ 모든 데이터가 시트에 저장되었습니다.")
    
    print("\n🎉 완료!")
    sys.exit(0)  # 성공 시 0 반환 (다음 단계 진행)


if __name__ == "__main__":
    main()
