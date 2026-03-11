import json

path = 'data/krx_code_cache.json'
with open(path, 'r', encoding='utf-8') as f:
    cache = json.load(f)

# OCI 수정: 010060(OCI홀딩스) -> 456040(OCI)
print(f"수정 전 OCI: {cache.get('OCI')}")
cache['OCI'] = '456040'
print(f"수정 후 OCI: {cache.get('OCI')}")

with open(path, 'w', encoding='utf-8') as f:
    json.dump(cache, f, ensure_ascii=False, indent=2)

print('캐시 수정 완료!')
