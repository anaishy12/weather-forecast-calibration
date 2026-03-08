import requests
from datetime import datetime

# 1. 여기에 공공데이터포털에서 받은 "디코딩(Decoding)" 키를 넣으세요!
KMA_KEY = "WDrv7Kj2Su267-yo9trtNA"

# 2. 오늘 날짜로 세팅
today = datetime.now().strftime('%Y%m%d')

url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
params = {
    'serviceKey': KMA_KEY,
    'pageNo': '1', 
    'numOfRows': '10', 
    'dataType': 'JSON',
    'base_date': today, 
    'base_time': '1400', # 오후 2시 예보 기준
    'nx': '61',          # 강남구 격자 X
    'ny': '125'          # 강남구 격자 Y
}

print("요청을 보냅니다... 잠시만 기다려주세요.")
response = requests.get(url, params=params, timeout=10)

print("\n==== 🚨 기상청이 보낸 실제 답변 원본 ====")
print(response.text)
print("=========================================\n")