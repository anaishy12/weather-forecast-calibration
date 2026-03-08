import pandas as pd
import requests
import os
from datetime import datetime, timezone, timedelta

INPUT_FILE = r"C:\Users\phy96\Desktop\my-capstone-project\data\location_with_grid.csv"
OUTPUT_FILE = r"C:\Users\phy96\Desktop\my-capstone-project\data\log_openmeteo.csv"

KST = timezone(timedelta(hours=9))
now = datetime.now(KST)
BASE_DATE = now.strftime('%Y%m%d')
BASE_TIME = '0800' if now.hour < 12 else '1400'

# 수정포인트 1: ecmwf_ifs04가 최근 Open-Meteo에서 ecmwf_ifs025로 업데이트됨에 따라 모델명 변경
MODELS_TO_FETCH = ['ecmwf_ifs025', 'gfs_seamless', 'ukmo_seamless', 'gem_seamless', 'kma_seamless']

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ 파일 없음: {INPUT_FILE}")
        return

    locations = pd.read_csv(INPUT_FILE)
    om_data = []

    # API에 요청할 모델 리스트를 문자열로 변환 (URL 파라미터용)
    models_str = ",".join(MODELS_TO_FETCH)

    for _, row in locations.iterrows():
        rgn = row.get('Region_en', 'Unknown')
        city = row.get('City_Gu_en', 'Unknown')
        lat, lon = row['lat'], row['lon']
        
        print(f"🔄 Open-Meteo 수집 중: {city}")
        
        try:
            url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
                   f"&hourly=temperature_2m,precipitation_probability,precipitation"
                   f"&models={models_str}"
                   f"&timezone=Asia%2FSeoul")
            
            # response.raise_for_status()를 추가하여 HTTP 에러(예: 400, 500) 발생 시 빠르게 예외 처리
            response = requests.get(url)
            response.raise_for_status() 
            res = response.json()
            
            if 'hourly' in res:
                hourly = res['hourly']
                times = hourly['time']
                
                # 수정포인트 2: API 응답에 특정 모델 데이터가 누락되었을 때 발생하는 IndexError 방지
                # 만약 키가 없다면 [None, None, ...] 형태의 빈 리스트를 반환하도록 안전하게 처리
                model_data = {}
                for model in MODELS_TO_FETCH:
                    model_data[model] = {
                        'pop': hourly.get(f'precipitation_probability_{model}', [None] * len(times)),
                        'pcp': hourly.get(f'precipitation_{model}', [None] * len(times)),
                        'temp': hourly.get(f'temperature_2m_{model}', [None] * len(times))
                    }
                
                now_str = now.strftime('%Y-%m-%dT%H:00')
                now_local = now.replace(tzinfo=None) # 시간 비교를 위해 타임존 정보 제거
                
                for i, target_time in enumerate(times):
                    # 현재 시간 이전 데이터는 스킵
                    if target_time < now_str: 
                        continue
                        
                    # 현재 시간 기준 120시간(5일)을 초과하는 데이터는 스킵 (정확한 시간 계산)
                    target_dt = datetime.strptime(target_time, '%Y-%m-%dT%H:%M')
                    if target_dt > now_local + timedelta(hours=120):
                        break

                    for model in MODELS_TO_FETCH:
                        # 수정포인트 2: Provider 이름을 깔끔하게 앞단어만 추출 (ECMWF, GFS, KMA 등)
                        provider_name = model.split('_')[0].upper()
                        
                        om_data.append({
                            'Provider': provider_name,
                            'base_time': f"{BASE_DATE}{BASE_TIME}", 
                            'Rgn_en': rgn, 
                            'City_Gu_en': city,
                            'fcst_time': target_time.replace('T', ' ') + ':00',
                            'POP': model_data[model]['pop'][i],
                            'PCP': model_data[model]['pcp'][i],
                            'TEMP': model_data[model]['temp'][i]
                        })
        except Exception as e:
            print(f"⚠️ 에러 발생 ({city}): {e}")

    df = pd.DataFrame(om_data)
    if not df.empty:
        df.to_csv(OUTPUT_FILE, mode='a', header=not os.path.exists(OUTPUT_FILE), index=False, encoding='utf-8-sig')
        print(f"✅ Open-Meteo 데이터 저장 완료! -> {OUTPUT_FILE}")
    else:
        print("⚠️ 수집되어 저장할 데이터가 없습니다.")

if __name__ == "__main__":
    main()