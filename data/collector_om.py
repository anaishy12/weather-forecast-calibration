import pandas as pd
import requests
import os
from datetime import datetime, timezone, timedelta

INPUT_FILE = r"location_with_grid.csv"
OUTPUT_FILE = r"log_openmeteo_raw.csv"

KST = timezone(timedelta(hours=9))
now = datetime.now(KST)
BASE_DATE = now.strftime('%Y%m%d')
BASE_TIME = '0800' if now.hour < 12 else '1400'

CURRENT_TIME_KST = now.strftime('%Y-%m-%d %H:%M:%S')

MODELS_TO_FETCH = ['ecmwf_ifs025', 'gfs_seamless', 'ukmo_seamless', 'gem_seamless', 'kma_seamless']

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ 파일 없음: {INPUT_FILE}")
        return

    locations = pd.read_csv(INPUT_FILE)
    om_data = []

    for _, row in locations.iterrows():
        rgn = row.get('Region_en', 'Unknown')
        city = row.get('City_Gu_en', 'Unknown')
        lat, lon = row['lat'], row['lon']
        
        print(f"🔄 Open-Meteo 수집 중: {city} (Lat: {lat}, Lon: {lon})")
        
        # 모델별로 개별 요청 (하나의 모델 에러가 다른 모델 수집에 영향을 주지 않도록 격리)
        for model in MODELS_TO_FETCH:
            try:
                url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
                       f"&hourly=temperature_2m,precipitation_probability,precipitation"
                       f"&models={model}"
                       f"&timezone=Asia%2FSeoul")
                
                response = requests.get(url)
                
                # 강수확률 미지원 등 400 에러 시, 강수확률을 빼고 재요청
                if response.status_code == 400:
                    url_fallback = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
                                    f"&hourly=temperature_2m,precipitation"
                                    f"&models={model}"
                                    f"&timezone=Asia%2FSeoul")
                    response = requests.get(url_fallback)

                if response.status_code != 200:
                    print(f"   ⚠️ API 에러 [{model}]: {response.text}")
                    continue # 해당 모델만 스킵하고 다음 모델 진행
                    
                res = response.json()
                
                if 'hourly' in res:
                    hourly = res['hourly']
                    times = hourly['time']
                    
                    # 데이터 매핑 (없으면 빈칸 처리)
                    pop_data = hourly.get(f'precipitation_probability_{model}', [None] * len(times))
                    pcp_data = hourly.get(f'precipitation_{model}', [None] * len(times))
                    temp_data = hourly.get(f'temperature_2m_{model}', [None] * len(times))
                    
                    now_str = now.strftime('%Y-%m-%dT%H:00')
                    now_local = now.replace(tzinfo=None)
                    
                    added_count = 0
                    for i, target_time in enumerate(times):
                        if target_time < now_str: 
                            continue
                            
                        target_dt = datetime.strptime(target_time, '%Y-%m-%dT%H:%M')
                        if target_dt > now_local + timedelta(hours=120):
                            break

                        provider_name = model.split('_')[0].upper()
                        
                        om_data.append({
                            'Provider': provider_name,
                            'base_time': f"{BASE_DATE}{BASE_TIME}", 
                            'Rgn_en': rgn, 
                            'City_Gu_en': city,
                            'fcst_time': target_time.replace('T', ' ') + ':00',
                            'POP': pop_data[i],
                            'PCP': pcp_data[i],
                            'TEMP': temp_data[i],
                            'updated_at': CURRENT_TIME_KST
                        })
                        added_count += 1
                        
                    # print(f"   ✅ [{model}] {added_count}개의 시간대 데이터 추가됨")
                    
            except Exception as e:
                print(f"   ⚠️ 알 수 없는 에러 발생 [{model}]: {e}")

    # DataFrame 생성 및 저장
    df = pd.DataFrame(om_data)
    
    if not df.empty:
        # 파일이 비어있는(0바이트) 상태라면 새로 헤더를 써주기 위해 exists 체크 후 파일 크기까지 확인
        file_exists_and_not_empty = os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0
        
        df.to_csv(OUTPUT_FILE, mode='a', header=not file_exists_and_not_empty, index=False, encoding='utf-8-sig')
        print(f"✅ Open-Meteo 데이터 총 {len(df)}건 저장 완료! -> {OUTPUT_FILE}")
    else:
        print("⚠️ 수집되어 저장할 데이터가 0건입니다. 터미널의 에러 로그를 확인해주세요.")

if __name__ == "__main__":
    main()
