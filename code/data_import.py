import requests
import pandas as pd
from datetime import datetime, timedelta
import pyarrow
import json

with open('/home/ubuntu/code/school-lunch/temp/school_code.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data)

# 필요한 컬럼만 추출
school_df = df[['ATPT_OFCDC_SC_CODE', 'SD_SCHUL_CODE']].copy()

# 컬럼명 한글로 변경 (선택)
school_df.columns = ['지역코드', '학교코드']


# 환경설정
API_KEY = 'aab4745edc524960bacf952b293f469d' # 본인 API_KEY 입력
EDU_CODE = 'B10'  # 서울 교육청
SCH_CODE = school_df[school_df['지역코드'].str.upper() == EDU_CODE]['학교코드']
BASE_URL = 'https://open.neis.go.kr/hub/mealServiceDietInfo'
DATE = datetime(2021, 3, 1)

# 최종 DataFrame
all_df = pd.DataFrame()

# 👉 날짜 반복
ymd = DATE.strftime('%Y%m')
page = 1

for school in SCH_CODE:
    params = {
            'KEY': API_KEY,
            'Type': 'json',
            'ATPT_OFCDC_SC_CODE': EDU_CODE,
            'SD_SCHUL_CODE': school,
            'MLSV_YMD': ymd,
            'MMEAL_SC_CODE': 2,
            'pIndex': page,
            'pSize': 1000
        }

    res = requests.get(BASE_URL, params=params)
    data = res.json()

    try:
        rows = data['mealServiceDietInfo'][1]['row']
    except (KeyError, IndexError):
        pass

    df = pd.DataFrame(rows)
    all_df = pd.concat([all_df, df], ignore_index=True)

    if len(rows) < 1000:
        pass
    else:
        page += 1


    

# parquet 저장

save_path = '/home/ubuntu/code/school-lunch/temp/example.parquet'

# Parquet으로 저장 (압축 옵션 선택 가능: snappy, gzip 등)
all_df.to_parquet(save_path, index=False, engine='pyarrow', compression='snappy')
print('success')



