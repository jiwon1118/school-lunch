import requests
import pandas as pd
from datetime import datetime, timedelta
import pyarrow
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# SparkSession 생성
spark = SparkSession.builder.appName("school-lunch").getOrCreate()

with open('/home/ubuntu/code/school-lunch/temp/school_code.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data)

# 필요한 컬럼만 추출
school_df = df[['ATPT_OFCDC_SC_CODE', 'SD_SCHUL_CODE', 'SCHUL_KND_SC_NM']].copy()

# 컬럼명 한글로 변경 (선택)
school_df.columns = ['지역코드', '학교코드', '학교구분']


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

# schema = StructType([
#     StructField("ATPT_OFCDC_SC_CODE", StringType(), True),
#     StructField("ATPT_OFCDC_SC_NM", StringType(), True),
#     StructField("SD_SCHUL_CODE", StringType(), True),
#     StructField("SCHUL_NM", StringType(), True),
#     StructField("MMEAL_SC_CODE", StringType(), True),
#     StructField("MMEAL_SC_NM", StringType(), True),
#     StructField("MLSV_YMD", StringType(), True),
#     StructField("MLSV_FGR", FloatType(), True),
#     StructField("DDISH_NM", StringType(), True),
#     StructField("ORPLC_INFO", StringType(), True),
#     StructField("CAL_INFO", StringType(), True),
#     StructField("NTR_INFO", StringType(), True),
#     StructField("LOAD_DTM", StringType(), True),
# ])

# NEEDED_FIELDS = [
#     "ATPT_OFCDC_SC_CODE", "ATPT_OFCDC_SC_NM", "SD_SCHUL_CODE", "SCHUL_NM",
#     "MMEAL_SC_CODE", "MMEAL_SC_NM", "MLSV_YMD", "MLSV_FGR",
#     "DDISH_NM", "ORPLC_INFO", "CAL_INFO", "NTR_INFO", "LOAD_DTM"
# ]

url_list = []
for school in SCH_CODE:
    url_list.append(f'https://open.neis.go.kr/hub/mealServiceDietInfo?KEY={API_KEY}&Type=json&ATPT_OFCDC_SC_CODE={EDU_CODE}&SD_SCHUL_CODE={school}&MLSV_YMD={ymd}&MMEAL_SC_CODE=2&pIndex={page}&pSize=1000')
    
urls = spark.sparkContext.parallelize(url_list, numSlices=20)

def fetch_json(url):
    try:
        res = requests.get(url)
        res.raise_for_status()  # HTTP 에러 코드 체크 (4xx, 5xx)
        data = res.json()
        if "mealServiceDietInfo" in data and len(data["mealServiceDietInfo"]) > 1:
            return data["mealServiceDietInfo"][1]["row"]  # ✅ 핵심 데이터 반환
        else:
            return []  # 데이터가 없을 경우 빈 리스트
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return []

rdd = urls.flatMap(fetch_json)
all_df = spark.createDataFrame(rdd)
all_df = all_df.toPandas()

# for school in SCH_CODE:
#     params = {
#             'KEY': API_KEY,
#             'Type': 'json',
#             'ATPT_OFCDC_SC_CODE': EDU_CODE,
#             'SD_SCHUL_CODE': school,
#             'MLSV_YMD': ymd,
#             'MMEAL_SC_CODE': 2,
#             'pIndex': page,
#             'pSize': 1000
#         }

#     res = requests.get(BASE_URL, params=params)
#     data = res.json()

#     try:
#         rows = data['mealServiceDietInfo'][1]['row']
#     except (KeyError, IndexError):
#         pass

#     df = pd.DataFrame(rows)
#     all_df = pd.concat([all_df, df], ignore_index=True)

#     if len(rows) < 1000:
#         pass
#     else:
#         page += 1

print(all_df)

# 학교 구분 추가
school_merge_df = school_df[['학교코드', '학교구분']]

# 병합
all_df = all_df.merge(school_merge_df, how='left', left_on='SD_SCHUL_CODE', right_on='학교코드')
# 'LV' 컬럼으로 이름 변경
all_df.rename(columns={'학교구분': 'LV'}, inplace=True)


# parquet 저장
save_path = '/home/ubuntu/code/school-lunch/temp/example.parquet'

# Parquet으로 저장 (압축 옵션 선택 가능: snappy, gzip 등)
all_df.to_parquet(save_path, index=False, engine='pyarrow', compression='snappy')
print('success')



