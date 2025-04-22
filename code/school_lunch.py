import requests
import pandas as pd
from datetime import datetime, timedelta
import pyarrow
import json
import re
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import sys

webhook_url = 'https://discordapp.com/api/webhooks/1362586291937612107/gXsqabc7FDZLsmEk23TwXINH89Q1m9zZb9pDevUEFopdePsjcyCEwiBYIIcwloSrKrnz'
DATE = sys.argv[1]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ubuntu/.ssh/shining-reality-455501-q0-7b280468bf04.json"

from google.cloud import storage


# gcs에서 학교 코드 json 파일을 읽어오기
def load_json_from_gcs(bucket_name: str, blob_name: str):
    client = storage.Client()
    print(f"Downloading {blob_name} from {bucket_name}")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    json_str = blob.download_as_text(encoding='utf-8')
    print("Download complete")
    data = json.loads(json_str)
    df = pd.DataFrame(data)
    return df

# api에서 데이터를 가져오기
def get_api():
    df = load_json_from_gcs("school-lunch-bucket", "lunch_menu/school_data.json")
    # 필요한 컬럼만 추출
    school_df = df[['ATPT_OFCDC_SC_CODE', 'SD_SCHUL_CODE', 'SCHUL_KND_SC_NM']].copy()
    # 컬럼명 한글로 변경 (선택)
    school_df.columns = ['지역코드', '학교코드', '학교구분']
    
    # SparkSession 생성
    spark = SparkSession.builder.appName("school-lunch").getOrCreate()
    
    # 환경설정
    API_KEY = '261957623ead45779884d5b6e27385cf' # 본인 API_KEY 입력
    EDU_CODE = school_df['지역코드'].unique()
    SCH_CODE = []
    BASE_URL = 'https://open.neis.go.kr/hub/mealServiceDietInfo'

    # 최종 DataFrame
    all_df = pd.DataFrame()

    # 👉 날짜 반복
    ymd = DATE
    page = 1
    
    url_list = []
    for reg in EDU_CODE:
        SCH_CODE = school_df[school_df['지역코드'].str.upper() == reg]['학교코드']
        for school in SCH_CODE:
            url_list.append(f'https://open.neis.go.kr/hub/mealServiceDietInfo?KEY={API_KEY}&Type=json&ATPT_OFCDC_SC_CODE={reg}&SD_SCHUL_CODE={school}&MLSV_YMD={ymd}&MMEAL_SC_CODE=2&pIndex={page}&pSize=1000')
    
    urls = spark.sparkContext.parallelize(url_list, numSlices=20)
    print(f"총 URL 수: {len(url_list)}")
    
    rdd = urls.flatMap(fetch_json)
    all_df = spark.createDataFrame(rdd)
    all_df = all_df.toPandas()
        
    # 학교 구분 추가
    school_merge_df = school_df[['학교코드', '학교구분']]

    # 병합
    all_df = all_df.merge(school_merge_df, how='left', left_on='SD_SCHUL_CODE', right_on='학교코드')
    # 'LV' 컬럼으로 이름 변경
    all_df.rename(columns={'학교구분': 'LV'}, inplace=True)
    
    message = {
    "content": f"전국 {ymd} 데이터 추출 성공"
    }
    response = requests.post(webhook_url, data=json.dumps(message), headers={'Content-Type': 'application/json'})
    print('--------------------------------------------------------------------------------')
    print("1차 : 데이터 처리 성공")
    print('--------------------------------------------------------------------------------')
    
    return all_df

# SPARK처리용 함수
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


def pre_parquet(df):

    # 필요한 컬럼만 유지
    keep_cols = [
        "MLSV_YMD",           # 급식 날짜
        "ATPT_OFCDC_SC_CODE", # 교육청 코드
        "ATPT_OFCDC_SC_NM",   # 교육청 이름
        "SD_SCHUL_CODE",      # 학교 코드
        "SCHUL_NM",           # 학교 이름
        "DDISH_NM",           # 급식 메뉴
        "CAL_INFO",           # 칼로리 정보
        "NTR_INFO",           # 영양소 정보
        "MLSV_FGR",           # 급식 인원 수
        "LV"                  # 학교 종류
    ]
    df = df[keep_cols]
    
    # 날짜 형식 변환
    df["DATE"] = pd.to_datetime(df["MLSV_YMD"], format="%Y%m%d")
    
    df = df.rename(columns={
        "ATPT_OFCDC_SC_CODE": "REG_C",  # 지역 코드
        "ATPT_OFCDC_SC_NM": "REG_N",    # 교육청 이름
        "SD_SCHUL_CODE": "SCH_C",       # 학교 코드
        "SCHUL_NM": "SCH_N",            # 학교 이름
        "MLSV_FGR": "COUNT",            # 급식 인원 수
        })


    # 칼로리 수치 추출
    df["CAL"] = df["CAL_INFO"].str.extract(r"([\d.]+)").astype(float)

    # 영양정보 dict 변환
    def parse_nutrition(info):
        try:
            parts = info.split("<br/>")
            return {
                kv.split(":")[0].strip(): kv.split(":")[1].strip()
                for kv in parts if ":" in kv
            }
        except:
            return {}

    df["NUT_DICT"] = df["NTR_INFO"].apply(parse_nutrition)

    # 메뉴 파싱 및 컬럼화
    def parse_menu_list(dish_text):
        try:
            return [item.strip() for item in dish_text.split("<br/>") if item.strip()]
        except:
            return []
        
    def clean_menu(menu_item):
        # 한글/숫자만 처음부터 끝까지 추출 (영문자나 특수문자 나오기 전까지만)
        match = re.match(r'^[가-힣0-9]+', menu_item)
        return match.group(0) if match else menu_item
    
    def remove_trailing_digits(menu_item):
        # 뒤에 붙은 숫자만 제거 (중간 숫자는 유지)
        return re.sub(r'\d+$', '', menu_item)
    
    df["MENU_LIST"] = df["DDISH_NM"].apply(parse_menu_list)
    df["MENU"] = df["MENU_LIST"].apply(lambda menus: [clean_menu(m) for m in menus])
    df["MENU"] = df["MENU"].apply(lambda menus: [remove_trailing_digits(m) for m in menus])
    df["MENU"] = df["MENU"].apply(list)
    rdf = df.explode("MENU").reset_index(drop=True)

    # 정리: 원본 텍스트 컬럼 제거
    rdf.drop(columns=["MLSV_YMD", "CAL_INFO", "NTR_INFO", "DDISH_NM", "MENU_LIST"], inplace=True)
    print('--------------------------------------------------------------------------------')
    print("2차 : 데이터 전처리 성공")
    print('--------------------------------------------------------------------------------')
    return rdf


def upload_partitioned_parquet_to_gcs(df, bucket_name, base_path):
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['DATE_YEAR'] = df['DATE'].dt.year.astype('int64')
    df['DATE_MONTH'] = df['DATE'].dt.month.astype('int64')

    # 임시 디렉토리에 partition 저장
    with tempfile.TemporaryDirectory() as tmp_dir:
        df.to_parquet(tmp_dir, engine='pyarrow', index=False, partition_cols=['DATE_YEAR', 'DATE_MONTH'])

        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # 폴더 내 모든 파일을 GCS로 업로드
        for root, _, files in os.walk(tmp_dir):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, tmp_dir)
                blob_path = os.path.join(base_path, relative_path).replace("\\", "/")  # Windows 경로 대응
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(local_path)
                print(f"✅ Uploaded to gs://{bucket_name}/{blob_path}")
    
    print('--------------------------------------------------------------------------------')
    print("3차 : 데이터 업로드 성공")
    print('--------------------------------------------------------------------------------')

# 메인 실행
df = get_api()
rdf = pre_parquet(df)
upload_partitioned_parquet_to_gcs(rdf, 'school-lunch-bucket', 'lunch_menu')
