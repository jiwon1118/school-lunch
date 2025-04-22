#from pyspark.sql import SparkSession
#from pyspark.sql.functions import col
import sys
import requests
import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import gcsfs
import pyarrow.dataset as ds

DATE = sys.argv[1]
year = int(DATE[:4])
month = int(DATE[4:6])
webhook_url = 'https://discordapp.com/api/webhooks/1362586291937612107/gXsqabc7FDZLsmEk23TwXINH89Q1m9zZb9pDevUEFopdePsjcyCEwiBYIIcwloSrKrnz'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ubuntu/.ssh/shining-reality-455501-q0-7b280468bf04.json"

# Spark 세션 시작
#spark = SparkSession.builder.appName('ChangeDataType').getOrCreate()

fs = gcsfs.GCSFileSystem()
gcs_path = f'school-lunch-bucket/lunch_menu/DATE_YEAR={year}/DATE_MONTH={month}'

# Parquet 파일 읽기
#df = pd.read_parquet(gcs_path, engine='pyarrow', filesystem=gcsfs.GCSFileSystem())
dataset = ds.dataset(gcs_path, filesystem=fs, format="parquet")

# 컬럼의 데이터 타입을 int32로 변경
#df = df.withColumn('DATE', col('DATE').cast('int'))
df = dataset.to_table().to_pandas()
#df['DATE'] = df['DATE'].dt.year * 10000 + df['DATE'].dt.month * 100 + df['DATE'].dt.day
#df['DATE'] = df['DATE'].astype('int32')
#df['NUT_DICT'] = df['NUT_DICT'].apply(json.dumps) 


key_map = {'단백질(g)': 'pro_g', '리보플라빈(mg)': 'riboflav_mg', '비타민A(R.E)': 'vitaA_RE', '비타민C(mg)': 'vitaC_mg', '지방(g)': 'fat_g', '철분(mg)': 'iron_mg', '칼슘(mg)': 'cal_mg', '탄수화물(g)': 'crab_g', '티아민(mg)': 'thiam_mg'}

df['NUT_DICT'] = df['NUT_DICT'].apply(lambda d: {key_map.get(k, k): v for k, v in d.items()})

if fs.exists(gcs_path):
    fs.rm(gcs_path, recursive=True)


# 변경된 DataFrame을 Parquet 파일로 다시 저장
#df.write.mode('overwrite').parquet(f'gs://school-lunch-bucket/lunch_menu/DATE_YEAR={year}/DATE_MONTH={month}/')
pq.write_to_dataset(
    table=pa.Table.from_pandas(df),
    root_path=gcs_path,
    filesystem=fs,
    existing_data_behavior='overwrite_or_ignore'
)


message = {
    "content": f"{year}{month} 데이터 변환 종료2"
    }
response = requests.post(webhook_url, data=json.dumps(message), headers={'Content-Type': 'application/json'})
print('--------------------------------------------------------------------------------')
print("1차 : 데이터 변환 성공")
print('--------------------------------------------------------------------------------')

# 세션 종료
#spark.stop()