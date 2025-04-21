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

DATE = sys.argv[1]
year = int(DATE[:4])
month = int(DATE[4:6])
webhook_url = 'https://discordapp.com/api/webhooks/1362586291937612107/gXsqabc7FDZLsmEk23TwXINH89Q1m9zZb9pDevUEFopdePsjcyCEwiBYIIcwloSrKrnz'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ubuntu/.ssh/shining-reality-455501-q0-7b280468bf04.json"

# Spark 세션 시작
#spark = SparkSession.builder.appName('ChangeDataType').getOrCreate()

gcs_path = f'gs://school-lunch-bucket/lunch_menu/DATE_YEAR={year}/DATE_MONTH={month}/'

# Parquet 파일 읽기
df = pd.read_parquet(gcs_path, engine='pyarrow', filesystem=gcsfs.GCSFileSystem())

# 컬럼의 데이터 타입을 int32로 변경
#df = df.withColumn('DATE', col('DATE').cast('int'))
df['DATE'] = df['DATE'].astype('int32')


# 변경된 DataFrame을 Parquet 파일로 다시 저장
#df.write.mode('overwrite').parquet(f'gs://school-lunch-bucket/lunch_menu/DATE_YEAR={year}/DATE_MONTH={month}/')
pq.write_to_dataset(
    table=pa.Table.from_pandas(df),
    root_path=gcs_path,
    filesystem=gcsfs.GCSFileSystem(),
    existing_data_behavior='overwrite_or_ignore'
)


message = {
    "content": f"{year}{month} 데이터 변환 종료"
    }
response = requests.post(webhook_url, data=json.dumps(message), headers={'Content-Type': 'application/json'})
print('--------------------------------------------------------------------------------')
print("1차 : 데이터 변환 성공")
print('--------------------------------------------------------------------------------')

# 세션 종료
#spark.stop()