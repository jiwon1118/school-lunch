import requests
import pandas as pd
from google.cloud import storage

KEY="261957623ead45779884d5b6e27385cf"
URL=f"https://open.neis.go.kr/hub/schoolInfo?KEY={KEY}&Type=json"


def fetch_all_pages(pSize=1000):

    all_rows = []
    page = 1

    while True:
        url = f"{URL}&pIndex={page}&pSize={pSize}"
        res = requests.get(url).json()

        try:
            rows = res['schoolInfo'][1]['row']
            all_rows.extend(rows)
            page += 1
        except (KeyError, IndexError):
            break  # 더 이상 데이터 없음

    return all_rows

def upload_json_to_gcs(df, bucket_name, blob_name):
    # JSON 문자열로 변환
    json_data = df.to_json(orient="records", force_ascii=False, indent=2)

    # GCS 클라이언트
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # 업로드
    blob.upload_from_string(json_data, content_type="application/json")
    print(f"✅ Uploaded to gs://{bucket_name}/{blob_name}")
    
def list2df(data: list):
    df = pd.DataFrame(data)

    # 필요한 컬럼만 유지
    keep_cols = [
        "ATPT_OFCDC_SC_CODE", # 교육청 코드
        "SD_SCHUL_CODE",      # 학교 코드
        "SCHUL_KND_SC_NM"     # 학교 종류 명
    ]
    df = df[keep_cols]
   
    # GCS에 업로드
    upload_json_to_gcs(df, "school-lunch-bucket", "lunch_menu/school_data.json")
    return df


list2df(fetch_all_pages())
