import os
import requests
import pandas as pd

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

def list2df(data: list):
    df = pd.DataFrame(data)

    # 필요한 컬럼만 유지
    keep_cols = [
        "ATPT_OFCDC_SC_CODE", # 교육청 코드
        "SD_SCHUL_CODE",      # 학교 코드
        "SCHUL_KND_SC_NM"     # 학교 종류 명
    ]
    df = df[keep_cols]
    
    output_path = "~/code/school-lunch/temp/school_code.json"
    # JSON 파일로 저장 (indent로 예쁘게, orient는 records 형식)
    df.to_json(output_path, orient="records", force_ascii=False, indent=2)
    return df

df = list2df(fetch_all_pages())
print(df.head())
