import os
import requests
import pandas as pd
import pyarrow
from school_code_jiwon import list2df, fetch_all_pages

KEY="261957623ead45779884d5b6e27385cf"
BASE_URL=f"https://open.neis.go.kr/hub/mealServiceDietInfo?KEY={KEY}&Type=json"

def call_api(region_code, dt, pIndex=1):
    # 학교 코드 데이터 가져오기
    df_sc = pd.read_json("temp/school_code.json")
    school_codes = df_sc[df_sc["ATPT_OFCDC_SC_CODE"] == region_code]["SD_SCHUL_CODE"].tolist()
    
    all_rows = []

    for school_code in school_codes:
        url = f"{BASE_URL}&pIndex={pIndex}&ATPT_OFCDC_SC_CODE={region_code}&SD_SCHUL_CODE={school_code}&MMEAL_SC_CODE=2&MLSV_YMD={dt}"
        
        try:
            res = requests.get(url).json()
            rows = res['mealServiceDietInfo'][1]['row']
            all_rows.extend(rows)
        except (KeyError, IndexError):
            continue  # 해당 학교에 데이터가 없으면 패스

    return all_rows


def list2df_menu(data: list):
    df = pd.DataFrame(data)

    # 필요한 컬럼만 유지
    keep_cols = [
        "MLSV_YMD",           # 급식 날짜
        "ATPT_OFCDC_SC_CODE", # 교육청 코드
        "ATPT_OFCDC_SC_NM",   # 교육청 이름
        "SCHUL_NM",           # 학교 이름
        "DDISH_NM",           # 급식 메뉴
        "CAL_INFO",           # 칼로리 정보
        "NTR_INFO",           # 영양소 정보
        "MLSV_FGR"            # 급식 인원 수
    ]
    df = df[keep_cols]

    # 날짜 형식 변환
    df["MLSV_YMD"] = pd.to_datetime(df["MLSV_YMD"], format="%Y%m%d")

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

    df["NUTRITION_DICT"] = df["NTR_INFO"].apply(parse_nutrition)

    # 메뉴 파싱 및 컬럼화
    def parse_menu_list(dish_text):
        try:
            return [item.strip() for item in dish_text.split("<br/>") if item.strip()]
        except:
            return []

    df["MENU_LIST"] = df["DDISH_NM"].apply(parse_menu_list)

    # # 가장 긴 메뉴 개수만큼 컬럼 분리
    # max_menu_len = df["MENU_LIST"].apply(len).max()
    # for i in range(max_menu_len):
    #     df[f"menu_{i+1}"] = df["MENU_LIST"].apply(lambda x: x[i] if i < len(x) else None)

    # 정리: 원본 텍스트 컬럼 제거
    df.drop(columns=["CAL_INFO", "NTR_INFO", "DDISH_NM", "MENU_LIST"], inplace=True)

    return df

df = list2df(call_api("B10", "202503"))
print(df.head(3))

