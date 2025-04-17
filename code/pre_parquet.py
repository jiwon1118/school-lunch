import os
import requests
import pandas as pd
import re
import pyarrow

df = pd.read_parquet("~/code/school-lunch/temp/example.parquet")
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
    
    rdf.to_parquet("~/code/school-lunch/temp/test.parquet", index=False)
    return rdf

pre_parquet(df)
