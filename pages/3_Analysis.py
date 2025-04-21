import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Lunch", page_icon="🍱")
st.title("영양 불균형 탐지 및 급식 품질 분석")
st.write("**🧪권장섭취량을 통한 영양 불균형 탐지와 메뉴 다양성을 바탕으로 한 급식 품질 평가 분석**")
# BigQuery 클라이언트 생성
client = bigquery.Client()

# 쿼리 정의
query = """
    SELECT *
    FROM `shining-reality-455501-q0.school_lunch.school-lunch`
    LIMIT 50
"""
df = client.query(query).to_dataframe()
st.dataframe(df)

# GCP 프로젝트 ID
PROJECT_ID = "shining-reality-455501-q0"

# BigQuery 클라이언트 생성
client = bigquery.Client()

# 쿼리문
query = """
WITH daily_menu AS (
  SELECT LV, SCH_N, DATE, COUNT(*) AS menu_count
  FROM `shining-reality-455501-q0.school_lunch.school-lunch`
  WHERE LV IN ('초등학교', '중학교', '고등학교')
  GROUP BY LV, SCH_N, DATE
)

SELECT LV, ROUND(AVG(menu_count), 1) AS avg_daily_menu
FROM daily_menu
GROUP BY LV
ORDER BY LV
"""

# 쿼리 실행
df = client.query(query).to_dataframe()

# Streamlit UI
st.title("학교급별 하루 평균 메뉴 수")

st.dataframe(df)



query = """
WITH daily_menu AS (
  SELECT 
    REG_C,
    LV,
    SCH_N,
    EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', CAST(DATE AS STRING))) AS DATE_YEAR,
    DATE,
    COUNT(*) AS menu_count
  FROM `shining-reality-455501-q0.school_lunch.school-lunch`
  WHERE LV IN ('초등학교', '중학교', '고등학교')
  GROUP BY REG_C, LV, SCH_N, DATE, DATE_YEAR
),

school_avg_daily AS (
  SELECT 
    REG_C,
    LV,
    DATE_YEAR,
    SCH_N,
    ROUND(AVG(menu_count), 1) AS school_avg_menu
  FROM daily_menu
  GROUP BY REG_C, LV, DATE_YEAR, SCH_N
)

SELECT 
  REG_C,
  LV,
  DATE_YEAR,
  ROUND(AVG(school_avg_menu), 1) AS avg_daily_menu
FROM school_avg_daily
GROUP BY REG_C, LV, DATE_YEAR
ORDER BY REG_C, LV, DATE_YEAR;
"""

# 쿼리 실행 → pandas DataFrame
df = client.query(query).to_dataframe()

# UI 구성
st.title("📊 지역별·학교급별 하루 평균 메뉴 수")

# 연도 선택
years = sorted(df["DATE_YEAR"].unique())
selected_year = st.selectbox("📅 연도를 선택하세요", years, index=len(years)-1)

# 필터 적용
filtered_df = df[df["DATE_YEAR"] == selected_year]

# 피벗 테이블: 지역 vs 학교급
pivot_df = filtered_df.pivot(index="REG_C", columns="LV", values="avg_daily_menu").fillna(0)

st.write(f"### {selected_year}년 지역별 하루 평균 메뉴 수 (학교급별)")

# 테이블 출력
st.dataframe(pivot_df.style.format("{:.1f}"))

# 바 차트 시각화 (학교급별)
st.bar_chart(pivot_df)