import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

# BigQuery 클라이언트 생성
client = bigquery.Client()

st.set_page_config(page_title="Lunch", page_icon="🍱")
st.title("학교별 급식 통계와 트렌드")
st.write(" ")
st.subheader("연도별 인기 메뉴 TOP 20")
st.write("**📊 매년 전국에서 가장 인기있는 메뉴는 무엇일지!! top 20**")

# 연도 선택 박스 (필요 시 동적으로 생성 가능)
year_options = ['2021', '2022', '2023', '2024', '2025']  # 예시: 데이터가 있는 연도만 포함
selected_year = st.selectbox("📅 연도를 선택하세요", year_options)

query = f"""
SELECT
  SUBSTR(CAST(DATE AS STRING), 1, 4) AS year,
  MENU,
  COUNT(*) AS frequency
FROM
  `shining-reality-455501-q0.school_lunch.school-lunch`
WHERE
  SUBSTR(CAST(DATE AS STRING), 1, 4) = '{selected_year}'
  AND MENU NOT LIKE '%밥%'
  AND MENU NOT LIKE '%우유%'
  AND MENU NOT LIKE '%요구르트%'
  AND MENU NOT LIKE '%김치%'
  AND MENU NOT LIKE '%깍두기%'
  AND MENU NOT LIKE '%겉절이%'
  AND MENU NOT LIKE '%석박지%'
  AND MENU NOT LIKE '%단무지%'
  AND MENU NOT LIKE '%무생채%'
  AND MENU NOT LIKE '%귤%'
  AND MENU NOT LIKE '%과일%'
  AND MENU NOT LIKE '%사과%'
  AND MENU NOT LIKE '%포도%'
  AND MENU NOT LIKE '%수박%'
  AND MENU NOT LIKE '%바나나%'
  AND MENU NOT LIKE '%파인애플%'
  AND MENU NOT LIKE '%오렌지%'
  AND MENU NOT LIKE '%골드키위%'
  AND MENU NOT LIKE '%방울토마토%'
  AND MENU NOT LIKE '%배%'
  AND MENU NOT LIKE '%딸기%'
  AND MENU NOT LIKE '%멜론%'
  AND MENU NOT LIKE '%참외%'
GROUP BY
  year, MENU
ORDER BY
  frequency DESC
LIMIT 20
"""

df = client.query(query).to_dataframe()
st.dataframe(df)


st.subheader("New Menu")
st.write("**📊 전년도에는 없던 새로 생긴 메뉴 목록**")

# 연도 선택
year_options = ['2022', '2023', '2024', '2025']  # 2021은 전년도 비교 불가
selected_year = st.selectbox("📅 연도를 선택하세요", year_options)

# 전년도 계산
prev_year = str(int(selected_year) - 1)

# 쿼리 정의
query = f"""
WITH current_year_menus AS (
    SELECT DISTINCT MENU AS menu
    FROM `shining-reality-455501-q0.school_lunch.school-lunch`
    WHERE SUBSTR(CAST(DATE AS STRING), 1, 4) = '{selected_year}'
    AND REGEXP_CONTAINS(MENU, r'^[가-힣]')  -- ✅ 한글로 시작하는 것만
),
previous_year_menus AS (
    SELECT DISTINCT MENU AS menu
    FROM `shining-reality-455501-q0.school_lunch.school-lunch`
    WHERE SUBSTR(CAST(DATE AS STRING), 1, 4) = '{prev_year}'
)
SELECT menu
FROM current_year_menus
WHERE menu NOT IN (SELECT menu FROM previous_year_menus)
LIMIT 20
"""

# 쿼리 실행
df = client.query(query).to_dataframe()
st.dataframe(df)


