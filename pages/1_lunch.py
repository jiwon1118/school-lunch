import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Lunch", page_icon="🍱")
st.title("급식 일정 확인")
st.write("**📅 학교별로 한달 급식 일정을 확인해보세요**")


# BigQuery 클라이언트 생성
client = bigquery.Client()

# 시도교육청 리스트 예시

region_options = [
    "서울특별시교육청", "부산광역시교육청", "대구광역시교육청",
    "인천광역시교육청", "경기도교육청", "제주특별자치도교육청",
    "광주광역시교육청", "대전광역시교육청", "울산광역시교육청",
    "세종특별자치시교육청", "강원특별자치도교육청", "충청북도교육청",
    "충청남도교육청", "전라북도교육청", "전라남도교육청",
    "경상북도교육청", "경상남도교육청"
]

# UI 입력
st.subheader("🎯 검색 조건을 선택하세요")
col1, col2 = st.columns(2)
with col1:
    selected_region = st.selectbox("📍 시도교육청", region_options)
with col2:
    school_name = st.text_input("🏫 학교 이름 (예: 가락고등학교)")

# 날짜 입력 (BigQuery DATE가 int32형식으로 저장되어 있으므로 YYYYMMDD로 변환)
date_input = st.date_input("📅 날짜 선택", value=datetime.today())

# 날짜를 int32 형식으로 변환 (YYYYMMDD 형태)
date_int = int(date_input.strftime('%Y%m%d'))  # 정수로 변환

# 쿼리 정의
query = """
    SELECT REG_N, SCH_N, DATE, MENU
    FROM `shining-reality-455501-q0.school_lunch.school-lunch`
    LIMIT 50
"""

# 검색 버튼
if st.button("🔍 검색"):
    if selected_region or school_name or date_input:
        # 쿼리 기본 구조
        base_query = """
            SELECT REG_N, SCH_N, DATE, MENU
            FROM `shining-reality-455501-q0.school_lunch.school-lunch`
        """

        # 조건절 생성
        conditions = []
        if selected_region:
            conditions.append(f"REG_N = '{selected_region}'")
        if school_name:
            conditions.append(f"SCH_N LIKE '%{school_name}%'")
        if date_int:
            conditions.append(f"DATE = {date_int}")

        # 조건이 있다면 WHERE절 붙이기
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        # LIMIT 추가
        base_query += " LIMIT 50"

        # 생성된 쿼리 보여주기 (디버깅 or 확인용)
        st.code(base_query, language='sql')

        # 여기서 실제 BigQuery 호출하면 됨 (예: pandas-gbq or bigquery.Client 등)
        df = client.query(base_query).to_dataframe()
        st.dataframe(df)

    else:
       st.warning("적어도 하나의 검색 조건을 입력해주세요.")
else:
    st.subheader("기본 데이터")
    df = client.query(query).to_dataframe()
    st.dataframe(df)
