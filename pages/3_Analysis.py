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