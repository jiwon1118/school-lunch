import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import calendar
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_info(st.secrets["google"])

# ✅ project 명시적으로 지정!
project_id = st.secrets["google"]["project_id"]

# BigQuery 클라이언트 생성
client = bigquery.Client(credentials=credentials, project=project_id)

st.set_page_config(page_title="Lunch", page_icon="🍱")
st.title("🍱 급식 영양 균형 분석 달력")
st.subheader("🧪권장섭취량을 통한 영양 불균형 탐지")
st.write(" ")

# 권장 섭취량 (한 끼 기준)
# 초등학교
recommended_E = {
    'cal_mg': 233.5, 'crab_g': 102, 'fat_g': 14, 'iron_mg': 3.27,
    'pro_g': 13.77, 'riboflav_mg': 0.27, 'thiam_mg': 0.27,
    'vitaA_RE': 167, 'vitaC_mg': 20
}
# 중학교
recommended_M = {
    'cal_mg': 300, 'crab_g': 132, 'fat_g': 19, 'iron_mg': 5,
    'pro_g': 19.2, 'riboflav_mg': 0.43, 'thiam_mg': 0.38,
    'vitaA_RE': 233.5, 'vitaC_mg': 30
}
# 고등학교
recommended_H = {
    'cal_mg': 267, 'crab_g': 137, 'fat_g': 20, 'iron_mg': 4.7,
    'pro_g': 19.75, 'riboflav_mg': 0.57, 'thiam_mg': 0.44,
    'vitaA_RE': 292, 'vitaC_mg': 33.4
}


# 🎯 사용자 입력
school_name = st.text_input("🏫 학교 이름 (예: 가락고등학교)")
year = st.selectbox("📅 연도 선택", [2021, 2022, 2023, 2024, 2025])
month = st.selectbox("📆 월 선택", list(range(1, 13)))

# 데이터 불러오기
# 날짜 범위 설정
start_date = int(f"{year}{month:02}01")
end_date = int(f"{year}{month:02}{calendar.monthrange(year, month)[1]}")

if school_name:

    # 쿼리
    query = f"""
        SELECT DATE, NUT_DICT, LV
        FROM `shining-reality-455501-q0.school_lunch.school-lunch`
        WHERE SCH_N LIKE '%{school_name}%'
        AND DATE BETWEEN {start_date} AND {end_date}
    """
    df = client.query(query).to_dataframe()
    lv = df["LV"].iloc[0]
    
    # 등급별 권장섭취량 선택 함수
    def get_recommended(lv):
        if lv == '초등학교':
            return recommended_E
        elif lv == '중학교':
            return recommended_M
        elif lv == '고등학교':
            return recommended_H
        else:
            return recommended_M  # 기본값
    
    standard = get_recommended(lv)
    
    def is_deficient(nut_dict, standard):
        deficient_nutrients = []  # 부족한 영양소를 담을 리스트
        try:
            for key, rec_val in standard.items():
                if float(nut_dict.get(key, 0)) < rec_val:
                    deficient_nutrients.append(key)  # 부족한 영양소의 key 추가
            if deficient_nutrients:
                return deficient_nutrients  # 부족한 영양소가 있으면 그 key들을 반환
            return None  # 부족한 영양소가 없으면 None 반환
        except:
            return None
        
    # 📆 달력 생성
    month_calendar = calendar.monthcalendar(year, month)

    # 📊 달력 테이블 생성
    calendar_df = pd.DataFrame(month_calendar, columns=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

    def get_display(day):
        if day == 0:
            return ""
        # 날짜를 YYYYMMDD 형식으로 변환
        date_str = int(f"{year}{month:02}{day:02}")  # 문자열이 아닌 정수로 변환
        row = df[df["DATE"] == date_str]
        if not row.empty:  # row가 비어있지 않으면
            deficient_nutrients = is_deficient(row.iloc[0]['NUT_DICT'], standard)
            if deficient_nutrients:
                # 부족한 영양소 key를 출력
                deficient_str = ', '.join(deficient_nutrients)
                return f"<div style='font-size: 13px; font-weight: bold;'>{day}</div> <div style='font-size: 12px;'>❗(부족: {deficient_str})</div>"
            else:
                return f"<div style='font-size: 13px; font-weight: bold;'>{day} ✅</div>"
        return f"{day}"  # row가 비어있는 경우 그냥 날짜 표시
    
    # 🖼️ 달력 내용 채우기
    # 달력을 pandas DataFrame으로 생성하고, 스타일을 적용하여 보여줍니다
    calendar_df = calendar_df.applymap(get_display)

    # 🪧 출력
    st.subheader(f"📅 {year}년 {month}월 영양 균형 달력")
    #st.dataframe(calendar_df, use_container_width=True)
    # HTML로 출력
    st.markdown(calendar_df.to_html(escape=False), unsafe_allow_html=True)
    

# ✅ 월 평균 부족 영양소 개수
    df["deficient_count"] = df["NUT_DICT"].apply(
    lambda x: len(is_deficient(x, standard) or []))
    avg_def = df["deficient_count"].mean()

    # 별점 평가
    if avg_def == 3.5:
        stars = "⭐⭐⭐⭐⭐ (매우 양호)"
    elif avg_def <= 5:
        stars = "⭐⭐⭐ (보통)"
    else:
        stars = "⭐ (심각)"

    st.subheader("📊 월별 평균 부족 영양소 개수 평가")
    st.markdown(f"""
    - 평균 부족 영양소 수: **{avg_def:.1f} 개**
    - 평가: **{stars}**
    """)
