import streamlit as st
import gcsfs 
import pandas as pd
import altair as alt
import re


### 사용하지 않은 코드를 제외하고 st.write로 작성된 파일관련 메세지, 데이터로드 메세지, 데이터 미리보기 부분을 생략해서 새로운 파일로 작성였으므로 ###
### 이 파일을 사용하지 않음 ###

# from datetime import datetime
# from google.cloud import bigquery
# from google.cloud import storage
# import os 

st.set_page_config(page_title="Lunch", page_icon="🍱")
st.title("영양 불균형 탐지 및 급식 품질 분석")
st.write("**🧪권장섭취량을 통한 영양 불균형 탐지와 메뉴 다양성을 바탕으로 한 급식 품질 평가 분석**")



# # BigQuery 클라이언트 생성
# client = bigquery.Client()

# # 쿼리 정의
# query = """
#     SELECT *
#     FROM `shining-reality-455501-q0.school_lunch.school-lunch`
#     LIMIT 50
# """
# df = client.query(query).to_dataframe()
# st.dataframe(df)

import streamlit as st
from google.cloud import storage
import pandas as pd
import altair as alt
import gcsfs # GCS 경로 사용을 위해 필요

# --- 설정 ---
# 분석할 GCS CSV 파일의 URI를 지정하세요.
GCS_URI = 'gs://school-lunch-bucket/lunch_menu/analysis_data_csv/student_by_class_and_school.csv' # <-- 실제 파일 경로로 변경해야 합니다.

# 고정된 시도교육청 컬럼 이름
PROVINCE_COLUMN_NAME = '시도교육청' # <-- 데이터 파일의 실제 시도교육청 컬럼 이름으로 변경해야 합니다.

# 고정된 연도 컬럼 이름
YEAR_COLUMN_NAME = '연도' # <-- 데이터 파일의 실제 연도 컬럼 이름으로 변경해야 합니다.

# 고정된 학급구분 컬럼 이름
CLASS_TYPE_COLUMN_NAME = '학급구분' # <-- 실제 학급구분 컬럼 이름으로 변경해야 합니다.

# 사용자가 선택 가능한 연도 리스트 (필터링 기준)
SELECTABLE_YEARS = [2021, 2022, 2023, 2024]

# Y축으로 시각화할 변수 컬럼 이름 리스트 (이 목록 안에서만 선택 가능)
# 이 부분을 새 CSV 파일에 있는 실제 변수 컬럼 이름들로 채워야 합니다.
Y_AXIS_VARIABLES = [
    '계 학생수', # <-- 실제 변수 컬럼 이름 1
    #'교원수', # <-- 실제 변수 컬럼 이름 2
    #'급식비', # <-- 실제 변수 컬럼 이름 3
    # ... 다른 변수 컬럼 이름들을 여기에 추가 ...
]

# 데이터에 실제로 존재하는 개별 학급구분 타입 리스트 (전체 합계 계산에 사용)
# 데이터에 '초등학교', '중학교', '고등학교' 외에 다른 개별 타입이 있다면 추가해야 합니다.
SPECIFIC_CLASS_TYPES = ['초등학교', '중학교', '고등학교'] # <-- 실제 데이터에 있는 값들로 변경

# 사용자가 학급구분 드롭다운에서 선택 가능한 옵션 리스트 ('전체' 포함)
CLASS_TYPE_OPTIONS = ['전체'] + SPECIFIC_CLASS_TYPES


# 앱 제목 설정
st.title(f"{PROVINCE_COLUMN_NAME}별 특정 연도 데이터 시각화") # 제목 간소화

# st.write(f"데이터 소스: {GCS_URI}")
# st.write(f"'{PROVINCE_COLUMN_NAME}' 컬럼 기준, '{YEAR_COLUMN_NAME}' 컬럼의 {SELECTABLE_YEARS}년 데이터 시각화")
# st.write(f"Y축 변수: {Y_AXIS_VARIABLES}")
# st.write(f"학급구분: {CLASS_TYPE_OPTIONS} 중 선택 가능 (전체는 {SPECIFIC_CLASS_TYPES}의 합계)")
# st.write(f"합계 계산에는 데이터의 '{CLASS_TYPE_COLUMN_NAME}' 컬럼 내 값 중 {SPECIFIC_CLASS_TYPES} 목록에 해당하는 행들만 사용됩니다.")
# st.write("GCS 접근 및 'gcsfs' 라이브러리 필요.")


# --- 데이터 로드 ---
# df = None
# try:
#     st.info(f"GCS에서 파일 '{GCS_URI}' 읽는 중...")
#     df = pd.read_csv(GCS_URI)
#     st.success(f"파일 '{GCS_URI}'을 성공적으로 읽었습니다.")

# except FileNotFoundError:
#      st.error(f"오류: 지정된 GCS 경로에 파일이 없거나 접근할 수 없습니다: '{GCS_URI}'")
# except Exception as e:
#     st.error(f"파일 로드 중 오류 발생: {e}")
#     st.error("gcsfs 라이브러리 설치 및 GCS 접근 권한/경로를 확인해주세요.")


# --- 데이터 전처리 및 시각화 (파일 로드 성공 시만 실행) ---
# if df is not None:
df = pd.read_csv(GCS_URI)
# 필수 컬럼 존재 체크 (Y축 변수들도 포함)
required_cols_check = [PROVINCE_COLUMN_NAME, YEAR_COLUMN_NAME, CLASS_TYPE_COLUMN_NAME] + Y_AXIS_VARIABLES
missing_required_cols = [col for col in required_cols_check if col not in df.columns]

# 데이터 내 개별 학급구분 타입 존재 체크 (전체 합계 계산을 위해 필요)
# 실제 데이터에 SPECIFIC_CLASS_TYPES에 있는 값들이 모두 있는지 확인
present_specific_types_in_data = [ct for ct in SPECIFIC_CLASS_TYPES if ct in df[CLASS_TYPE_COLUMN_NAME].unique()]
missing_specific_types_in_data = [ct for ct in SPECIFIC_CLASS_TYPES if ct not in df[CLASS_TYPE_COLUMN_NAME].unique()]


if missing_required_cols:
        st.error(f"오류: CSV 파일에 필수 컬럼이 없습니다: {missing_required_cols}. 설정(PROVINCE_COLUMN_NAME, YEAR_COLUMN_NAME, CLASS_TYPE_COLUMN_NAME, Y_AXIS_VARIABLES)을 확인해주세요.")
elif missing_specific_types_in_data:
        st.error(f"오류: '{CLASS_TYPE_COLUMN_NAME}' 컬럼에 전체 합계 계산에 필요한 타입({SPECIFIC_CLASS_TYPES}) 중 일부({missing_specific_types_in_data})가 없습니다. 데이터 내용을 확인해주세요.")
else:
    # # --- 데이터 미리보기 ---
    # st.write("### 데이터 미리보기")
    # st.dataframe(df.head())

    st.write("### 시각화 설정")

    # --- Y축으로 사용할 변수 선택 ---
    selected_variable_name = st.selectbox("Y축 변수 선택", sorted(Y_AXIS_VARIABLES))

    # --- 데이터에 실제로 있는 연도 확인 및 특정 연도 선택 ---
    available_years_in_data = sorted(df[YEAR_COLUMN_NAME].unique().tolist())
    common_years = sorted(list(set(available_years_in_data) & set(SELECTABLE_YEARS)))

    if not common_years:
            st.warning(f"지정된 연도({SELECTABLE_YEARS}) 중 데이터에 실제로 존재하는 연도가 없습니다. 데이터의 '{YEAR_COLUMN_NAME}' 컬럼 값을 확인해주세요.")
    else:
            selected_plot_year = st.selectbox("데이터를 볼 연도를 선택해주세요", sorted([str(y) for y in common_years]))

            # --- 학급구분 선택 ---
            st.write(f"시각화할 '{CLASS_TYPE_COLUMN_NAME}'을(를) 선택해주세요.")
            selected_class_types = st.multiselect(
                f"{CLASS_TYPE_COLUMN_NAME} 선택",
                CLASS_TYPE_OPTIONS,
                default=['전체'] # 기본값을 '전체'로 설정
            )

            if not selected_class_types:
                st.warning(f"'{CLASS_TYPE_COLUMN_NAME}'을(를) 하나 이상 선택해주세요.")
            else:
                # --- 데이터 필터링 및 준비 ---
                try:
                    # 1. 선택된 연도로 필터링 및 필요한 컬럼만 선택
                    cols_to_select_and_rename = [PROVINCE_COLUMN_NAME, CLASS_TYPE_COLUMN_NAME, selected_variable_name]
                    df_year_filtered = df[df[YEAR_COLUMN_NAME].astype(str) == selected_plot_year][cols_to_select_and_rename].copy()

                    # 2. 값 컬럼 이름 통일 및 숫자 변환, 기본 NaN 제거
                    df_process = df_year_filtered.copy()
                    df_process.rename(columns={selected_variable_name: '값'}, inplace=True)
                    df_process['값'] = pd.to_numeric(df_process['값'], errors='coerce')
                    df_process.dropna(subset=['값', PROVINCE_COLUMN_NAME], inplace=True) # 값 또는 시도교육청 없는 행 제거


                    # '전체' 값 계산 및 시각화 데이터프레임 준비
                    df_to_plot = pd.DataFrame() # 최종 시각화 데이터프레임을 빈 것으로 시작

                    # 사용자가 '전체'를 선택했고, 합계 계산 대상 데이터 (SPECIFIC_CLASS_TYPES)가 df_process에 있는 경우
                    specific_types_present_for_summation = [
                        item for item in SPECIFIC_CLASS_TYPES
                        if item in df_process[CLASS_TYPE_COLUMN_NAME].unique()
                    ]
                    if '전체' in selected_class_types and specific_types_present_for_summation:
                        # df_process에서 합산 대상 개별 타입들만 필터링
                        df_specific_types_only = df_process[df_process[CLASS_TYPE_COLUMN_NAME].isin(specific_types_present_for_summation)].copy()

                        if not df_specific_types_only.empty: # 합계 계산 대상 데이터가 있는 경우만 진행
                            df_total = df_specific_types_only.groupby(PROVINCE_COLUMN_NAME)['값'].sum().reset_index()
                            df_total[CLASS_TYPE_COLUMN_NAME] = '전체' # 학급구분 컬럼 추가 및 값 설정
                            # 필요한 경우 다른 컬럼 추가 (예: 연도) 가능하나, 여기서는 시각화에 불필요
                            df_to_plot = pd.concat([df_to_plot, df_total], ignore_index=True)
                        # else: selected year/variable has no data for SPECIFIC_CLASS_TYPES, cannot calculate total (warning happens below)


                    # 사용자가 선택한 개별 학급구분 타입 데이터 추가
                    # selected_class_types 중 SPECIFIC_CLASS_TYPES에 실제로 있고, df_process에도 있는 것들만 필터링 대상
                    selected_specific_types_to_add = [
                        ct for ct in selected_class_types
                        if ct in present_specific_types_in_data # 실제 데이터(df_process)에도 있어야 함
                    ]

                    if selected_specific_types_to_add: # 추가할 개별 타입이 있는 경우만 진행
                        # df_process에서 이 유효한 개별 타입들에 해당하는 행들만 필터링
                        df_specific_selected = df_process[df_process[CLASS_TYPE_COLUMN_NAME].isin(selected_specific_types_to_add)].copy()

                        if not df_specific_selected.empty: # 필터링된 개별 타입 데이터가 있는 경우만 합치기
                            df_to_plot = pd.concat([df_to_plot, df_specific_selected], ignore_index=True)
                        # else: 데이터가 없어 합치지 않음 (경고는 이미 위에서 처리)


                    # 최종 시각화 데이터프레임 정리: 학급구분 또는 시도교육청 누락 행 제거 및 중복 제거
                    # (간혹 합치기 과정에서 예상치 못한 None/NaN 또는 중복 발생 가능)
                    df_plot = df_to_plot.dropna(subset=[PROVINCE_COLUMN_NAME, CLASS_TYPE_COLUMN_NAME]).copy()
                    # 중복 제거 시, 시도교육청과 학급구분 조합이 같으면 중복으로 간주 (같은 막대가 두 번 그려지는 것 방지)
                    df_plot = df_plot.drop_duplicates(subset=[PROVINCE_COLUMN_NAME, CLASS_TYPE_COLUMN_NAME]).copy()


                    # --- 시각화 (막대 그래프) ---
                    if not df_plot.empty:
                        st.write(f"### {selected_plot_year}년 {selected_variable_name} ({PROVINCE_COLUMN_NAME}별 - {', '.join(selected_class_types)})")

                        # --- 정렬 옵션 체크박스 ---
                        sort_by_value_checkbox = st.checkbox("Y축 값 (내림차순)으로 정렬", value=True)

                        if sort_by_value_checkbox:
                            sort_param = '-y'
                        else:
                            sort_param = 'ascending' # 가나다순 정렬

                        # 시도교육청 기준으로 정렬 적용 및 학급구분으로 색상/그룹화
                        chart = alt.Chart(df_plot).mark_bar().encode(
                            x=alt.X(PROVINCE_COLUMN_NAME, sort=sort_param, title=PROVINCE_COLUMN_NAME),
                            y=alt.Y('값', type='quantitative', title=selected_variable_name),
                            color=alt.Color(CLASS_TYPE_COLUMN_NAME, title=CLASS_TYPE_COLUMN_NAME), # 학급구분별 색상
                            xOffset=alt.XOffset(CLASS_TYPE_COLUMN_NAME, title=CLASS_TYPE_COLUMN_NAME), # 학급구분별 그룹화
                            tooltip=[PROVINCE_COLUMN_NAME, CLASS_TYPE_COLUMN_NAME, alt.Tooltip('값', title=selected_variable_name)]
                        ).properties(
                            title=f'{selected_plot_year}년 {selected_variable_name} by {PROVINCE_COLUMN_NAME} ({", ".join(selected_class_types)})'
                        ).interactive()

                        st.altair_chart(chart, use_container_width=True)

                    else:
                        st.warning(f"선택된 조건에 해당하는 최종 데이터가 없습니다 (필터링 및 계산 후 데이터가 비어있거나, 값이 없거나 숫자 변환 실패).")


                except Exception as e:
                    st.error(f"데이터 필터링 또는 시각화 중 오류가 발생했습니다: {e}")
                    # import traceback
                    # st.error(traceback.format_exc()) # 개발/디버깅 시 주석 해제

             # else: selected_class_types is empty -> 경고 메시지 출력됨
             # pass

        # else: common_years is empty -> 경고 메시지 출력됨
        # pass

    # else: 필수 컬럼 또는 SPECIFIC_CLASS_TYPES 누락 -> 오류 메시지 출력됨
    # pass


st.write("---")
st.write("이 앱은 Streamlit, Pandas, Altair, gcsfs를 사용하며, 지정된 GCS 경로 파일을 읽습니다.")