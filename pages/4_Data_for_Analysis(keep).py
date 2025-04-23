import streamlit as st
from google.cloud import storage # gcsfs 사용 시 직접 필요 없을 수 있지만, 남겨둡니다.
import pandas as pd
import altair as alt
import gcsfs # Google Cloud Storage (gs://) 경로 사용을 위해 필요
# import json
# from google.oauth2 import service_account

# credentials = service_account.Credentials.from_service_account_info(st.secrets["google"])


# ### 함수 사용으로 차트별 반복되는 코드를 간소화하고 가독성을 높였음 ###
# ### 연도 선택 후 학교급별 그룹형 차트 표시 방식에서 학교급 선택 후 연도별 그룹형 차트 표시로 수정하였음 ###

# # Streamlit 페이지 기본 설정
# st.set_page_config(page_title="Lunch", page_icon="🍱")
# st.title("17개 시도별 급식 관련 자료 분석")
# st.subheader("📊 학교알리미 공공데이터 자료를 활용한 집계")
# st.write(" ")

# # --- 데이터 로드 함수 (GCS에서 파일 읽기) ---
# @st.cache_data
# def load_data_from_gcs(gcs_uri):
#     """GCS URI로부터 데이터를 로드합니다."""
#     try:
#         # secrets["google"] → JSON 문자열로 변환
#         service_account_info = json.loads(json.dumps(dict(st.secrets["google"])))

#         # GCSFS 객체 생성 (여기서 문자열로 전달)
#         fs = gcsfs.GCSFileSystem(token=service_account_info)

#         # 파일 열기 및 읽기
#         with fs.open(gcs_uri, "rb") as f:
#             df = pd.read_csv(f)
#         return df
#     except FileNotFoundError:
#         st.error(f"오류: 지정된 GCS 경로에 파일이 없습니다: '{gcs_uri}'")
#         return None
#     except Exception as e:
#         st.error(f"GCS에서 파일을 읽는 중 오류 발생: {e}")
#         return None

# --- 데이터 로드 함수 (GCS에서 파일 읽기) ---
# Streamlit 앱 성능을 위해 데이터 로드는 캐싱하는 것이 좋습니다.
#@st.cache_data
def load_data_from_gcs(gcs_uri):
    """GCS URI로부터 데이터를 로드합니다."""
    try:
        # gcsfs가 설치되어 있으면 pandas가 자동으로 gs:// 경로를 처리합니다.
        df = pd.read_csv(gcs_uri)
        return df
    except FileNotFoundError:
        st.error(f"오류: 지정된 GCS 경로에 파일이 없습니다: '{gcs_uri}'")
        return None
    except Exception as e:
        st.error(f"GCS에서 파일을 읽는 중 오류 발생: {e}")
        return None



# --- 차트 생성 함수 (새로운 로직 반영) ---
def render_chart_section(chart_num, gcs_uri, province_col, year_col, class_type_col, selectable_years, y_axis_variables, specific_class_types, class_type_options, title_text):
    """
    단일 차트 섹션을 렌더링하는 함수 (학교급 선택 -> 연도 선택 -> 시도별 연도 그룹 막대).

    Args:
        chart_num (int): 차트 번호 (key 생성을 위해 사용).
        gcs_uri (str): 데이터 파일의 GCS URI.
        province_col (str): 시도교육청 컬럼 이름.
        year_col (str): 연도 컬럼 이름.
        class_type_col (str): 학급구분 컬럼 이름.
        selectable_years (list): 데이터에 포함될 연도 목록.
        y_axis_variables (list): Y축으로 사용할 변수 컬럼 이름 목록.
        specific_class_types (list): 전체 합계/평균 계산에 사용할 개별 학급구분 타입 목록.
        class_type_options (list): 학급구분 선택 옵션 목록 ('전체' 포함).
        title_text (str): 차트 제목에 사용될 기본 텍스트.
    """
    st.write(f"---")
    st.write(f"## {title_text}") # 차트 제목

    # 데이터 로드
    df = load_data_from_gcs(gcs_uri)

    if df is None:
        return # 데이터 로드 실패 시 함수 종료

    # 필수 컬럼 존재 체크
    required_cols_check = [province_col, year_col, class_type_col] + y_axis_variables
    missing_required_cols = [col for col in required_cols_check if col not in df.columns]

    # 데이터 내 개별 학급구분 타입 존재 체크 (전체 합계 계산을 위해 필요)
    present_specific_types_in_data = [ct for ct in specific_class_types if ct in df[class_type_col].unique()]
    missing_specific_types_in_data = [ct for ct in specific_class_types if ct not in df[class_type_col].unique()]

    if missing_required_cols:
        st.error(f"오류: CSV 파일에 필수 컬럼이 없습니다: {missing_required_cols}. 설정을 확인해주세요.")
        return
    elif missing_specific_types_in_data and '전체' in class_type_options: # '전체' 옵션이 있는데, 그에 필요한 데이터 타입이 없으면 오류
         st.error(f"오류: '{class_type_col}' 컬럼에 전체 합계/평균 계산에 필요한 타입({specific_class_types}) 중 일부({missing_specific_types_in_data})가 없습니다. 데이터 내용을 확인해주세요.")
         return
    elif not present_specific_types_in_data and '전체' in class_type_options:
         # 전체 합계를 계산할 대상이 데이터에 아예 없는 경우 (그러나 필수 컬럼 오류는 아님)
         st.warning(f"경고: 데이터에 전체 합계/평균 계산을 위한 개별 학급구분 타입({specific_class_types})이 존재하지 않습니다. '전체' 옵션을 선택해도 해당 데이터는 표시되지 않습니다.")


    # --- 위젯 설정 (학교급 먼저, 연도 나중에) ---
    st.write("**✅ 변수 설정 (데이터, 학교급, 연도)**") # 제목 변경

    # Y축 변수 선택 (key는 차트 번호와 위젯 목적 조합)
    selected_variable_name = st.selectbox(
        "시각화할 데이터를 선택해주세요",
        sorted(y_axis_variables),
        key=f"chart{chart_num}_select_variable"
    )

    # 학교급 선택 (하나만 선택 가능하도록 변경)
    st.write(f"학교급{class_type_col}을 선택해주세요.") # 라벨 텍스트 수정
    # --- default 대신 index 사용 (Streamlit 구버전 호환) ---
    default_index = 0 # 기본값은 첫 번째 항목 (인덱스 0)
    if '전체' in class_type_options:
        try:
            default_index = class_type_options.index('전체')
        except ValueError:
            pass # '전체'가 목록에 없는 경우

    # 만약 class_type_options가 비어있다면 selectbox는 오류를 발생시킵니다.
    # 비어있는 경우를 대비한 추가 로직이 필요할 수 있으나, 현재는 비어있지 않다고 가정합니다.
    selected_class_type = st.selectbox( # <-- Use selectbox
        f"학교급 선택", # 라벨 간소화
        class_type_options,
        index=default_index, # <-- index 사용
        key=f"chart{chart_num}_select_class_type" # 고유한 key
    )

    # 데이터에 실제로 있는 연도 확인 및 특정 연도 선택
    available_years_in_data = sorted(df[year_col].unique().tolist())
    common_years = sorted(list(set(available_years_in_data) & set(selectable_years)))

    selected_plot_years = [] # 연도 선택 결과 (리스트)

    if not common_years:
        st.warning(f"지정된 연도({selectable_years}) 중 데이터에 실제로 존재하는 연도가 없습니다. 데이터의 '{year_col}' 컬럼 값을 확인해주세요.")
    # 연도 선택 위젯 표시 조건: 데이터 변수가 선택되었고, 학교급이 하나 선택되었을 경우
    elif selected_class_type is None: # 학교급이 선택되지 않았으면 (selectbox는 None을 반환할 수 있음)
         st.info(f"학교급 선택을 완료하면 연도 선택 위젯이 표시됩니다.")
    else: # 학교급이 하나 선택되었고, 데이터에 유효 연도가 있는 경우
        # 연도 선택 (나중에 표시, 여러 개 선택 가능)
        selected_plot_years = st.multiselect(
            "연도를 선택해주세요",
            sorted([str(y) for y in common_years]),
            default=sorted([str(y) for y in common_years]), # 기본값: 모든 유효 연도 선택
            key=f"chart{chart_num}_select_years" # 고유한 key (복수형)
        )

    # 정렬 옵션 체크박스
    sort_by_value_checkbox = st.checkbox(
        "내림차순 (Y축 값 기준)으로 정렬", # 라벨 수정
        value=True,
        key=f"chart{chart_num}_sort_checkbox" # 고유한 key
    )


    # --- 데이터 필터링 및 준비 (단수 학교급 선택 로직 반영) ---
    # 필요한 모든 선택이 완료되었을 경우에만 시각화 로직 실행
    if selected_variable_name and selected_class_type is not None and selected_plot_years: # 데이터 변수, 학교급(하나), 연도(하나 이상) 선택됨
        try:
            # 제목에 선택된 학교급(단수)와 연도들(복수) 표시
            st.write(f"### {selected_variable_name} by {province_col} ({selected_class_type} - {', '.join(selected_plot_years)}년)")

            # 1. 선택된 연도로 데이터 1차 필터링
            df_filtered_by_year = df[df[year_col].astype(str).isin(selected_plot_years)].copy()

            # 2. 값 컬럼 이름 통일 및 숫자 변환, 기본 NaN 제거
            df_process = df_filtered_by_year.copy()
            df_process.rename(columns={selected_variable_name: '값'}, inplace=True)
            df_process['값'] = pd.to_numeric(df_process['값'], errors='coerce')
            df_process.dropna(subset=['값', province_col, class_type_col, year_col], inplace=True)


            # 3. 선택된 학교급(하나)에 따라 최종 시각화 데이터프레임 구성
            df_to_plot = pd.DataFrame()
            dataframes_to_concat = []

            # 사용자가 '전체'를 선택했다면, '전체' 데이터 생성 및 추가
            if selected_class_type == '전체':
                 # '전체'가 선택된 경우: 개별 학교급들의 합계 또는 평균 계산
                 specific_types_in_processed_data_for_total = [
                      item for item in specific_class_types
                      if item in df_process[class_type_col].unique()
                 ]
                 if specific_types_in_processed_data_for_total:
                      df_specific_types_only_for_total = df_process[
                           df_process[class_type_col].isin(specific_types_in_processed_data_for_total)
                      ].copy()

                      if not df_specific_types_only_for_total.empty:
                           # 시도교육청별, 연도별로 그룹화하여 합계/평균 계산
                           # 차트 1, 2는 합계, 차트 3, 4는 평균 (사용자 요청 반영)
                           if chart_num == 3 or chart_num == 4:
                               df_total = df_specific_types_only_for_total.groupby([province_col, year_col])['값'].mean().reset_index()
                               # --- 여기에 평균 계산 안내 메시지 추가 ---
                               st.info("참고: 선택된 학교급이 '전체'일 경우, 개별 학교급 데이터의 **평균**으로 계산됩니다.")

                           else: # 차트 1, 2
                               df_total = df_specific_types_only_for_total.groupby([province_col, year_col])['값'].sum().reset_index()
                               # --- 여기에 합계 계산 안내 메시지 추가 ---
                               st.info("참고: 선택된 학교급이 '전체'일 경우, 개별 학교급 데이터의 **합계**로 계산됩니다.")


                           df_total[class_type_col] = '전체' # '전체' 학급구분 값 추가
                           dataframes_to_concat.append(df_total)
                      else:
                           st.warning(f"경고: 선택된 연도({', '.join(selected_plot_years)}) 데이터에 전체 합계/평균 계산을 위한 개별 학교급 타입({specific_class_types})의 유효한 값이 없어 '전체' 값을 계산할 수 없습니다.")


            # 사용자가 개별 학교급 타입을 선택했다면 해당 데이터 추가
            # selected_class_type 변수는 이미 선택된 단일 값입니다.
            elif selected_class_type != '전체':
                 # 선택된 특정 학교급이 실제 데이터(df_process)에 있는지 확인
                 if selected_class_type in df_process[class_type_col].unique():
                      # --- 여기서 해당 학교급 데이터만 필터링하고 dataframes_to_concat에 추가 ---
                      df_specific_selected = df_process[
                           df_process[class_type_col] == selected_class_type # <-- Filter using == for single value
                      ].copy()
                      if not df_specific_selected.empty:
                           dataframes_to_concat.append(df_specific_selected)
                      else:
                           st.warning(f"경고: 선택된 연도({', '.join(selected_plot_years)}) 데이터에 선택된 학교급('{selected_class_type}')의 유효한 값이 없습니다.")
                 else:
                     st.warning(f"경고: 선택된 학교급('{selected_class_type}')이 데이터에 존재하지 않습니다.")


            # 구성된 데이터프레임들을 하나로 합침
            if dataframes_to_concat:
                df_to_plot = pd.concat(dataframes_to_concat, ignore_index=True)

                # 최종 정리 (혹시 모를 중복 등 제거)
                # 시도교육청, 연도, 학급구분 조합으로 중복 제거 (학교급은 하나만 있으므로 사실상 시도교육청, 연도 조합)
                df_plot = df_to_plot.dropna(subset=['값', province_col, class_type_col, year_col]).copy()
                df_plot = df_plot.drop_duplicates(subset=[province_col, year_col, class_type_col]).copy()

            else:
                 df_plot = pd.DataFrame() # 시각화할 데이터 없음


            # --- 시각화 (Altair 그룹형 막대 그래프 - 연도별 그룹핑) ---
            if not df_plot.empty:
                # 정렬 파라미터 결정 (sort_by_value_checkbox 사용)
                sort_param = '-y' if sort_by_value_checkbox else 'ascending'

                # --- Y축 스케일 및 포맷 결정 (차트3 비율 데이터용) ---
                y_scale = alt.Undefined # 기본 스케일 (Altair 자동 결정)
                value_format = ',.0f' # 기본 값 포맷 (천 단위 쉼표, 소수점 없음)

                # 현재 차트가 3번이고, 선택된 변수가 비율 변수 목록에 있는지 확인
                # y_axis_variables는 이 함수에 전달된 그 차트의 변수 목록입니다.
                if chart_num == 4 and selected_variable_name in y_axis_variables:
                    y_scale = alt.Scale(domain=[0, 100]) # Y축 범위를 0 ~ 100으로 고정
                    value_format = ',.1f' # 비율은 소수점 첫째 자리까지 표시하도록 포맷 변경

                # --- Y축 인코딩 설정 (결정된 scale과 format 적용) ---
                y_encoding = alt.Y(
                    '값',
                    type='quantitative',
                    title=selected_variable_name,
                    scale=y_scale, # 비율 차트일 경우 0-100 스케일 적용
                    axis=alt.Axis(title=selected_variable_name, format=value_format) # 축 라벨 포맷 적용
                )

                # --- Altair 인코딩 설정 (그룹형 막대 그래프 - 연도별 그룹핑) ---
                chart_encoding = {
                    # x축은 시도교육청 (메인 카테고리)
                    "x": alt.X(
                        province_col,
                        sort=sort_param,
                        axis=alt.Axis(title=province_col, labels=True), # 축 라벨 표시 확인
                        # ***이 부분이 핵심***
                        # scale의 paddingInner 속성을 사용하여 개별 시도교육청 그룹 사이의 간격을 조절합니다.
                        # 값은 0~1 사이의 비율이며, 클수록 간격이 넓어집니다.
                        scale=alt.Scale(paddingInner=0.4) # 예시 값 0.2, 필요에 따라 0.1, 0.3 등으로 조절하세요.
                    ),
                    # y축 인코딩
                    "y": y_encoding, # 위에 정의된 y_encoding 변수 사용

                    # 색상은 연도별로 다르게
                    "color": alt.Color(year_col, title=year_col), # <-- 연도별 색상

                    # ***이 부분이 핵심***
                    # xOffset을 사용하여 동일한 시도교육청 내에서 연도별 막대를 옆으로 나란히 배치
                    "xOffset": alt.XOffset(year_col, title=year_col), # <-- 연도별 그룹핑

                    # 툴팁 설정
                    "tooltip": [
                        province_col,
                        year_col, # <-- 툴팁에 연도 표시
                        class_type_col, # <-- 툴팁에 선택된 학교급 표시
                        alt.Tooltip('값', title=selected_variable_name, format=value_format) # 값 포맷 적용
                    ]
                }

                # --- 차트 생성 ---
                chart = alt.Chart(df_plot).mark_bar(size=8).encode(**chart_encoding).properties(
                    title=f'{selected_variable_name} by {province_col} ({(selected_class_type)} - {", ".join(selected_plot_years)}년)' # 제목에 학교급과 연도 모두 표시
                ).interactive() # 확대/축소, 팬 기능 활성화


                st.altair_chart(chart, use_container_width=True) # Streamlit 컨테이너 넓이에 맞춤

            else:
                st.warning(f"선택된 조건에 해당하는 최종 시각화 데이터가 없습니다. 설정(학교급, 연도)을 다시 확인해주세요.")


        except Exception as e:
            st.error(f"데이터 필터링 또는 시각화 중 오류가 발생했습니다: {e}")
            st.exception(e) # 디버깅을 위해 예외 정보 출력

    else: # 필수 위젯 중 하나라도 선택되지 않았으면
        st.info("위의 설정(데이터, 학교급, 연도 선택)을 완료하면 차트가 표시됩니다.")


# --------------------- 차트 호출들 (변경 없음) -------------------------------------------------------------------------------------------------
# render_chart_section 함수 호출하는 부분은 그대로 유지합니다.
# 함수 내부 로직이 변경되었기 때문에, 동일한 호출로 새로운 형태의 차트가 그려집니다.

# --------------------- 차트1 호출 -------------------------------------------------------------------------------------------------
render_chart_section(
    chart_num=1,
    gcs_uri='gs://school-lunch-bucket/lunch_menu/analysis_data_csv/student_by_class_and_school.csv',
    province_col='시도교육청',
    year_col='연도',
    class_type_col='구분',
    selectable_years=[2021, 2022, 2023, 2024],
    y_axis_variables=['학생수 합계'], # 이 목록을 실제 컬럼 이름들로 채워야 합니다.
    specific_class_types=['초등학교', '중학교', '고등학교'], # 전체 합계 계산에 사용될 개별 타입
    class_type_options=['전체', '초등학교', '중학교', '고등학교'], # 사용자가 선택할 옵션
    title_text="시도교육청별 학교급별 학생수 집계" # 차트 제목 텍스트 (학교급별로 변경)
)


# --------------------- 차트2 호출 -------------------------------------------------------------------------------------------------
render_chart_section(
    chart_num=2,
    gcs_uri='gs://school-lunch-bucket/lunch_menu/analysis_data_csv/school_lunch_propotion_rate.csv', # GCS 파일 경로 확인
    province_col='시도교육청',
    year_col='연도',
    class_type_col='구분',      # 컬럼 이름 확인 ('학급구분' 대신 '구분'으로 변경됨)
    selectable_years=[2021, 2022, 2023, 2024],
    y_axis_variables=['급식비 합계'], # 이 목록을 실제 컬럼 이름들로 채워야 합니다.
    specific_class_types=['초등학교', '중학교', '고등학교'], # 차트2 데이터에 맞는 개별 타입 목록으로 수정 필요
    class_type_options=['전체', '초등학교', '중학교', '고등학교'], # 차트2 데이터에 맞는 선택 옵션 목록으로 수정 필요
    title_text="시도교육청별 학교급별 급식비 집계" # 차트 제목 텍스트 (학교급별로 변경)
)

# --------------------- 차트3 호출 -------------------------------------------------------------------------------------------------
render_chart_section(
    chart_num=3,
    gcs_uri='gs://school-lunch-bucket/lunch_menu/analysis_data_csv/school_lunch_money_per_student.csv', # GCS 파일 경로 확인
    province_col='시도교육청',
    year_col='연도',
    class_type_col='구분',      # 컬럼 이름 확인 ('학급구분' 대신 '구분'으로 변경됨)
    selectable_years=[2021, 2022, 2023, 2024],
    y_axis_variables=['학생 1인당 급식비'], # 이 목록을 실제 컬럼 이름들로 채워야 합니다.
    specific_class_types=['초등학교', '중학교', '고등학교'], # 차트2 데이터에 맞는 개별 타입 목록으로 수정 필요
    class_type_options=['전체', '초등학교', '중학교', '고등학교'], # 차트2 데이터에 맞는 선택 옵션 목록으로 수정 필요
    title_text="시도교육청별 학교급별 학생 1인당 급식비 집계" # 차트 제목 텍스트 (학교급별로 변경)
)


# --------------------- 차트4 호출 -------------------------------------------------------------------------------------------------
render_chart_section(
    chart_num=4,
    gcs_uri='gs://school-lunch-bucket/lunch_menu/analysis_data_csv/school_lunch_propotion_rate.csv', # GCS 파일 경로 확인 (차트2와 동일)
    province_col='시도교육청',
    year_col='연도',
    class_type_col='구분',      # 컬럼 이름 확인 ('학급구분' 대신 '구분'으로 변경됨)
    selectable_years=[2021, 2022, 2023, 2024],
    y_axis_variables=['교육청 비율', '자치단체 비율', '보호자 비율', '기타 비율'], # 이 목록을 실제 컬럼 이름들로 채워야 합니다.
    specific_class_types=['초등학교', '중학교', '고등학교'], # 차트3 데이터에 맞는 개별 타입 목록으로 수정 필요 (차트2와 동일할 가능성 높음)
    class_type_options=['전체', '초등학교', '중학교', '고등학교'], # 차트3 데이터에 맞는 선택 옵션 목록으로 수정 필요 (차트2와 동일할 가능성 높음)
    title_text="시도교육청별 학교급별 급식비 부담 비율" # 차트 제목 텍스트 (학교급별로 변경)
)

