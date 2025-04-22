import streamlit as st
from google.cloud import storage # gcsfs 사용 시 직접 필요 없을 수 있지만, 남겨둡니다.
import pandas as pd
import altair as alt
import gcsfs # Google Cloud Storage (gs://) 경로 사용을 위해 필요

### 함수 사용으로 차트별 반복되는 코드를 간소화하고 가독성을 높였음 ###

# Streamlit 페이지 기본 설정
st.set_page_config(page_title="급식 분석 대시보드", page_icon="🍱", layout="wide") # 레이아웃 wide로 설정
st.title("17개 시도별 급식 관련 자료 분석")
st.write("## 학교알리미 공공데이터 자료를 활용한 17개 시도별 학교급별 집계")


# --- 데이터 로드 함수 (GCS에서 파일 읽기) ---
# Streamlit 앱 성능을 위해 데이터 로드는 캐싱하는 것이 좋습니다.
@st.cache_data
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

# --- 차트 생성 함수 (코드 중복 최소화를 위해 함수로 만듦) ---
# 각 차트 섹션의 로직을 함수로 만들어 재사용성을 높입니다.
def render_chart_section(chart_num, gcs_uri, province_col, year_col, class_type_col, selectable_years, y_axis_variables, specific_class_types, class_type_options, title_text):
    """
    단일 차트 섹션을 렌더링하는 함수.

    Args:
        chart_num (int): 차트 번호 (key 생성을 위해 사용).
        gcs_uri (str): 데이터 파일의 GCS URI.
        province_col (str): 시도교육청 컬럼 이름.
        year_col (str): 연도 컬럼 이름.
        class_type_col (str): 학급구분 컬럼 이름.
        selectable_years (list): 사용자 선택 가능한 연도 목록.
        y_axis_variables (list): Y축으로 사용할 변수 컬럼 이름 목록.
        specific_class_types (list): 전체 합계 계산에 사용할 개별 학급구분 타입 목록.
        class_type_options (list): 학급구분 선택 옵션 목록 (전체 포함).
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
         st.error(f"오류: '{class_type_col}' 컬럼에 전체 합계 계산에 필요한 타입({specific_class_types}) 중 일부({missing_specific_types_in_data})가 없습니다. 데이터 내용을 확인해주세요.")
         return
    elif not present_specific_types_in_data and '전체' in class_type_options:
         # 전체 합계를 계산할 대상이 데이터에 아예 없는 경우 (그러나 필수 컬럼 오류는 아님)
         st.warning(f"경고: 데이터에 전체 합계 계산을 위한 개별 학급구분 타입({specific_class_types})이 존재하지 않습니다.")
         # 이 경우 '전체' 옵션을 선택해도 실제 전체 합계 데이터는 생성되지 않습니다. 사용자가 인지하도록 함.


    # --- 위젯 설정 ---
    st.write("### 변수 설정 (데이터, 연도, 학교급)")

    # Y축 변수 선택 (key는 차트 번호와 위젯 목적 조합)
    selected_variable_name = st.selectbox(
        "데이터를 선택해주세요",
        sorted(y_axis_variables),
        key=f"chart{chart_num}_select_variable"
    )

    # 데이터에 실제로 있는 연도 확인 및 특정 연도 선택
    available_years_in_data = sorted(df[year_col].unique().tolist())
    common_years = sorted(list(set(available_years_in_data) & set(selectable_years)))

    selected_plot_year = None # 기본값 설정

    if not common_years:
        st.warning(f"지정된 연도({selectable_years}) 중 데이터에 실제로 존재하는 연도가 없습니다. 데이터의 '{year_col}' 컬럼 값을 확인해주세요.")
        # common_years가 없으므로 연도 선택 selectbox를 표시하지 않습니다.
    else:
        # 연도 선택 (key는 차트 번호와 위젯 목적 조합)
        selected_plot_year = st.selectbox(
            "데이터를 볼 연도를 선택해주세요",
            sorted([str(y) for y in common_years]),
            key=f"chart{chart_num}_select_year"
        )

    # 학급구분 선택
    # common_years가 있어야 연도를 선택하고, 그래야 학급구분 선택 위젯을 보여주는 것이 논리적일 수 있습니다.
    # 또는 연도 선택과 별개로 항상 보여줄 수도 있습니다. 여기서는 연도 선택 가능할 때 보여주도록 합니다.
    selected_class_types = [] # 기본값 빈 리스트

    if selected_plot_year is not None: # 연도가 선택되었을 경우에만 학급구분 위젯 표시
        st.write(f"학교급별 '{class_type_col}'을(를) 선택해주세요. (*전체를 선택할 경우 초등학교, 중학교, 고등학교는 체크 해제해 주세요.)")
        selected_class_types = st.multiselect(
            f"{class_type_col} 선택",
            class_type_options,
            default=['전체'] if '전체' in class_type_options else (class_type_options[0] if class_type_options else []), # 기본값 설정
            key=f"chart{chart_num}_select_class_type" # 고유한 key
        )

    # 정렬 옵션 체크박스
    # 연도가 선택되고 학급구분이 선택된 이후에만 표시하는 것이 논리적일 수 있습니다.
    # 또는 항상 표시하고 시각화에만 반영할 수도 있습니다. 여기서는 시각화 전에 표시합니다.
    sort_by_value_checkbox = st.checkbox(
        "Y축 값 (내림차순)으로 정렬",
        value=True,
        key=f"chart{chart_num}_sort_checkbox" # 고유한 key
    )


    # --- 데이터 필터링 및 준비 (이제 위젯에서 선택된 값들 사용) ---
    # 필요한 모든 선택이 완료되었을 경우에만 시각화 로직 실행
    if selected_plot_year is not None and selected_class_types: # 연도가 선택되었고, 학급구분도 최소 하나 선택됨
        try:
            st.write(f"### {selected_plot_year}년 {selected_variable_name} ({province_col}별 - {', '.join(selected_class_types)})")

            # 1. 선택된 연도로 필터링 및 필요한 컬럼만 선택
            cols_to_select = [province_col, class_type_col, selected_variable_name]
            df_year_filtered = df[df[year_col].astype(str) == selected_plot_year][cols_to_select].copy()

            # 2. 값 컬럼 이름 통일 및 숫자 변환, 기본 NaN 제거
            df_process = df_year_filtered.copy()
            df_process.rename(columns={selected_variable_name: '값'}, inplace=True)
            df_process['값'] = pd.to_numeric(df_process['값'], errors='coerce')
            df_process.dropna(subset=['값', province_col], inplace=True) # 값 또는 시도교육청 없는 행 제거


            # '전체' 값 계산 및 시각화 데이터프레임 준비
            df_to_plot = pd.DataFrame() # 최종 시각화 데이터프레임을 빈 것으로 시작

            # 사용자가 '전체'를 선택했고, 합계 계산 대상 데이터가 df_process에 있는 경우
            # present_specific_types_in_data는 이 함수 시작 시 계산된 전체 데이터 기준입니다.
            # 여기서는 현재 필터링된 df_process 내에 합계 대상이 있는지 확인해야 합니다.
            specific_types_in_processed_data = [
                 item for item in specific_class_types
                 if item in df_process[class_type_col].unique()
            ]

            if '전체' in selected_class_types and specific_types_in_processed_data:
                # df_process에서 합산 대상 개별 타입들만 필터링하여 합계/평균 계산
                df_specific_types_only = df_process[df_process[class_type_col].isin(specific_types_in_processed_data)].copy()

                if not df_specific_types_only.empty: # 합계/평균 계산 대상 데이터가 있는 경우만 진행

                    # --- 차트 번호에 따라 합계 또는 평균 계산 ---
                    if chart_num == 3:
                        # 차트 3 (비율 데이터)인 경우, '전체'는 개별 타입 비율의 '평균'으로 계산
                        # 주의: 개별 학교급별 비율의 평균이 실제 전체 비율과 미묘하게 다를 수 있습니다 (가중 평균이 아니므로).
                        df_total = df_specific_types_only.groupby(province_col)['값'].mean().reset_index()
                        st.info("참고: '전체'는 선택된 개별 학교급 데이터의 **평균**입니다.") # 사용자에게 계산 방식 알림
                    else:
                        # 다른 차트 (수량 데이터)인 경우, '전체'는 개별 타입 수량의 '합계'로 계산
                        df_total = df_specific_types_only.groupby(province_col)['값'].sum().reset_index()
                        st.info("참고: '전체'는 선택된 개별 학교급 데이터의 **합계**입니다.") # 사용자에게 계산 방식 알림
                        # 수량 데이터일 경우 합계 계산 방식을 알릴 필요는 없을 수 있습니다. (필요시 st.info 추가)


                    df_total[class_type_col] = '전체' # 학급구분 컬럼 추가 및 값 설정
                    df_to_plot = pd.concat([df_to_plot, df_total], ignore_index=True)
                else:
                    st.warning(f"경고: {selected_plot_year}년 데이터에 전체 합계/평균 계산을 위한 개별 학급구분 타입({specific_class_types})의 유효한 값이 없어 '전체' 값을 계산할 수 없습니다.")



            # 사용자가 선택한 개별 학급구분 타입 데이터 추가
            # selected_class_types 중 실제 데이터(df_process)에도 있는 것들만 필터링 대상
            selected_specific_types_to_add = [
                ct for ct in selected_class_types
                if ct in df_process[class_type_col].unique() # 현재 처리 중인 데이터에 해당 타입이 있는지 확인
            ]

            if selected_specific_types_to_add: # 추가할 개별 타입이 있는 경우만 진행
                # df_process에서 이 유효한 개별 타입들에 해당하는 행들만 필터링
                df_specific_selected = df_process[df_process[class_type_col].isin(selected_specific_types_to_add)].copy()

                if not df_specific_selected.empty: # 필터링된 개별 타입 데이터가 있는 경우만 합치기
                    df_to_plot = pd.concat([df_to_plot, df_specific_selected], ignore_index=True)
                else:
                    # selected_class_types에는 있지만, selected_plot_year와 selected_variable_name 조건 하에서는
                    # 해당 데이터가 없는 경우입니다. 이미 위에서 경고 메시지로 어느 정도 포함될 수 있습니다.
                    # 필요하다면 여기서 더 구체적인 경고를 줄 수 있습니다.
                    pass


            # 최종 시각화 데이터프레임 정리: 학급구분 또는 시도교육청 누락 행 제거 및 중복 제거
            df_plot = df_to_plot.dropna(subset=[province_col, class_type_col]).copy()
            df_plot = df_plot.drop_duplicates(subset=[province_col, class_type_col]).copy()


            # --- 시각화 (Altair 막대 그래프) ---
            if not df_plot.empty:
                # 정렬 파라미터 결정 (sort_by_value_checkbox 사용)
                sort_param = '-y' if sort_by_value_checkbox else 'ascending'

                # 차트 생성
                chart = alt.Chart(df_plot).mark_bar().encode(
                    x=alt.X(province_col, sort=sort_param, title=province_col),
                    y=alt.Y('값', type='quantitative', title=selected_variable_name),
                    color=alt.Color(class_type_col, title=class_type_col), # 학급구분별 색상
                    column=alt.Column(class_type_col, header=alt.Header(titleOrient="bottom", labelOrient="bottom")), # 학급구분별로 컬럼 분리 (선택 사항, stacked bar 대신)
                    tooltip=[province_col, class_type_col, alt.Tooltip('값', title=selected_variable_name, format=',.0f')] # 툴팁에 값 포맷팅 추가
                ).properties(
                    title=f'{selected_plot_year}년 {selected_variable_name} by {province_col} ({", ".join(selected_class_types)})'
                ).interactive() # 확대/축소, 팬 기능 활성화

                # 만약 그룹화(color만 사용)된 stacked bar를 원한다면 column 인코딩을 제거합니다.
                # 예시:
                # chart = alt.Chart(df_plot).mark_bar().encode(
                #     x=alt.X(province_col, sort=sort_param, title=province_col),
                #     y=alt.Y('값', type='quantitative', title=selected_variable_name),
                #     color=alt.Color(class_type_col, title=class_type_col),
                #     tooltip=[province_col, class_type_col, alt.Tooltip('값', title=selected_variable_name, format=',.0f')]
                # ).properties(
                #     title=f'{selected_plot_year}년 {selected_variable_name} by {province_col} ({", ".join(selected_class_types)})'
                # ).interactive()


                st.altair_chart(chart, use_container_width=True)

            else:
                st.warning(f"선택된 조건에 해당하는 최종 시각화 데이터가 없습니다. 필터링 및 계산 결과를 확인해주세요.")


        except Exception as e:
            st.error(f"데이터 필터링 또는 시각화 중 오류가 발생했습니다: {e}")
            st.exception(e) # 디버깅을 위해 예외 정보 출력

    else: # 연도가 선택되지 않았거나 학급구분이 선택되지 않았으면
        # 위젯이 표시되지 않았거나, 사용자가 아직 선택을 완료하지 않은 상태
        st.info("위의 설정(연도, 학교급 선택)을 완료하면 차트가 표시됩니다.")


# --------------------- 차트1 호출 -------------------------------------------------------------------------------------------------
render_chart_section(
    chart_num=1,
    gcs_uri='gs://school-lunch-bucket/lunch_menu/analysis_data_csv/student_by_class_and_school.csv',
    province_col='시도교육청',
    year_col='연도',
    class_type_col='학급구분',
    selectable_years=[2021, 2022, 2023, 2024],
    y_axis_variables=['계 학생수'], # 이 목록을 실제 컬럼 이름들로 채워야 합니다.
    specific_class_types=['초등학교', '중학교', '고등학교'], # 전체 합계 계산에 사용될 개별 타입
    class_type_options=['전체', '초등학교', '중학교', '고등학교'], # 사용자가 선택할 옵션
    title_text="시도교육청별 연도별 학생수 집계" # 차트 제목 텍스트
)


# --------------------- 차트2 호출 -------------------------------------------------------------------------------------------------
# 차트1과 다른 GCS 파일 및 변수, 학급구분 컬럼 이름 등을 사용합니다.
render_chart_section(
    chart_num=2,
    gcs_uri='gs://school-lunch-bucket/lunch_menu/analysis_data_csv/school_lunch_propotion_rate.csv', # GCS 파일 경로 확인
    province_col='시도교육청', # 컬럼 이름 확인
    year_col='연도',         # 컬럼 이름 확인
    class_type_col='구분',      # 컬럼 이름 확인 ('학급구분' 대신 '구분'으로 변경됨)
    selectable_years=[2021, 2022, 2023, 2024],
    y_axis_variables=['금액 계'], # 이 목록을 실제 컬럼 이름들로 채워야 합니다.
    specific_class_types=['초등학교', '중학교', '고등학교'], # 차트2 데이터에 맞는 개별 타입 목록으로 수정 필요
    class_type_options=['전체', '초등학교', '중학교', '고등학교'], # 차트2 데이터에 맞는 선택 옵션 목록으로 수정 필요
    title_text="시도교육청별 연도별 급식비 집계" # 차트 제목 텍스트
)


# --------------------- 차트3 호출 -------------------------------------------------------------------------------------------------
# 차트1, 2와 다른 GCS 파일 및 변수, 학급구분 컬럼 이름 등을 사용합니다.
render_chart_section(
    chart_num=3,
    gcs_uri='gs://school-lunch-bucket/lunch_menu/analysis_data_csv/school_lunch_propotion_rate.csv', # GCS 파일 경로 확인 (차트2와 동일)
    province_col='시도교육청', # 컬럼 이름 확인
    year_col='연도',         # 컬럼 이름 확인
    class_type_col='구분',      # 컬럼 이름 확인 ('학급구분' 대신 '구분'으로 변경됨)
    selectable_years=[2021, 2022, 2023, 2024],
    y_axis_variables=['교육청 비율', '자치단체 비율', '보호자 비율', '기타 비율'], # 이 목록을 실제 컬럼 이름들로 채워야 합니다.
    specific_class_types=['초등학교', '중학교', '고등학교'], # 차트3 데이터에 맞는 개별 타입 목록으로 수정 필요 (차트2와 동일할 가능성 높음)
    class_type_options=['전체', '초등학교', '중학교', '고등학교'], # 차트3 데이터에 맞는 선택 옵션 목록으로 수정 필요 (차트2와 동일할 가능성 높음)
    title_text="시도교육청별 연도별 급식비 부담 비율" # 차트 제목 텍스트
)


st.write("---")
st.write("이 앱은 Streamlit, Pandas, Altair, gcsfs를 사용하며, 지정된 GCS 경로 파일을 읽어 데이터를 시각화합니다.")



