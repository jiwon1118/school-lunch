# 초·중·고등학교 급식 트렌드 분석과 외부 지표 연계 🍱🍽️

## :rocket: 프로젝트 개요
- **데이터 범위:** 전국 학교의 점심 급식 메뉴 및 영양소 정보
- **시기:** 2021년 ~ 현재
- **목표** 
**=> 전국 초·중·고등학교의 급식 메뉴와 영양정보를 분석하고, 학생 수·예산안 등 외부 지표와의 연계를 통해 인사이트를 도출하며, 이를 기반으로 자동화된 평가 분석 리포트 생성을 목표로 합니다.**

## 🗂️ 수집 데이터

### 1. 메인 데이터: 
- **데이터 출처 및 링크:** 나이스 교육정보 개방 포털, [급식식단정보 API](https://open.neis.go.kr/portal/data/service/selectServicePage.do?page=1&rows=10&sortColumn=&sortDirection=&infId=OPEN17320190722180924242823&infSeq=2)
- **데이터 크기 예상:** 약 30GB (4년 기준)
- **수집 주기 / 업데이트 주기:** @daily
- **제공 포맷 (예: JSON, CSV, XML, API 등):** CSV, JSON, API


### 2. 보조 데이터:
- 자치구별 학교급식운영 예산안 [지방교육재정 알리미](https://www.eduinfo.go.kr/portal/open/openData/dataSetPage.do#none;)
- **수집 주기 / 업데이트 주기:** 1년 


## 💻 분석 주제

1) 급식 메뉴 및 영양정보
2) 계절/요일별 트렌드: 여름엔 냉면류, 겨울엔 찌개류 증가 여부
3) 메뉴 인기 순위: 1년동안 출현 빈도 기준 Top 20 메뉴 시각화
4) 영양 불균형 탐지: 총 열량 칼로리, 단백질/지방 과소/과다 분석 - 초/중/고 권장 영양소 기준치 데이터 필요
5) 전국 시도별 비교: 급식 품질 vs 예산/학생수 연계 - 보조 데이터 필요
      + 학생 1인당 급식비 예산 분석 --> 분석 방향: 지역별로 1인당 급식비 추정 (예산 ÷ 식 수) / 메뉴 다양성, 영양소 질과 비교 → 예산이 높은 곳이 더 나은 식단인가?)
      + 급식 품질과의 관계 비교를 위한 시각화 -> 영양수준의 충실성, 메뉴의 다양성 평가 기준으로

**추후 추가 사항**

6) 지역별 특별 메뉴 (전국단위 분석)
7) 연도별 특별 메뉴 (시계열 분석)

## 📌 개념의 조작적 정의 요구사항 (Data Featuring)

1) 영양 수준의 충실성의 기준 선정

![Image](https://github.com/user-attachments/assets/21745637-fe51-45b7-ad66-511bdae69684)

| 평가           | 기준 설명                          |
|----------------|-------------------------------------|
| ⭐⭐⭐⭐⭐ (매우 좋음)    | 권장량 - 15% 이상 영양소 없음            |
| ⭐⭐⭐ (보통)    | 권장량 - 15% 이상 1~2 영양소            |
| ⭐ (매우 나쁨)      | 권장량 - 15% 이상 영양소 3개 이상         |

| 평가           | 기준 설명                          |
|----------------|-------------------------------------|
| ⭐⭐⭐⭐⭐ (매우 좋음)    | 에너지 권장량 700~900사이           |
| ⭐⭐⭐ (보통)    | 권장량 - 700이하 또는 900~1200사이         |
| ⭐ (매우 나쁨)      | 권장량 - 1200이상        |

2) 메뉴 다양성의 기준 선정

| 평가           | 기준 설명                          |
|----------------|-------------------------------------|
| ⭐⭐⭐⭐⭐ (매우 좋음)    | 총 메뉴가 7가지 이상           |
| ⭐⭐⭐⭐ (좋음)    | 총 메뉴가 6가지        |
| ⭐⭐⭐ (보통)      | 총 메뉴가 5가지 이하      |

3) 급식 품질의 기준 선정 - **위 일단위의 영양 평가와 메뉴 다양성을 합쳐서 월 평균으로 계산하여 평가**
- 15점 만점 기준 11점 이상 "품질 매우 좋음"
- 11점 미만 7점 이상"품질 좋음"
- 7점 미만"품질 보통"


## 📊 Airflow 활용

### DAG 설계 방향 

1. DAG 설명
- Task ID    설명
- fetch_meals_data    급식 원본 데이터 수집 (CSV, API, S3 등)
- clean_data    Null/이상치 처리, 날짜 정제 등 전처리
- sync_data    인원 간 데이터 동기화
- run_eda_analysis    메뉴 빈도 분석, 영양 통계, 계절 트렌드 등
- enrich_with_external_data   시도별 학생수, 예산 등의 외부 지표 결합
- save_results    분석 결과를 CSV, DB 등에 저장

### 저장소 
- Google Storage 사용 및 인원 간 동기화

### 알림 설정
- Discord 웹후크 기능으로 데이터 처리 중 오류 발생 시 팀 채널에 알림 활성화

### 기타


## 💠 데이터 아키텍쳐

### 조직도

![Image](https://github.com/user-attachments/assets/e48719dd-3931-4386-a609-597941169c01)
