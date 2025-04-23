import streamlit as st
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_info(st.secrets["google"])

st.set_page_config(page_title="Main", page_icon="🏠")
st.title("School-lunch 🍱")
#st.image

st.write("🏫 **전국 학교 급식 정보를 한눈에!**")

st.write(" ")
st.write(" ")
st.write("**📅 급식 일정 확인**")
st.write(" ")
st.write("**📊 급식 트렌드 알아보기 - 메뉴 인기 순위, New menu, 메뉴 개수 통계**")
st.write(" ")
st.write("**🧪영양 불균형 탐지, 급식 품질 분석**")
st.write(" ")
st.write(" ")




st.sidebar.markdown("# 배백조조🦢")

#st.sidebar.title("🔍 페이지 선택")
#page = st.sidebar.radio("이동할 페이지를 선택하세요", ["🏠 Main", "🧐 Products Recommend", "📅 Schedule", "✔ Outcall", "🎵 Music"])




st.markdown("---")
st.write("📌 **Made with   using Streamlit** | © 2025 School-lunch")
