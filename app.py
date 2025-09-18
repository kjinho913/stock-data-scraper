# app.py (수정된 코드)

import streamlit as st
import pandas as pd
from scraper import get_kospi_stocks_naver
import FinanceDataReader as fdr
from datetime import datetime, timedelta

# ⭐️ [수정] 시가총액을 위한 전용 서식 함수 재작성
def format_market_cap_korean(cap_in_billion):
    """시가총액(억 단위)을 조, 억 단위의 원화 문자열로 정확하게 변환합니다."""
    if pd.isna(cap_in_billion) or cap_in_billion == 0:
        return 'N/A'
    # 1. 단위를 다시 '억 원'으로 변환합니다.
    # 예: 459960.0 (억) -> 4599600 (억)
    cap_in_eok = cap_in_billion * 1
    # 2. '조'와 '억' 단위를 계산합니다. (1조 = 10,000억)
    trillion = int(cap_in_eok // 10000) # 조 단위
    billion = int(cap_in_eok % 10000)   # 나머지 억 단위
    result = ""
    if trillion > 0:
        result += f"{trillion:,}조 "
    if billion > 0:
        result += f"{billion:,}억"
    return result.strip() + "원"

# --- 기존 단위 변환 함수들 (현재가, 거래량 용) ---
def format_korean_units(number):
    if pd.isna(number) or number == 0: return '0'
    units = ['', '만', '억', '조']
    num_str = str(int(number))
    result = ""
    unit_idx = 0
    while len(num_str) > 0:
        part = num_str[-4:]
        num_str = num_str[:-4]
        if int(part) > 0: result = f"{int(part):,}{units[unit_idx]} {result}"
        unit_idx += 1
    return result.strip()

def format_price_korean(price):
    return f"{format_korean_units(price)}원"


st.set_page_config(page_title="나만의 주식 추천 앱", page_icon="📈", layout="wide")
st.title('📈 나만의 주식 추천 앱')
st.write('네이버 금융 데이터를 기반으로 원하는 조건의 주식을 찾아보세요.')

@st.cache_data(ttl=3600)
def load_data():
    return get_kospi_stocks_naver()

stock_df = load_data()

if stock_df is not None and not stock_df.empty:
    # (사이드바 부분은 이전과 동일)
    st.sidebar.header('🔍 필터링 조건 설정')
    st.sidebar.subheader('주가수익비율 (PER)')
    col1, col2 = st.sidebar.columns(2)
    per_min = st.sidebar.number_input('최소 PER', 0.0, value=5.0, step=1.0, format="%.2f", key="per_min")
    per_max = st.sidebar.number_input('최대 PER', 0.0, value=30.0, step=1.0, format="%.2f", key="per_max")
    st.sidebar.subheader('시가총액(억)')
    market_cap_min_default = int(stock_df['시가총액(억)'].min())
    market_cap_max_default = int(stock_df['시가총액(억)'].max())
    cap_min = st.sidebar.number_input('최소 시총', 0, value=market_cap_min_default, step=100, key="cap_min")
    cap_max = st.sidebar.number_input('최대 시총', 0, value=market_cap_max_default, step=100, key="cap_max")

    filtered_df = stock_df[
        (stock_df['PER(배)'] >= per_min) & (stock_df['PER(배)'] <= per_max) & (stock_df['PER(배)'] != 0) &
        (stock_df['시가총액(억)'] >= cap_min) & (stock_df['시가총액(억)'] <= cap_max)
    ].copy()

    # --- 표시용 컬럼 생성 ---
    filtered_df['시가총액'] = filtered_df['시가총액(억)'].apply(format_market_cap_korean)
    filtered_df['현재가(원)'] = filtered_df['현재가'].apply(format_price_korean)
    filtered_df['거래량(주)'] = filtered_df['거래량'].apply(format_korean_units)
    filtered_df['전일대비가격(원)'] = filtered_df['전일대비가격'].apply(format_price_korean)
    # 전일대비가격은 숫자이므로 단위 변환 불필요 (또는 format_price_korean 사용 가능)
    
    selected_stock_info = None
    if 'stock_selector' in st.session_state:
        selected_rows = st.session_state.stock_selector.get("selection", {"rows": []})["rows"]
        if selected_rows: selected_stock_info = filtered_df.iloc[selected_rows[0]]
    if selected_stock_info is None and not filtered_df.empty:
        selected_stock_info = filtered_df.iloc[0]

    st.header('📈 주가 그래프')
    # (그래프 부분은 이전과 동일)
    if selected_stock_info is not None:
        stock_name = selected_stock_info['종목명']
        stock_code = selected_stock_info['종목코드']
        st.subheader(f"**{stock_name}** ({stock_code})")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        try:
            history = fdr.DataReader(stock_code, start_date, end_date)
            st.line_chart(history['Close'])
        except Exception as e:
            st.error(f"주가 데이터를 가져오는 중 오류가 발생했습니다: {e}")
    else:
        st.info("조건에 맞는 종목이 없습니다.")

    st.header('📊 추천 종목 리스트')
    st.write(f"총 {len(filtered_df)}개 검색됨 (원하는 종목을 클릭하면 위 그래프가 변경됩니다)")
    
    st.data_editor(
        filtered_df,
        column_order=[
            '종목명', '현재가(원)', '전일대비', '전일대비가격(원)', '등락률', 
            '시가총액', '거래량(주)', 'PER(배)', 'ROE(%)', '외국인비율(%)'
        ],
        column_config={
            #'전일대비가격': st.column_config.NumberColumn(format="%,d원"),
            "외국인비율(%)": st.column_config.NumberColumn(format="%.2f%%"),
            "PER(배)": st.column_config.NumberColumn(format="%.2f"),
            "ROE(%)": st.column_config.NumberColumn(format="%.2f%%"),
        },
        disabled=filtered_df.columns,
        hide_index=True,
        key="stock_selector"
    )

else:
    st.error("데이터를 가져오는 데 실패했습니다. 잠시 후 다시 시도해주세요.")