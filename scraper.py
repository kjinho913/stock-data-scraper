# scraper.py (수정된 코드)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

def get_kospi_stocks_naver():
    stock_data = []
    
    for page in range(1, 2):
        url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok=0&page={page}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"오류가 발생했습니다: {e}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_='type_2')
        if table is None:
            continue
        
        rows = table.find('tbody').find_all('tr')

        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 12 or cols[1].text.strip() == 'N/A':
                continue

            try:
                stock_link = cols[1].find('a')
                name = stock_link.text.strip()
                stock_code = stock_link['href'].split('=')[-1]
                price = int(cols[2].text.strip().replace(',', ''))

                # ⭐️ [수정] '전일비'를 '상태'와 '가격'으로 분리
                change_td = cols[3]
                
                # 1. '전일대비' (상태: 상승, 하락, 보합) 추출
                change_status = "보합" # 기본값
                if blind_span := change_td.find('span', class_='blind'):
                    change_status = blind_span.text.strip()

                # 2. '전일대비가격' (숫자) 추출
                # .text.strip()으로 "하락 1,500" 같은 텍스트를 얻고, .split()으로 분리 후 마지막 요소(숫자)를 선택
                price_change_value_str = change_td.text.strip().split()[-1]
                price_change_value = int(price_change_value_str.replace(',', ''))
                
                change_rate_text = cols[4].text.strip()

                # --- 나머지 정보 ---
                par_value_str = cols[5].text.strip().replace(',', '')
                par_value = int(par_value_str) if par_value_str != 'N/A' else 0
                market_cap = int(cols[6].text.strip().replace(',', '')) 
                listed_shares_str = cols[7].text.strip().replace(',', '')
                listed_shares = int(listed_shares_str) if listed_shares_str != 'N/A' else 0
                foreign_ratio_str = cols[8].text.strip().replace('%', '')
                foreign_ratio = float(foreign_ratio_str) if foreign_ratio_str != 'N/A' else 0.0
                volume_str = cols[9].text.strip().replace(',', '')
                volume = int(volume_str) if volume_str != 'N/A' else 0
                per_str = cols[10].text.strip().replace(',', '')
                per = float(per_str) if per_str != 'N/A' else 0.0
                roe_str = cols[11].text.strip().replace(',', '')
                roe = float(roe_str) if roe_str != 'N/A' else 0.0

                stock_data.append({
                    'name': name,
                    'stock_code': stock_code,
                    'price': price,
                    'change_status': change_status,       # [수정] 상태 (상승/하락/보합)
                    'price_change_value': price_change_value,  # [수정] 변동 가격
                    'change_rate_text': change_rate_text,
                    'par_value': par_value,
                    'market_cap(억)': market_cap,
                    'listed_shares': listed_shares,
                    'foreign_ratio': foreign_ratio,
                    'volume': volume,
                    'per': per,
                    'roe': roe
                })
            except (ValueError, AttributeError, IndexError, TypeError) as e:
                continue
    
    if not stock_data:
        return None
        
    return pd.DataFrame(stock_data)

if __name__ == '__main__':
    stocks_df = get_kospi_stocks_naver()
    if stocks_df is not None:
        print("네이버 금융 스크레이핑 성공!")
        print(stocks_df.head())

        # ⭐⭐ [추가] CSV 파일로 저장하는 코드
        output_file_name = 'kospi_stocks.csv'
        try:
            stocks_df.to_csv(output_file_name, index=False, encoding='utf-8-sig')
            print(f"데이터가 '{output_file_name}' 파일에 성공적으로 저장되었습니다.")
        except Exception as e:
            print(f"CSV 파일 저장 중 오류 발생: {e}")