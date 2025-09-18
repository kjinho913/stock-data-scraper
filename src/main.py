# src/main.py

from .scraper import get_kospi_stocks_naver
import pandas as pd
import os

def main():
    """
    스크래핑 프로세스의 메인 실행 함수.
    """
    stocks_df = get_kospi_stocks_naver()
    
    if stocks_df is not None:
        print("네이버 금융 스크래핑 성공!")
        print(stocks_df.head())
        
        # ⭐ CSV 파일 저장 경로 설정
        output_file_path = 'data/kospi_stocks.csv'
        
        try:
            # 'data' 폴더가 없으면 생성
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            
            # CSV 파일로 저장
            stocks_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
            print(f"데이터가 '{output_file_path}' 파일에 성공적으로 저장되었습니다.")
        except Exception as e:
            print(f"CSV 파일 저장 중 오류 발생: {e}")

if __name__ == '__main__':
    # 이 파일을 직접 실행할 때 main() 함수 호출
    main()