# src/main.py

import pandas as pd
import os
import datetime
from .scraper import get_kospi_stocks_naver
from .data_import import load_data_to_db, get_column_names

# ⭐ [수정] DB_PARAMS 변수 제거

def main():
    """
    스크래핑부터 데이터베이스 적재까지 전체 프로세스 실행
    """
    stocks_df = get_kospi_stocks_naver()
    
    if stocks_df is not None:
        print("네이버 금융 스크래핑 성공!")
        
        today_date = datetime.date.today().strftime('%Y%m%d')
        file_path = f'data/kospi_stocks_{today_date}.csv'
        
        # 'data' 폴더가 없으면 생성
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # CSV 파일로 저장
        try:
            stocks_df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"데이터가 '{file_path}' 파일에 성공적으로 저장되었습니다.")
        except Exception as e:
            print(f"CSV 파일 저장 중 오류 발생: {e}")
            return
            
        # ⭐ [수정] db_params를 전달하지 않음
        if load_data_to_db(file_path):
            print("프로세스 완료: 데이터베이스에 성공적으로 적재되었습니다.")
        else:
            print("프로세스 실패: 데이터베이스 적재 중 오류 발생.")

if __name__ == '__main__':
    main()