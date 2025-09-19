# src/db_manager.py

import psycopg2
import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from io import StringIO
from psycopg2 import sql

load_dotenv()

def load_data_to_db(file_path):
    # ⭐ 1. 환경 변수와 DB 파라미터 확인
    schema_name = os.getenv("DB_SCHEMA")  
    db_params = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }
    print("--- 1단계: 환경 변수 및 DB 파라미터 확인 ---")
    print(f"스키마 이름: {schema_name}")
    print(f"DB 접속 정보: {db_params}")
    print("-" * 30)
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # ⭐ 2. 파일명에서 날짜 추출 및 날짜 형식 변환 확인
        file_name = os.path.basename(file_path)
        date_str = file_name.split('.')[0].split('_')[-1]
        formatted_date = datetime.datetime.strptime(date_str, '%Y%m%d').date().isoformat()
        print("--- 2단계: 파일명에서 날짜 추출 ---")
        print(f"추출된 날짜 문자열: {date_str}")
        print(f"변환된 ISO 형식 날짜: {formatted_date}")
        print("-" * 30)

        # ⭐ 3. 파티션 테이블 이름 생성 및 존재 여부 확인
        partition_name = f"daily_kospi_data_{date_str}"
        print("--- 3단계: 파티션 테이블 존재 여부 확인 ---")
        check_sql = f"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{schema_name}' AND tablename = '{partition_name}');"
        print(f"실행될 SQL: {check_sql}")
        cursor.execute(check_sql)
        print(cursor.execute(check_sql))
        partition_exists = cursor.fetchone()[0]
        print(f"'{partition_name}' 테이블 존재 여부: {partition_exists}")
        print("-" * 30)

        if not partition_exists:
            # ⭐ 4. 파티션 테이블 생성 SQL 확인
            print("--- 4단계: 파티션 테이블 생성 ---")
            next_day = (datetime.datetime.strptime(date_str, '%Y%m%d').date() + datetime.timedelta(days=1)).isoformat()
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{partition_name}
            PARTITION OF {schema_name}.daily_kospi_data
            FOR VALUES FROM ('{formatted_date}') TO ('{next_day}');
            """
            print(f"생성될 SQL: {create_sql}")
            cursor.execute(create_sql)
            conn.commit()
            print(f"'{schema_name}.{partition_name}' 파티션 생성 성공!")
            print("-" * 30)
        
        # ⭐ 5. CSV 파일 읽기 및 Pandas DataFrame 확인
        print("--- 5단계: CSV 파일 읽기 및 데이터 가공 ---")
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            df = pd.read_csv(f)
            df['scraped_at'] = formatted_date
            print(f"Pandas DataFrame 첫 5줄:\n{df.head()}")
            
            sio = StringIO()
            df.to_csv(sio, index=False, header=False, encoding='utf-8')
            sio.seek(0)
            print("데이터가 메모리 스트림(StringIO)으로 변환되었습니다.")
            print("-" * 30)

            # ⭐ 6. COPY SQL 명령 확인 및 실행
            print("--- 6단계: 데이터베이스에 COPY 실행 ---")
            copy_sql = sql.SQL("""
                COPY {table} FROM STDIN
                WITH (FORMAT csv, DELIMITER ',', NULL '', ENCODING 'UTF8')
            """).format(table=sql.Identifier(schema_name, partition_name))
            
            print(f"실행될 COPY SQL: {copy_sql.as_string(conn)}")
            
            cursor.copy_expert(copy_sql, sio)
            print("데이터베이스로 데이터 복사(COPY) 성공!")
            
        conn.commit()
        print("--- 7단계: 최종 커밋 완료 ---")
        print("데이터베이스에 최종 변경사항이 반영되었습니다.")
        print("데이터 적재 성공!")
        return True
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("--- 오류 발생 ---")
        print(f"데이터베이스 오류: {error}")
        return False
        
    finally:
        if conn:
            conn.close()
            print("--- 연결 종료 ---")
            print("데이터베이스 연결이 안전하게 종료되었습니다.")

def get_column_names():
    return [
        'name', 'stock_code', 'price', 'change_status', 'price_change_value', 
        'change_rate_text', 'par_value', 'market_cap', 'listed_shares', 
        'foreign_ratio', 'volume', 'per', 'roe', 'scraped_at'
    ]