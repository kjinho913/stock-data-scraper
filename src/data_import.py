import psycopg2
import os
import datetime
from dotenv import load_dotenv
import pandas as pd
from psycopg2 import sql


load_dotenv()


def get_column_names():
    # ⭐ [수정] 'scraped_at' 컬럼 추가
    return [
        'name', 'stock_code', 'price', 'change_status', 'price_change_value', 
        'change_rate_text', 'par_value', 'market_cap', 'listed_shares', 
        'foreign_ratio', 'volume', 'per', 'roe', 'scraped_at'
    ]

def load_data_to_db(file_path):
    """
    CSV 파일을 PostgreSQL 데이터베이스에 적재합니다.
    
    :param file_path: 적재할 CSV 파일 경로
    """
    # ⭐ [수정] 환경 변수에서 DB 연결 정보 가져오기
    schema_name = os.getenv("DB_SCHEMA")  
    db_params = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # ⭐ [수정] 파일명에서 날짜 부분만 정확하게 추출
        file_name = os.path.basename(file_path)
        date_str = file_name.split('.')[0].split('_')[-1] # 'kospi_stocks_20250919'에서 '20250919'만 추출
        
        # ⭐ [수정] datetime.date.fromisoformat 대신 strptime 사용
        # YYYYMMDD 형식을 YYYY-MM-DD 형식으로 변환
        formatted_date = datetime.datetime.strptime(date_str, '%Y%m%d').date().isoformat()
        
        # 파티션 테이블 이름 동적 생성
        partition_name = f"daily_kospi_data_{date_str}"
        
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = '{schema_name}' AND tablename = '{partition_name}');")
        partition_exists = cursor.fetchone()[0]

        if not partition_exists:
            print(f"'{partition_name}' 파티션 테이블이 존재하지 않아 생성합니다.")
            
            # 다음 날짜 계산
            next_day = (datetime.datetime.strptime(date_str, '%Y%m%d').date() + datetime.timedelta(days=1)).isoformat()
            
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS kospi.{partition_name}
            PARTITION OF {schema_name}.daily_kospi_data
            FOR VALUES FROM ('{formatted_date}') TO ('{next_day}');
            """
            cursor.execute(create_sql)
            conn.commit()
            print(f"'{partition_name}' 파티션 생성 성공!")

            
        print(f"'{file_path}' 파일을 '{partition_name}' 테이블에 적재 중...")
        # ⭐ [추가] columns_to_load 변수 정의
        columns_to_load = get_column_names()
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            # ⭐ [추가] CSV 파일에 날짜 정보 추가 후 COPY
            # copy_from으로 데이터를 넣을 때 scraped_at 컬럼에 날짜를 채워줘야 함
            from io import StringIO
            
            # DataFrame을 메모리상의 CSV로 변환
            # 먼저 pandas DataFrame에 날짜 컬럼을 추가
            df = pd.read_csv(f)
            df['scraped_at'] = formatted_date
            
            # 메모리 버퍼에 CSV 데이터 쓰기
            sio = StringIO()
            df.to_csv(sio, index=False, header=False, encoding='utf-8')
            sio.seek(0)

            # ⭐ [수정] copy_from을 copy_expert로 변경
            # SQL 문을 동적으로 생성
            copy_sql = sql.SQL("""
                COPY {table}({columns}) FROM STDIN
                WITH (FORMAT csv, DELIMITER ',', NULL '', ENCODING 'UTF8')
            """).format(table=sql.Identifier(schema_name, partition_name),
                        columns=sql.SQL(', ').join(sql.Identifier(col) for col in columns_to_load))
            # COPY FROM STDIN 명령어로 데이터 적재
            cursor.copy_expert(copy_sql, sio)
        conn.commit()
        print("데이터 적재 성공!")
        return True
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"데이터베이스 오류: {error}")
        return False
        
    finally:
        if conn:
            conn.close()
