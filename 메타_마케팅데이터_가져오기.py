import os
import json
from google.cloud import bigquery
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta
import time
from google.oauth2 import service_account
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ===========================================================
# 설정 파일 로드
# ===========================================================

try:
    with open('config.json', 'r', encoding='utf-8') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    print("❌ 'config.json' 파일을 찾을 수 없습니다. 같은 폴더에 넣어주세요.")
    exit()

# 전역 변수로 설정값 할당
KEY_PATH = CONFIG['google_key_file'] # 구글 키 파일명
PROJECT_ID = CONFIG['google_project_id'] # 구글 프로젝트 ID
DATASET_ID = CONFIG['bigquery_dataset'] # 빅쿼리 데이터셋 이름
SHEET_URL = CONFIG['google_sheet_url'] # 스프레드시트 주소

# -----------------------------------------------------------
# 유틸리티 함수 (결과 매핑, 토큰 가져오기)
# -----------------------------------------------------------
def get_token(account_name):
    """계정 이름에 따라 올바른 토큰을 반환"""
    if account_name == "Leshine_beauty":
        return CONFIG['meta_access_token2'] # 르샤인 전용
    return CONFIG['meta_access_token1']     # 나머지 공용

def get_result_info(action_list):
    if not isinstance(action_list, list):
        return 0.0, ""

    # 우선순위가 높은 순서대로 검사
    target_map = {
        'lead': 'Meta 잠재 고객',                  
        'complete_registration': '웹사이트 등록 완료', 
        'purchase': '구매',                       
        'contact': '문의',                        
        'schedule': '예약',                       
        'submit_application': '신청 제출',          
        'start_trial': '체험 시작',                 
        'link_click': '링크 클릭'                  
    }

    action_dict = {}
    for item in action_list:
        atype = item.get('action_type')
        val = float(item.get('value', 0))
        action_dict[atype] = val

    for key, label in target_map.items():
        if key in action_dict:
            return action_dict[key], label
            
    return 0.0, ""

# -----------------------------------------------------------
# [공통] 데이터 가공 및 업로드 함수
# -----------------------------------------------------------
def process_and_upload(data_list, table_name, option):
    # data_list가 Cursor 객체일 수도 있고 리스트일 수도 있음
    all_data = [x for x in data_list] if not isinstance(data_list, list) else data_list
    
    if not all_data:
        print("⚠️ 처리할 데이터가 없습니다.")
        return

    df = pd.DataFrame(all_data)

    rename_map = {'date_start': 'report_date', 'impressions': 'exposures'}
    df = df.rename(columns=rename_map)
    
    # actions 처리
    if 'actions' in df.columns:
        result_series = df['actions'].apply(get_result_info)
        df['leads'] = [x[0] for x in result_series]       
        df['result_type'] = [x[1] for x in result_series] 
    else:
        df['leads'] = 0
        df['result_type'] = ""

    # 숫자 변환
    numeric_cols = ['spend', 'exposures', 'clicks', 'leads']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            # 빅쿼리 에러 방지를 위해 명시적으로 타입 변경
            if col == 'spend':
                df[col] = df[col].astype(float) # 광고비는 소수점일 수 있음
            else:
                df[col] = df[col].astype(int)   # 클릭, 노출, 결과수는 정수

    # 문자 데이터 (string) 강제 변환
    string_cols = ['campaign_name', 'ad_name', 'ad_id', 'result_type']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # ★ 필터: 지출, 결과, 클릭 중 하나라도 있으면 저장
    df = df[ (df['leads'] > 0) | (df['spend'] > 0) | (df['clicks'] > 0) ]
    
    if df.empty:
        print("⚠️ 필터링 후 남은 데이터가 없습니다.")
        return

    df['collected_at'] = datetime.now() 
    df['channel'] = 'meta'

    bq_columns = [
        'campaign_name', 'ad_name', 'ad_id',
        'exposures', 'clicks', 'leads', 'result_type', 'spend', 
        'report_date', 'collected_at', 'channel'
    ]
    
    final_df = df[[c for c in bq_columns if c in df.columns]].copy()
    final_df['report_date'] = pd.to_datetime(final_df['report_date']).dt.date
    print(final_df)

    insert_bigquery(final_df, table_name, option)


def link_meta_yearly(account_info, start_str, end_str):
    ad_account_id = account_info['id']      # config에서 가져온 계정 ID
    table_name = account_info['bq_table_name'] # config에서 가져온 테이블명
    hospital_name = CONFIG['hospital_name']
    my_access_token = get_token(account_info['name'])

    # 1. 페이스북 연결
    try:
        FacebookAdsApi.init(access_token=my_access_token)
    except Exception as e:
        print("❌ 페이스북 토큰 인증 실패:", e)
        return
    
    # 날짜 변환
    start_date = datetime.strptime(start_str, '%Y%m%d')
    end_date = datetime.strptime(end_str, '%Y%m%d')
    
    fields = ['campaign_name', 'ad_name', 'ad_id', 'spend', 'impressions', 'clicks', 'actions']

    # 결과를 담을 빈 리스트
    total_data_list = []
    current_start = start_date
    print(f"📅 데이터 수집 시작: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    # 20일씩 끊어서 루프
    while current_start < end_date:
        current_end = current_start + timedelta(days=20) 
        if current_end > end_date:
            current_end = end_date

        s_str = current_start.strftime('%Y-%m-%d')
        e_str = current_end.strftime('%Y-%m-%d')

        params = {
            'level': 'ad',
            'limit': '1000',
            'time_range': {'since': s_str, 'until': e_str},
            'time_increment': '1' 
        }

        try:
            print(f"⏳ 요청 중... ({s_str} ~ {e_str})")
            data_cursor = AdAccount(ad_account_id).get_insights(fields=fields, params=params)
            chunk_data = [x for x in data_cursor]
            total_data_list.extend(chunk_data) # 리스트 합치기
            
            print(f"   -> {len(chunk_data)}개 수집 완료.")
            time.sleep(0.5)

        except Exception as e:
            print(f"❌ {s_str}~{e_str} 구간 에러 발생: {e}")

        # 다음 구간 시작일 = 이번 구간 종료일 + 1일
        current_start = current_end + timedelta(days=1)

    # -----------------------------------------------------------
    # 데이터 통합 및 처리
    # -----------------------------------------------------------
    if total_data_list:
        print(f"   ✅ 총 {len(total_data_list)}건 수집 완료. 처리 시작...")
        # 여기서 insert_bigquery의 옵션은 상황에 따라 'replace' 또는 'append'
        # 보통 과거 데이터 재적재는 'append' 후 중복제거를 돌리는 게 안전합니다.
        process_and_upload(total_data_list, table_name, 'append')
    else:
        print("⚠️ 수집된 데이터가 없습니다.")    
 

def link_meta_daily(account_info):
    ad_account_id = account_info['id']      # config에서 가져온 계정 ID
    table_name = account_info['bq_table_name'] # config에서 가져온 테이블명
    hospital_name = CONFIG['hospital_name']
    my_access_token = get_token(account_info['name'])

    # 1. 페이스북 연결
    try:
        FacebookAdsApi.init(access_token=my_access_token)
    except Exception as e:
        print("❌ 페이스북 토큰 인증 실패:", e)
        return
    
    fields = ['campaign_name', 'ad_name', 'ad_id', 'spend', 'impressions', 'clicks', 'actions']

    params = {
        # 'level': 'campaign',      # 캠페인 -> 폴더 
        'level': 'ad',              # 광고 -> 파일
        'date_preset': 'yesterday',
        'limit': '500', # 한 번에 500개씩 요청
    }

    # try:
    print("⏳ 데이터 요청 중...")
    data_cursor = AdAccount(ad_account_id).get_insights(fields=fields, params=params)
    process_and_upload(data_cursor, table_name, 'append')

    # except Exception as e:
    #     print("❌ 연동 실패:", e)


def insert_bigquery(final_df, table_name, option):
    # -----------------------------------------------------------
    # 6. BigQuery로 전송 (업로드)
    # -----------------------------------------------------------
    destination_table = f"{DATASET_ID}.{table_name}"

    # try:
    # Config에 있는 키 파일 경로 사용
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    pandas_gbq.to_gbq(
        final_df, destination_table, project_id=PROJECT_ID,
        if_exists=option, credentials=credentials
    )
    print("🎉 BigQuery 저장 완료.")
    # except Exception as e:
    #     print("❌ 업로드 실패:", e)

# -----------------------------------------------------------
# 중복 제거 함수 (Config 사용)
# -----------------------------------------------------------
def remove_duplicates(table_name):
    
    try:
        credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        print(f"🧹 [{table_name}] 중복 데이터 청소 시작...")

        # ★ 핵심 SQL: 중복된 행 중 'collected_at'이 가장 최신인 것만 남기고 덮어쓰기
        query = f"""
                CREATE OR REPLACE TABLE `{table_id}`
                PARTITION BY report_date
                CLUSTER BY campaign_name
                AS
                SELECT * EXCEPT(rn)
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY report_date, campaign_name, ad_name, ad_id, channel
                            ORDER BY collected_at DESC
                        ) as rn
                    FROM `{table_id}`
                )
                WHERE rn = 1
                """
        
        query_job = client.query(query)  # 쿼리 실행
        query_job.result()  # 완료될 때까지 대기
        print(f"✨ [BigQuery] {table_name} 중복 제거 완료.")

    except Exception as e:
        print("❌ 중복 제거 실패:", e)

# -----------------------------------------------------------
# 시트 동기화 함수 (Config 사용)
# -----------------------------------------------------------
def sync_bq_to_sheet(table_name):
    # 테이블 이름에 따라 시트 탭 이름 자동 결정
    """
    구글 시트의 마지막 날짜를 확인하고, 그 이후의 데이터를 빅쿼리에서 가져와 추가합니다.
    """

    if 'beauty' in table_name:
        sheet_name = "B메타"
        category_name = "뷰티"
    elif 'foot' in table_name:
        sheet_name = "F메타"
        category_name = "풋"
    elif 'dosu' in table_name:
        sheet_name = "D메타"
        category_name = "도수"
    else:
        return
    
    # -------------------------------------------------------
    # 1. 구글 시트 연결 및 마지막 날짜 확인
    # -------------------------------------------------------
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_PATH, scope)
    client = gspread.authorize(creds)
    
    try:
        doc = client.open_by_url(SHEET_URL)
        worksheet = doc.worksheet(sheet_name)
    except Exception as e:
        print(f"⚠️ 시트 접속 실패 ({sheet_name}): {e}")
        return

    print(f"🧐 [{category_name}] 시트의 마지막 데이터를 확인하는 중... (탭 이름: {sheet_name})")
    
    date_values = worksheet.col_values(1)
    
    # 헤더(1행) 제외하고 날짜만 리스트로 변환
    valid_dates = []
    for d in date_values[1:]:
        try:
            # 1. 빈 값이나 공백 문자열이면 패스
            if not d or str(d).strip() == '': continue
            
            # 2. 날짜로 변환 시도
            ts = pd.to_datetime(d, errors='coerce') 
            
            # 3. [핵심] 변환 결과가 NaT(날짜 아님)이면 패스
            if pd.isna(ts):continue
            
            # 4. 정상 날짜면 리스트에 추가
            valid_dates.append(ts.date())
        except:
            continue # 날짜 아닌 값(빈칸 등)은 무시

    if not valid_dates:
        print(f"⚠️ [{category_name}] 시트에 날짜 데이터가 없습니다. 2024-01-01부터 가져옵니다.")
        target_start_date = '2024-01-01' # 기본 시작일 (적절히 수정)
    else:
        last_date = max(valid_dates)
        # 마지막 날짜 + 1일 (다음 날부터 가져오기 위함)
        target_start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"📅 시트 마지막 날짜: {last_date}")
        print(f"🚀 업데이트 시작일: {target_start_date} (이 날짜부터 가져옵니다)")

    # -------------------------------------------------------
    # 2. BigQuery에서 데이터 조회 (Last Date + 1 ~ )
    # -------------------------------------------------------
    bq_client = bigquery.Client.from_service_account_json(KEY_PATH)
    
    # 필요한 원본 컬럼만 불러옵니다
    query = f"""
            SELECT 
                report_date, campaign_name, ad_name, ad_id, result_type, leads, spend
            FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
            WHERE report_date >= '{target_start_date}'
            ORDER BY report_date ASC, campaign_name ASC
        """
    
    print("⏳ BigQuery 조회 중...")
    df = bq_client.query(query).to_dataframe()
    if df.empty:
        print("✨ 업데이트할 새로운 데이터가 없습니다. (최신 상태)")
        return

    # -------------------------------------------------------
    # 3. 데이터 조립 (일 | 캠페인 | 소재 | ID | 공란 | 결과 | CPA | 지출)
    # -------------------------------------------------------
    df['report_date'] = df['report_date'].astype(str)
    df['leads'] = df['leads'].fillna(0).astype(int)
    df['spend'] = df['spend'].fillna(0).astype(int)
    
    def calc_cpa(row):
        if row['leads'] > 0:
            return int(round(row['spend'] / row['leads']))
        return 0
        
    df['cpa'] = df.apply(calc_cpa, axis=1)

    data_to_append = []
    
    # iterrows를 사용하여 각 행을 직접 처리
    for index, row in df.iterrows():
        # Leads 처리: 0이면 빈 문자열(""), 아니면 숫자(int) 그대로 사용
        leads_val = int(row['leads'])
        leads_out = leads_val if leads_val > 0 else ""
        
        row_data = [
            str(row['report_date']),
            str(row['campaign_name']),
            str(row['ad_name']),
            str(row['ad_id']),
            str(row['result_type']),
            leads_out,          # 숫자 또는 빈칸
            int(row['cpa']),    # 숫자
            int(row['spend'])   # 숫자
        ]
        data_to_append.append(row_data)

    if data_to_append:
        print(f"👀 시트 전송 데이터 미리보기 (첫줄): {data_to_append[0]}")
    else:
        print("⚠️ 전송할 데이터 리스트가 비어있습니다!")
        return

    # -------------------------------------------------------
    # 4. 시트에 추가(빈 행을 찾아서 A열부터 강제로 집어넣기) 
    # -------------------------------------------------------
    # A열(1번째 열)의 데이터 개수를 세서, 그 다음 줄 번호를 찾음
    next_row = len(worksheet.col_values(1)) + 1
    end_row = next_row + len(data_to_append) - 1
    range_to_update = f"A{next_row}:H{end_row}"
    
    worksheet.update(
        range_name=range_to_update, 
        values=data_to_append, 
        value_input_option='USER_ENTERED'
    )
    
    print(f"✅ [{category_name}] 총 {len(data_to_append)}행 추가 완료! (범위: {range_to_update})")

# -----------------------------------------------------------
# 시트 중복 제거 함수 (Config 사용)
# -----------------------------------------------------------
def clean_sheet_duplicates(table_name):
    """
    시트의 데이터를 몽땅 읽어와서 [일(A열) + 광고소재ID(D열)] 기준으로 중복을 제거하고 다시 씁니다.
    """
    # 시트 탭 설정 (기존과 동일)
    if 'beauty' in table_name:
        sheet_name = "B메타"
        category_name = "뷰티"
    elif 'foot' in table_name:
        sheet_name = "F메타"
        category_name = "풋"
    elif 'dosu' in table_name:
        sheet_name = "D메타"
        category_name = "도수"
    else:
        return

    try:
        print(f"🧹 [{category_name}] 시트 자체 중복 제거 시작...")
        
        # 1. 인증 및 시트 열기
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_PATH, scope)
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        worksheet = doc.worksheet(sheet_name)

        # 2. 모든 데이터 읽기
        all_values = worksheet.get_all_values()
        if not all_values:
            print("데이터가 없습니다.")
            return

        header = all_values[0] # 헤더
        data = all_values[1:]  # 본문
        if not data: return
        
        initial_count = len(data)
        # 3. Pandas로 변환하여 중복 제거
        df = pd.DataFrame(data, columns=header)
        df_clean = df.drop_duplicates() 
        final_count = len(df_clean)

        if initial_count == final_count:
            print(f"✨ [{category_name}] 중복된 데이터가 없습니다. (변동 없음)")
            return

        # 4. 시트 클리어 후 다시 쓰기
        print(f"🗑️ 중복 {initial_count - final_count}건 발견! 안전하게 덮어쓰기를 진행합니다.")
        
        clean_data_list = df_clean.values.tolist()
        range_to_update = f"A2"
        worksheet.update(
            range_name=range_to_update, 
            values=clean_data_list,
            value_input_option='USER_ENTERED'
        )

        if initial_count > final_count:
            # 지워야 할 시작 행 번호 (헤더 1줄 + 데이터 길이 + 1)
            start_row_to_clear = final_count + 2 
            # 넉넉하게 맨 끝까지 지우기
            worksheet.batch_clear([f"A{start_row_to_clear}:H{initial_count + 5}"])
            
        print(f"✅ [{sheet_name}] 정리 완료!")

    except Exception as e:
        print(f"❌ 시트 청소 실패: {e}")


#========================================================
if __name__ == "__main__":

    MODE = "DAILY"   # 매일 아침 자동 실행용 (어제 데이터)
    # MODE = "RANGE"   # 과거 데이터 한꺼번에 수집용 (기간 지정)

    # RANGE 모드일 때만 사용하는 날짜 (YYYYMMDD)
    START_DATE = "20250101"
    END_DATE = "20251221"
    print("=== 메타(Meta) 메타데이터 수집 및 구글 시트 동기화 ===")


    for account in CONFIG['ad_accounts']:
        print(f"=== [{account['name']}] 작업 시작 ===")

        # 1. 수집 (모드에 따라 다르게 실행)
        if MODE == "DAILY":
            link_meta_daily(account)
        elif MODE == "RANGE":
            link_meta_yearly(account, START_DATE, END_DATE)

        # 2. 적재 후처리 (중복제거, 시트연동)
        remove_duplicates(account['bq_table_name'])
        sync_bq_to_sheet(account['bq_table_name'])
        clean_sheet_duplicates(account['bq_table_name'])
        
        print("---------------------------------------")

    print("=== 모든 작업이 완료되었습니다 ===")
    
