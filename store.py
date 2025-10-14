import pandas as pd
import math
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from config import get_config
from supabase import create_client, Client

config = get_config()

def calculate_progress_ratio(row) -> float:
    """
    실시간 진행률을 계산합니다.
    엑셀 수식과 동일한 로직을 구현합니다.
    """
    try:
        today = datetime.now().date()
        start_date = pd.to_datetime(row['occurrence_start']).date()
        end_date = pd.to_datetime(row['occurrence_end']).date()
        duration_days = row['duration_days']
        rate_model = row['rate_model_code']
        
        # 기본 진행률 계산
        if today < start_date:
            base_progress = 0.0
        elif today > end_date:
            base_progress = 1.0
        else:
            base_progress = (today - start_date).days / duration_days
        
        # 모델별 진행률 적용
        if rate_model == 1:  # Front-loaded (전진형)
            return math.sqrt(base_progress)
        elif rate_model == 2:  # Uniform (균등형)
            return base_progress
        else:  # Back-loaded (후진형) - rate_model == 3 또는 기타
            return 1 / (1 + math.exp(-(base_progress * 12 - 6)))
            
    except Exception as e:
        print(f"진행률 계산 오류: {e}")
        return 0.0

def load_candidates() -> pd.DataFrame:
    """
    토사 후보 데이터를 로드합니다.
    Supabase를 사용하여 데이터를 조회합니다.
    """
    # Supabase 설정이 있으면 Supabase 사용, 없으면 기존 CSV 방식 사용
    if config.SUPABASE_URL and config.SUPABASE_KEY:
        return _load_supabase_data()
    elif config.USE_REAL_DATA and config.REAL_DATA_PATH.exists():
        return _load_real_data()
    elif config.SAMPLE_DATA_PATH.exists():
        return _load_sample_data()
    else:
        raise FileNotFoundError(f"데이터 파일이 없습니다: {config.REAL_DATA_PATH} 또는 {config.SAMPLE_DATA_PATH}")

def _load_supabase_data() -> pd.DataFrame:
    """Supabase에서 토석 데이터를 조회하여 매칭용으로 변환"""
    try:
        # Supabase 클라이언트 생성
        supabase: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
        
        # soil_data 테이블에서 모든 데이터 조회
        print("Supabase에서 토석 데이터 조회 중...")
        response = supabase.table('soil_data').select('*').execute()
        
        if not response.data:
            print("Supabase에서 데이터를 찾을 수 없습니다.")
            raise ValueError("Supabase에서 데이터를 찾을 수 없습니다.")
        
        # DataFrame으로 변환
        df = pd.DataFrame(response.data)
        print(f"Supabase 데이터 로드 성공: {len(df)}행, {len(df.columns)}열")
        print(f"컬럼명: {list(df.columns)}")
        
        # 필요한 컬럼만 선택하고 매칭용으로 변환 (좌표 포함)
        try:
            processed = df[['project_id', 'project_name', 'soil_category', 'type', 'subtype', 
                           'usage', 'total_volume_m3', 'inout_status', 'address', 
                           'occurrence_start', 'occurrence_end', 'duration_days', 'rate_model_code',
                           'lat', 'lng']].copy()
            print("컬럼 선택 성공")
        except KeyError as e:
            print(f"컬럼 선택 실패: {e}")
            print("사용 가능한 컬럼들:")
            for col in df.columns:
                print(f"  - {col}")
            raise
        
        # 실시간 진행률 계산
        processed['progress_ratio_today'] = processed.apply(calculate_progress_ratio, axis=1)
        
        # 오늘 기준 토석량 계산 (진행률 * 총량)
        processed['current_volume_today'] = processed['progress_ratio_today'] * processed['total_volume_m3']
        
        # 매칭용 컬럼으로 변환
        processed['name'] = processed['project_name']
        processed['inout_type'] = processed['inout_status'].apply(_convert_inout_to_type)  # 공급/수요 구분용
        processed['volume_m3'] = processed['total_volume_m3']
        processed['soil_type'] = processed['soil_category']  # 사토, 순성토 등
        processed['usage'] = processed['usage']  # 매립용, 되메우기용 등
        # type 컬럼은 원본 그대로 유지 (토사, 리핑암, 발파암)
        
        # 실제 좌표 사용 (NaN 값 처리)
        processed['lon'] = processed['lng']  # lng를 lon으로 변환
        
        # NaN 값 처리
        processed['lat'] = processed['lat'].fillna(config.DEFAULT_LAT)
        processed['lon'] = processed['lon'].fillna(config.DEFAULT_LON)
        processed['volume_m3'] = processed['volume_m3'].fillna(0)
        processed['progress_ratio_today'] = processed['progress_ratio_today'].fillna(0)
        processed['current_volume_today'] = processed['current_volume_today'].fillna(0)
        
        # 필수 컬럼만 반환 (project_id 포함, 진행률 포함, 오늘 기준 토석량 추가, inout_status 추가, inout_type 추가)
        return processed[['project_id', 'name', 'type', 'inout_type', 'lat', 'lon', 'volume_m3', 'soil_type', 'usage', 'address', 'progress_ratio_today', 'current_volume_today', 'inout_status']]
        
    except Exception as e:
        print(f"Supabase 데이터 로드 오류: {e}")
        # Supabase 실패 시 기존 CSV 방식으로 폴백
        print("Supabase 실패, CSV 방식으로 폴백...")
        if config.USE_REAL_DATA and config.REAL_DATA_PATH.exists():
            return _load_real_data()
        elif config.SAMPLE_DATA_PATH.exists():
            return _load_sample_data()
        else:
            raise

def _load_real_data() -> pd.DataFrame:
    """실제 토석공이스시스템 데이터를 매칭용으로 변환"""
    # 탭 구분자와 인코딩 문제 해결
    encodings = ['utf-8-sig', 'cp949', 'euc-kr', 'utf-8']
    df = None
    for encoding in encodings:
        try:
            df = pd.read_csv(config.REAL_DATA_PATH, encoding=encoding)
            print(f"CSV 읽기 성공: {encoding} 인코딩, 탭 구분자")
            break
        except Exception as e:
            print(f"CSV 읽기 실패: {encoding} - {e}")
            continue
    
    if df is None:
        raise ValueError("모든 인코딩으로 CSV 읽기 실패!")
    
    print(f"CSV 로드 성공: {len(df)}행, {len(df.columns)}열")
    print(f"컬럼명: {list(df.columns)}")
    
    # 필요한 컬럼만 선택하고 매칭용으로 변환 (좌표 포함)
    try:
        processed = df[['project_id', 'project_name', 'soil_category', 'type', 'subtype', 
                       'usage', 'total_volume_m3', 'inout_status', 'address', 
                       'occurrence_start', 'occurrence_end', 'duration_days', 'rate_model_code',
                       'lat', 'lng']].copy()
        print("컬럼 선택 성공")
    except KeyError as e:
        print(f"컬럼 선택 실패: {e}")
        print("사용 가능한 컬럼들:")
        for col in df.columns:
            print(f"  - {col}")
        raise
    
    # 실시간 진행률 계산
    processed['progress_ratio_today'] = processed.apply(calculate_progress_ratio, axis=1)
    
    # 오늘 기준 토석량 계산 (진행률 * 총량)
    processed['current_volume_today'] = processed['progress_ratio_today'] * processed['total_volume_m3']
    
    # 매칭용 컬럼으로 변환
    processed['name'] = processed['project_name']
    processed['inout_type'] = processed['inout_status'].apply(_convert_inout_to_type)  # 공급/수요 구분용
    processed['volume_m3'] = processed['total_volume_m3']
    processed['soil_type'] = processed['soil_category']  # 사토, 순성토 등
    processed['usage'] = processed['usage']  # 매립용, 되메우기용 등
    # type 컬럼은 원본 그대로 유지 (토사, 리핑암, 발파암)
    
    # 실제 좌표 사용 (NaN 값 처리)
    processed['lon'] = processed['lng']  # lng를 lon으로 변환
    
    # NaN 값 처리
    processed['lat'] = processed['lat'].fillna(config.DEFAULT_LAT)
    processed['lon'] = processed['lon'].fillna(config.DEFAULT_LON)
    processed['volume_m3'] = processed['volume_m3'].fillna(0)
    processed['progress_ratio_today'] = processed['progress_ratio_today'].fillna(0)
    processed['current_volume_today'] = processed['current_volume_today'].fillna(0)
    
    # 필수 컬럼만 반환 (project_id 포함, 진행률 포함, 오늘 기준 토석량 추가, inout_status 추가, inout_type 추가)
    return processed[['project_id', 'name', 'type', 'inout_type', 'lat', 'lon', 'volume_m3', 'soil_type', 'usage', 'address', 'progress_ratio_today', 'current_volume_today', 'inout_status']]

def _load_sample_data() -> pd.DataFrame:
    """기존 샘플 데이터 처리"""
    df = pd.read_csv(config.SAMPLE_DATA_PATH)
    need_cols = {"name","type","lat","lon","volume_m3","soil_type"}
    missing = need_cols - set(df.columns)
    if missing:
        raise ValueError(f"필수 컬럼 누락: {missing}")
    return df

def _convert_inout_to_type(inout_status: str) -> str:
    """반출입 상태를 공급/수요 타입으로 변환"""
    if pd.isna(inout_status):
        return 'unknown'
    elif '미반출' in str(inout_status):
        return 'supply'  # 공급처
    elif '미반입' in str(inout_status):
        return 'demand'  # 수요처
    else:
        return 'unknown'

def get_soil_categories() -> Dict[str, Dict[str, Any]]:
    """토질 분류 코드 반환"""
    return {
        "사토": {"code": 1, "description": "사질이 많은 토사", "preferences": ["건설", "도로공사"]},
        "순성토": {"code": 2, "description": "점토가 많은 토사", "preferences": ["농업", "조경"]},
        "리핑암": {"code": 3, "description": "쪼개진 암석", "preferences": ["복구", "건설"]},
        "발파암": {"code": 4, "description": "폭파된 암석", "preferences": ["복구", "건설"]},
        "풍화암": {"code": 5, "description": "풍화된 암석", "preferences": ["복구", "조경"]}
    }

def get_usage_categories() -> Dict[str, Dict[str, Any]]:
    """용도 분류 코드 반환"""
    return {
        "매립용": {"code": 4, "description": "매립에 사용되는 토사", "soil_preference": ["사토", "순성토"]},
        "되메우기용": {"code": 3, "description": "되메우기에 사용되는 토사", "soil_preference": ["사토", "순성토"]},
        "조경식재용": {"code": 6, "description": "조경 및 식재용 토사", "soil_preference": ["순성토", "풍화암"]},
        "구조물되메우기용": {"code": 1, "description": "구조물 되메우기용", "soil_preference": ["사토", "리핑암"]},
        "도로성토용": {"code": 2, "description": "도로 성토용", "soil_preference": ["사토", "발파암"]},
        "기타유용": {"code": 8, "description": "기타 용도", "soil_preference": ["사토", "순성토"]}
    }

def get_soil_type_mapping() -> Dict[str, str]:
    """기존 토질 분류를 새로운 분류로 매핑"""
    return {
        "점토": "순성토",
        "사질": "사토", 
        "자갈": "리핑암",
        "혼합": "사토",
        "황토": "순성토",
        "모래": "사토",
        "암석": "발파암"
    }
