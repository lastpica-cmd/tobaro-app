import os
from pathlib import Path
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

class Config:
    """기본 설정"""
    # 데이터 관련
    USE_REAL_DATA = True  # 실제 데이터 사용
    REAL_DATA_PATH = Path(__file__).parent / "soil_data_processed.csv"
    SAMPLE_DATA_PATH = Path(__file__).parent / "sample_data.csv"
    
    # 캐싱 관련
    USE_CACHE = True
    CACHE_DURATION = 3600  # 1시간 (초)
    CACHE_SIZE = 100  # 최대 캐시 항목 수
    
    # API 관련
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL = "gpt-3.5-turbo"
    OPENAI_MAX_TOKENS = 300
    OPENAI_TEMPERATURE = 0.1
    
    # Kakao Address Search API
    KAKAO_REST_API_KEY = os.getenv("KAKAO_REST_API_KEY", "f1158b2a7742bf8f2a92f7ca86eb0520")
    
    # Kakao Map API (JavaScript 키)
    KAKAO_MAP_API_KEY = os.getenv("KAKAO_MAP_API_KEY", "cc393f84546f58b913c7314ad5a9e445")
    
    # Supabase 설정
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    
    # 매칭 알고리즘 관련
    DEFAULT_DISTANCE_WEIGHT = 0.3  # 거리 가중치 감소
    DEFAULT_VOLUME_WEIGHT = 0.4    # 용량 가중치 증가
    DEFAULT_SOIL_WEIGHT = 0.2
    DEFAULT_ACCESS_WEIGHT = 0.1
    
    # 거리 제한 (km)
    MAX_DISTANCE_KM = 500.0
    
    # 신뢰도 임계값
    MIN_CONFIDENCE = 0.55
    HIGH_CONFIDENCE = 0.8
    
    # 기본 좌표 (서울 시청)
    DEFAULT_LAT = 37.5665
    DEFAULT_LON = 126.9780

class DevelopmentConfig(Config):
    """개발 환경 설정"""
    DEBUG = True
    USE_REAL_DATA = True  # 실제 좌표 데이터 사용
    USE_CACHE = True

class ProductionConfig(Config):
    """프로덕션 환경 설정"""
    DEBUG = False
    USE_REAL_DATA = True  # 프로덕션에서는 실제 데이터 사용
    USE_CACHE = True

# 환경별 설정 선택
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}

def get_config():
    """현재 환경에 맞는 설정 반환"""
    env = os.getenv('FLASK_ENV', 'default')
    return config[env]()
