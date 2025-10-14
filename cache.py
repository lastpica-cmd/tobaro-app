import hashlib
import json
import time
from typing import Any, Optional, Dict
from config import get_config

config = get_config()

class SimpleCache:
    """간단한 메모리 캐시 시스템"""
    
    def __init__(self):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = config.CACHE_SIZE
        self.duration = config.CACHE_DURATION
    
    def _generate_key(self, data: Any) -> str:
        """데이터를 기반으로 캐시 키 생성"""
        if isinstance(data, dict):
            # 딕셔너리를 정렬된 문자열로 변환
            sorted_data = json.dumps(data, sort_keys=True)
        else:
            sorted_data = str(data)
        
        return hashlib.md5(sorted_data.encode()).hexdigest()
    
    def _is_expired(self, timestamp: float) -> bool:
        """캐시가 만료되었는지 확인"""
        return time.time() - timestamp > self.duration
    
    def _cleanup_expired(self):
        """만료된 캐시 항목들 정리"""
        current_time = time.time()
        expired_keys = [
            key for key, value in self.cache.items()
            if current_time - value['timestamp'] > self.duration
        ]
        for key in expired_keys:
            del self.cache[key]
    
    def _cleanup_oldest(self):
        """가장 오래된 캐시 항목들 정리 (크기 제한)"""
        if len(self.cache) >= self.max_size:
            # 가장 오래된 항목부터 제거
            sorted_items = sorted(
                self.cache.items(),
                key=lambda x: x[1]['timestamp']
            )
            # 20% 제거
            remove_count = max(1, len(sorted_items) // 5)
            for key, _ in sorted_items[:remove_count]:
                del self.cache[key]
    
    def get(self, key: str) -> Optional[Any]:
        """캐시에서 값 가져오기"""
        if not config.USE_CACHE:
            return None
            
        if key in self.cache:
            cache_item = self.cache[key]
            if not self._is_expired(cache_item['timestamp']):
                return cache_item['value']
            else:
                # 만료된 항목 제거
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Any) -> None:
        """캐시에 값 저장하기"""
        if not config.USE_CACHE:
            return
            
        self._cleanup_expired()
        self._cleanup_oldest()
        
        self.cache[key] = {
            'value': value,
            'timestamp': time.time()
        }
    
    def get_or_set(self, key: str, func, *args, **kwargs) -> Any:
        """캐시에서 가져오거나 함수 실행 후 캐시에 저장"""
        cached_value = self.get(key)
        if cached_value is not None:
            return cached_value
        
        # 캐시에 없으면 함수 실행
        result = func(*args, **kwargs)
        self.set(key, result)
        return result
    
    def clear(self) -> None:
        """캐시 전체 삭제"""
        self.cache.clear()
    
    def stats(self) -> Dict[str, Any]:
        """캐시 통계 반환"""
        current_time = time.time()
        active_count = sum(
            1 for item in self.cache.values()
            if not self._is_expired(item['timestamp'])
        )
        
        return {
            'total_items': len(self.cache),
            'active_items': active_count,
            'expired_items': len(self.cache) - active_count,
            'max_size': self.max_size,
            'duration': self.duration
        }

# 전역 캐시 인스턴스
cache = SimpleCache()

def cache_route_result(text: str, route_result: Any) -> None:
    """라우팅 결과를 캐시에 저장"""
    cache_key = f"route:{cache._generate_key(text)}"
    cache.set(cache_key, route_result)

def get_cached_route_result(text: str) -> Optional[Any]:
    """캐시에서 라우팅 결과 가져오기"""
    cache_key = f"route:{cache._generate_key(text)}"
    return cache.get(cache_key)

def cache_matching_result(entities: dict, candidates: Any, result: Any) -> None:
    """매칭 결과를 캐시에 저장"""
    cache_key = f"match:{cache._generate_key({'entities': entities, 'candidates_hash': hash(str(candidates))})}"
    cache.set(cache_key, result)

def get_cached_matching_result(entities: dict, candidates: Any) -> Optional[Any]:
    """캐시에서 매칭 결과 가져오기"""
    cache_key = f"match:{cache._generate_key({'entities': entities, 'candidates_hash': hash(str(candidates))})}"
    return cache.get(cache_key)
