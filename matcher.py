import math
import pandas as pd
import requests
from typing import Dict, Any, List, Tuple
from config import get_config
from store import get_soil_categories, get_usage_categories, get_soil_type_mapping

config = get_config()

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def get_real_distance(user_lat, user_lon, place_lat, place_lon):
    """
    Kakao Directions API를 사용하여 실제 도로거리를 계산합니다.
    실패 시 직선거리 * 1.4를 반환합니다.
    """
    try:
        # Kakao Directions API 호출
        url = "https://apis-navi.kakaomobility.com/v1/directions"
        headers = {
            "Authorization": f"KakaoAK {config.KAKAO_REST_API_KEY}",
            "Content-Type": "application/json"
        }
        
        params = {
            "origin": f"{user_lon},{user_lat}",
            "destination": f"{place_lon},{place_lat}",
            "priority": "RECOMMEND",
            "car_fuel": "GASOLINE",
            "car_hipass": False,
            "alternatives": False,
            "road_details": False
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=5)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('routes') and len(result['routes']) > 0:
                # 실제 도로거리 반환 (km)
                distance = result['routes'][0]['summary']['distance'] / 1000.0
                return distance
        
        # API 실패 시 직선거리 * 1.4 반환
        straight_distance = haversine_km(user_lat, user_lon, place_lat, place_lon)
        return straight_distance * 1.4
        
    except Exception as e:
        print(f"Directions API 오류: {e}")
        # 오류 시 직선거리 * 1.4 반환
        straight_distance = haversine_km(user_lat, user_lon, place_lat, place_lon)
        return straight_distance * 1.4

def geocode_user_address(address: str) -> tuple:
    """사용자 입력 주소를 카카오 API로 좌표 변환"""
    print(f"=== geocode_user_address 시작 ===")
    print(f"입력 주소: '{address}'")
    print(f"주소 타입: {type(address)}")
    print(f"주소 길이: {len(address) if address else 0}")
    
    if not address or (isinstance(address, str) and address.strip() == ""):
        print("주소가 비어있음 - 에러 발생")
        raise ValueError("구체적인 시/군/구를 입력해주세요. 예) '경기도 수원시'")
    
    # 광범위한 지역명 체크 (더 포괄적으로 확장)
    broad_regions = [
        "경기도", "서울시", "부산시", "대구시", "인천시", "광주시", "대전시", "울산시", "세종시", 
        "강원도", "충청북도", "충청남도", "전라북도", "전라남도", "경상북도", "경상남도", "제주도",
        "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주",
        "서울 근처", "경기도 근처", "부산 근처", "대구 근처", "인천 근처", "광주 근처", "대전 근처", "울산 근처", "세종 근처", 
        "강원 근처", "충북 근처", "충남 근처", "전북 근처", "전남 근처", "경북 근처", "경남 근처", "제주 근처"
    ]
    
    if address in broad_regions:
        # 더 구체적인 예시 제공
        if "서울" in address:
            example = "서울시 강남구"
        elif "경기" in address:
            example = "경기도 수원시"
        elif "부산" in address:
            example = "부산시 해운대구"
        elif "대구" in address:
            example = "대구시 수성구"
        elif "인천" in address:
            example = "인천시 연수구"
        elif "광주" in address:
            example = "광주시 서구"
        elif "대전" in address:
            example = "대전시 유성구"
        elif "울산" in address:
            example = "울산시 남구"
        elif "세종" in address:
            example = "세종시 조치원읍"
        elif "강원" in address:
            example = "강원도 춘천시"
        elif "충북" in address:
            example = "충청북도 청주시"
        elif "충남" in address:
            example = "충청남도 천안시"
        elif "전북" in address:
            example = "전라북도 전주시"
        elif "전남" in address:
            example = "전라남도 목포시"
        elif "경북" in address:
            example = "경상북도 포항시"
        elif "경남" in address:
            example = "경상남도 창원시"
        elif "제주" in address:
            example = "제주도 제주시"
        else:
            example = "구체적인 시/군/구"
            
        raise ValueError(f"'{address}'는 너무 광범위한 지역명입니다. 구체적인 시/군/구를 입력해주세요. 예) '{example}'")
    
    api_key = config.KAKAO_REST_API_KEY
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {'Authorization': f'KakaoAK {api_key}'}
    params = {'query': address}
    
    try:
        print(f"카카오 API 호출: {address}")
        print(f"API 키: {api_key[:10]}...")
        
        response = requests.get(url, headers=headers, params=params)
        print(f"응답 상태: {response.status_code}")
        
        data = response.json()
        print(f"API 응답: {data}")
        
        if data.get('documents'):
            location = data['documents'][0]['address']
            lat, lng = float(location['y']), float(location['x'])
            print(f"변환 성공: {lat}, {lng}")
            return lat, lng
        else:
            print(f"주소 변환 실패: {address} - 문서 없음")
            return config.DEFAULT_LAT, config.DEFAULT_LON
    except Exception as e:
        print(f"주소 변환 오류: {e}")
        return config.DEFAULT_LAT, config.DEFAULT_LON

def calculate_transport_cost(distance_km, volume_m3, cost_per_ton_km=123):
    """
    운송비를 계산합니다.
    
    Args:
        distance_km: 운송 거리 (km)
        volume_m3: 토사 용량 (m³)
        cost_per_ton_km: 1톤당 1km 운송비 (원) - 기본값 123원
    
    Returns:
        운송비 (원)
    """
    # None 값 처리
    if volume_m3 is None or distance_km is None:
        return 0
    
    # 토사 밀도 가정: 1.5톤/m³ (일반적인 토사 밀도)
    soil_density = 1.5  # 톤/m³
    total_weight = volume_m3 * soil_density  # 톤
    
    # 운송비 = 거리 × 무게 × 단위비용
    transport_cost = distance_km * total_weight * cost_per_ton_km
    
    return round(transport_cost)

def volume_fit_score(supply, demand):
    """
    용량 적합도 점수를 계산합니다.
    용량 조건을 만족하는 경우 모든 후보가 동일한 점수를 받습니다.
    """
    if supply is None or demand is None or max(supply, demand) == 0:
        return 0.0
    
    # 용량 조건을 만족하는 경우 (supply >= demand) 동일한 점수 부여
    if supply >= demand:
        return 1.0  # 모든 후보가 동일한 용량 점수
    
    # 용량 부족인 경우 (이미 필터링되어 도달하지 않음)
    return 0.0

def soil_match_score(supply_soil, demand_soil, purpose=None, usage=None):
    """
    토질 매칭 점수를 계산합니다.
    실제 토석공이스시스템 데이터 구조에 맞춰 개선되었습니다.
    """
    if not demand_soil or not supply_soil:
        return 0.5  # 미입력 시 중립
    
    # 기존 토질 분류를 새로운 분류로 매핑
    soil_mapping = get_soil_type_mapping()
    supply_soil = soil_mapping.get(supply_soil, supply_soil)
    demand_soil = soil_mapping.get(demand_soil, demand_soil)
    
    if supply_soil == demand_soil:
        return 1.0
    
    # 실제 토석공이스시스템 용도별 토질 선호도
    usage_preferences = {
        "매립용": {"사토": 0.9, "순성토": 0.95, "리핑암": 0.4, "발파암": 0.3, "풍화암": 0.6},
        "되메우기용": {"사토": 0.95, "순성토": 0.9, "리핑암": 0.7, "발파암": 0.6, "풍화암": 0.8},
        "조경식재용": {"사토": 0.6, "순성토": 0.9, "리핑암": 0.5, "발파암": 0.4, "풍화암": 0.85},
        "구조물되메우기용": {"사토": 0.9, "순성토": 0.7, "리핑암": 0.95, "발파암": 0.9, "풍화암": 0.6},
        "도로성토용": {"사토": 0.95, "순성토": 0.6, "리핑암": 0.9, "발파암": 0.95, "풍화암": 0.7},
        "기타유용": {"사토": 0.8, "순성토": 0.8, "리핑암": 0.7, "발파암": 0.6, "풍화암": 0.7}
    }
    
    # 토질 유사성 매트릭스 (실제 분류 기반)
    soil_similarity = {
        ("사토", "순성토"): 0.7, ("순성토", "사토"): 0.7,
        ("사토", "리핑암"): 0.5, ("리핑암", "사토"): 0.5,
        ("사토", "발파암"): 0.4, ("발파암", "사토"): 0.4,
        ("사토", "풍화암"): 0.6, ("풍화암", "사토"): 0.6,
        ("순성토", "리핑암"): 0.3, ("리핑암", "순성토"): 0.3,
        ("순성토", "발파암"): 0.2, ("발파암", "순성토"): 0.2,
        ("순성토", "풍화암"): 0.8, ("풍화암", "순성토"): 0.8,
        ("리핑암", "발파암"): 0.9, ("발파암", "리핑암"): 0.9,
        ("리핑암", "풍화암"): 0.6, ("풍화암", "리핑암"): 0.6,
        ("발파암", "풍화암"): 0.5, ("풍화암", "발파암"): 0.5
    }
    
    # 기본 유사성 점수
    base_score = soil_similarity.get((supply_soil, demand_soil), 0.0)
    
    # 용도별 가중치 적용 (usage 우선, purpose는 fallback)
    target_usage = usage or purpose
    if target_usage and target_usage in usage_preferences:
        usage_weight = usage_preferences[target_usage].get(supply_soil, 0.5)
        # 유사성과 용도 적합성을 종합
        return (base_score * 0.6 + usage_weight * 0.4)
    
    return base_score

def score_candidate(distance_km, supply_volume, demand_volume, supply_soil, demand_soil, 
                   access_ok=True, deadline_penalty=False, purpose=None, urgency=None, usage=None, progress_ratio=None):
    """
    후보 점수를 계산합니다.
    용량 조건을 우선순위로 설정하여 수정되었습니다.
    """
    # 용량 조건 우선순위 설정
    if supply_volume < demand_volume:
        return 0.0  # 요청 용량 미달 시 0점
    
    # 용량 조건을 만족하는 경우에만 점수 계산
    # 거리 우선 가중치 (거리 > 용량 > 토질 > 접근성)
    base_weights = {
        "dist": 0.5,  # 거리 가중치 증가 (가까운 곳 우선)
        "vol": 0.3,   # 용량 가중치 감소
        "soil": 0.15, # 토질 가중치
        "acc": 0.05   # 접근성 가중치
    }
    
    # 긴급도별 가중치 조정
    if urgency == "긴급":
        base_weights = {"dist": 0.3, "vol": 0.4, "soil": 0.2, "acc": 0.1}  # 긴급시 용량 우선
    elif urgency == "여유":
        base_weights = {"dist": 0.6, "vol": 0.2, "soil": 0.15, "acc": 0.05}  # 여유시 거리 최우선
    
    # 거리 점수 계산 (200km 기준으로 축소)
    Dmax = 200.0  # 200km로 축소 (더 가까운 곳 우선)
    dist_term = 1.0 - min(distance_km / Dmax, 1.0) if distance_km is not None else 0.5
    
    # 물량 적합도 점수 (용량 초과 보너스 제거)
    vol_term = volume_fit_score(supply_volume, demand_volume)
    
    # 토질 매칭 점수 (usage 우선, purpose는 fallback)
    soil_term = soil_match_score(supply_soil, demand_soil, purpose, usage)
    
    # 접근성 점수
    acc_term = 1.0 if access_ok else 0.0
    
    # 기본 점수 계산
    base = 100.0 * (base_weights["dist"]*dist_term + base_weights["vol"]*vol_term + 
                   base_weights["soil"]*soil_term + base_weights["acc"]*acc_term)
    
    # 긴급도 보너스
    urgency_bonus = 0
    if urgency == "긴급":
        urgency_bonus = 5.0
    elif urgency == "급함":
        urgency_bonus = 3.0
    
    # 진행률은 점수 산정에서 제외 (사용자와 무관한 요소)
    progress_bonus = 0
    
    # 페널티
    penalty = 10.0 if deadline_penalty else 0.0
    
    return max(0.0, base + urgency_bonus + progress_bonus - penalty)

def get_default_volume_by_purpose(purpose: str) -> int:
    """
    목적별 기본 용량을 반환합니다.
    
    Args:
        purpose: 용도 (농업, 조경, 복구, 건설 등)
    
    Returns:
        기본 용량 (m³)
    """
    purpose_defaults = {
        "농업": 30,
        "조경": 20, 
        "복구": 100,
        "건설": 200,
        # 기타 용도들을 4가지 기본 용도로 매핑
        "매립": 200,  # 건설용으로 매핑
        "되메우기": 100,  # 복구용으로 매핑
        "기초공사": 200,  # 건설용으로 매핑
        "도로공사": 200,  # 건설용으로 매핑
        "하천정비": 100,  # 복구용으로 매핑
        "산사태복구": 100,  # 복구용으로 매핑
    }
    
    # 기본값: 건설용 (가장 큰 용량)
    return purpose_defaults.get(purpose, 200)

def rank_candidates(entities: dict, candidates_df: pd.DataFrame):
    """
    토사 후보들을 랭킹합니다.
    경유지 조건 제거, 500km 반경, 용량 우선순위 적용
    
    Args:
        entities: {"region","volume_m3","soil_type","distance_km","date","purpose","urgency","usage"}
        candidates_df: columns = name,type,lat,lon,volume_m3,soil_type,usage,address
    """
    try:
        print(f"=== rank_candidates 시작 ===")
        print(f"입력 entities: {entities}")
        print(f"candidates_df 크기: {len(candidates_df)}")
        
        # 사용자 입력 주소를 좌표로 변환
        user_address = entities.get("region", "")
        print(f"사용자 주소: {user_address}")
        ref_lat, ref_lon = geocode_user_address(user_address)
        print(f"변환된 좌표: {ref_lat}, {ref_lon}")
    except Exception as e:
        print(f"=== rank_candidates 초기 단계 에러 ===")
        print(f"에러 타입: {type(e).__name__}")
        print(f"에러 메시지: {str(e)}")
        import traceback
        print(f"에러 위치: {traceback.format_exc()}")
        raise
    
    # 요청 정보 추출
    demand_volume = entities.get("volume_m3")
    demand_soil = entities.get("soil_type")
    purpose = entities.get("purpose")
    urgency = entities.get("urgency")
    usage = entities.get("usage")

    # 용량이 없으면 목적별 기본값 적용
    applied_defaults = []
    if demand_volume is None and purpose:
        demand_volume = get_default_volume_by_purpose(purpose)
        applied_defaults.append(f"용량: {demand_volume}m³ ({purpose}용 기본값 적용)")
    elif demand_volume is None:
        demand_volume = 200  # 기본값: 건설용
        applied_defaults.append(f"용량: {demand_volume}m³ (기본값 적용)")

    print(f"요청 정보: 용량={demand_volume}m³, 토질={demand_soil}, 용도={usage}")
    if applied_defaults:
        print(f"기본값 적용: {', '.join(applied_defaults)}")
    
    # 1단계: 직선거리로 1차 필터링 (빠른 처리)
    candidates_filtered = []
    print(f"총 후보 데이터: {len(candidates_df)}개")
    
    for _, row in candidates_df.iterrows():
        # 공급처만 랭킹 (공급→수요 매칭)
        inout_type = row.get("inout_type")
        inout_status = row.get("inout_status", "")
        
        # 공급처 필터링 (supply 타입만 처리)
        if inout_type != "supply":
            continue
            
        # 직선거리 계산 (1차 필터링)
        straight_dist = haversine_km(ref_lat, ref_lon, float(row["lat"]), float(row["lon"]))
        
        # 200km 직선거리 제한 (1차 필터링)
        if straight_dist > 200.0:
            continue
            
        # 현재 용량 확인
        current_volume = row.get("current_volume_today", 0) or 0
        
        # 용량 조건 확인 (요청 용량 이상인 곳만)
        if current_volume < demand_volume:
            continue
            
        candidates_filtered.append(row)
    
    print(f"1차 필터링 후: {len(candidates_filtered)}개")
    
    # 직선거리 기준으로 정렬하여 상위 10개만 실제 도로거리 계산
    candidates_filtered.sort(key=lambda x: haversine_km(ref_lat, ref_lon, float(x["lat"]), float(x["lon"])))
    candidates_filtered = candidates_filtered[:10]
    print(f"직선거리 상위 10개 → 실제 도로거리 계산 대상: {len(candidates_filtered)}개")
    
    # 2단계: 실제 도로거리로 정확한 매칭
    rows = []
    for i, row in enumerate(candidates_filtered):
        print(f"실제 거리 계산 중... ({i+1}/{len(candidates_filtered)})")
        
        # 실제 도로거리 계산
        real_dist = get_real_distance(ref_lat, ref_lon, float(row["lat"]), float(row["lon"]))
        
        # 500km 실제 도로거리 제한 (원래 설정으로 복원)
        if real_dist > 500.0:
            continue
            
        current_volume = row.get("current_volume_today", 0) or 0
        
        # 점수 계산 (실제 도로거리 사용)
        score = score_candidate(
            real_dist, 
            current_volume,
            demand_volume, 
            row.get("soil_type"), 
            demand_soil,
            access_ok=True, 
            deadline_penalty=False, 
            purpose=purpose, 
            urgency=urgency,
            usage=usage or row.get("usage"),
            progress_ratio=row.get("progress_ratio_today")
        )
        
        # 운송비 계산 (실제 도로거리 사용) - 요청량 기준으로 계산
        transport_cost = calculate_transport_cost(real_dist, demand_volume)
        
        rows.append({
            "name": row.get("name"),
            "distance_km": round(real_dist, 1),
            "capacity_m3": int(row.get("volume_m3") or 0),
            "current_capacity_m3": int(current_volume),
            "soil_type": row.get("soil_type") or "불명",
            "type": row.get("type") or "불명",
            "usage": row.get("usage") or "불명",
            "address": row.get("address", ""),
            "progress_ratio": round(row.get("progress_ratio_today", 0), 3),
            "transport_cost": transport_cost,
            "score": round(score, 1),
            "lat": row.get("lat"),
            "lng": row.get("lon")
        })
    
    # 같은 공급지(name) 중복 제거
    seen_suppliers = set()
    filtered_rows = []
    
    for row in rows:
        supplier_name = row.get("name")
        if supplier_name not in seen_suppliers:
            seen_suppliers.add(supplier_name)
            filtered_rows.append(row)
        else:
            # 같은 공급지가 이미 있으면 더 좋은 점수인지 확인
            existing_index = next(i for i, r in enumerate(filtered_rows) if r.get("name") == supplier_name)
            if row.get("score", 0) > filtered_rows[existing_index].get("score", 0):
                filtered_rows[existing_index] = row  # 더 좋은 점수로 교체
    
    # 점수순 정렬 및 상위 3개만 반환
    out = pd.DataFrame(filtered_rows).sort_values("score", ascending=False).head(3).reset_index(drop=True)

    # 요약 생성
    prefs = []
    
    # 용량 우선순위 표시
    prefs.append(f"용량 {demand_volume}m³ 이상 공급처 우선")
    prefs.append("직선거리 상위 10개 → 실제 도로거리 3순위 매칭")
    
    # 긴급도별 우선순위 표시
    if urgency == "긴급":
        prefs.append("긴급 요청: 용량/토질 적합도 최우선")
    elif urgency == "급함":
        prefs.append("급한 요청: 용량/토질 적합도 중시")
    elif urgency == "여유":
        prefs.append("여유 요청: 거리 최우선 매칭")
    else:
        prefs.append("실제 도로거리 우선/용량/토질 종합 점수 순")
    
    # 용도별 특화 표시
    target_usage = usage or purpose
    if target_usage:
        usage_names = {
            "매립용": "매립용", "되메우기용": "되메우기용", "조경식재용": "조경용",
            "구조물되메우기용": "구조물용", "도로성토용": "도로용", "기타유용": "기타용도"
        }
        usage_name = usage_names.get(target_usage, target_usage)
        prefs.append(f"{usage_name} 토질 선호도 반영")
    elif demand_soil:
        prefs.append(f"토질 '{demand_soil}' 선호 반영")
    
    summary = prefs[:3]
    
    # 기본값 적용 내역을 summary에 추가
    if applied_defaults:
        summary.insert(0, f"[기본값 적용] {', '.join(applied_defaults)}")
    
    return out, summary, applied_defaults
