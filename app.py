from flask import Flask, request, jsonify, render_template
from router import hybrid_route, RouteResult
from matcher import rank_candidates, geocode_user_address
from store import load_candidates, query_candidates_by_conditions
from cache import get_cached_route_result, cache_route_result, get_cached_matching_result, cache_matching_result
from config import get_config
import pandas as pd
import os
from supabase import create_client, Client

import requests

config = get_config()

def reverse_geocode(lat, lng):
    """좌표를 주소로 변환 (역지오코딩)"""
    try:
        url = "https://dapi.kakao.com/v2/local/geo/coord2address.json"
        headers = {
            "Authorization": f"KakaoAK {config.KAKAO_REST_API_KEY}"
        }
        params = {
            "x": lng,
            "y": lat,
            "input_coord": "WGS84"
        }
        
        print(f"역지오코딩 API 호출: {lng},{lat}")
        response = requests.get(url, headers=headers, params=params)
        print(f"역지오코딩 API 응답: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"역지오코딩 API 결과: {data}")
            
            if data.get('documents') and len(data['documents']) > 0:
                doc = data['documents'][0]
                address = doc.get('address', {})
                
                # 가장 구체적인 주소 구성
                region_1 = address.get('region_1depth_name', '')
                region_2 = address.get('region_2depth_name', '')
                region_3 = address.get('region_3depth_name', '')
                region_4 = address.get('region_4depth_name', '')
                
                # 구체적인 주소 조합
                if region_4:
                    detailed_address = f"{region_1} {region_2} {region_3} {region_4}"
                elif region_3:
                    detailed_address = f"{region_1} {region_2} {region_3}"
                else:
                    detailed_address = f"{region_1} {region_2}"
                
                print(f"역지오코딩 결과: {detailed_address}")
                return detailed_address
            else:
                print("역지오코딩 결과 없음")
                return None
        else:
            print(f"역지오코딩 API 오류: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"역지오코딩 오류: {e}")
        return None

app = Flask(__name__)

@app.route("/")
def index():
    """메인 웹 페이지 (지도 포함)"""
    return render_template('index_with_map.html', kakao_map_api_key=config.KAKAO_MAP_API_KEY)

@app.route("/simple")
def simple():
    """간단한 챗봇 페이지 (지도 없음)"""
    return render_template('index.html')

@app.post("/ask")
def ask():
    try:
        data = request.get_json(silent=True) or {}
        text = (data.get("text") or "").strip()
        
        if not text:
            return jsonify({
                "error": False,
                "followup": "토사 관련 질문을 입력해 주세요. 예) '예산군에서 제방 복구용 토석을 어디서 구할 수 있어?'",
                "examples": [
                    "예천군에서 농사용 흙 500m³ 필요해"
                ]
            })

        # 라우팅 처리 (캐시 확인)
        print(f"=== 라우팅 시작 ===")
        print(f"입력 텍스트: {text}")
        
        cached_route = get_cached_route_result(text)
        if cached_route:
            route: RouteResult = cached_route
            print("캐시된 라우팅 결과 사용")
        else:
            print("새로운 라우팅 실행 중...")
            route: RouteResult = hybrid_route(text)
            print(f"라우팅 결과: {route.intent}, 신뢰도: {route.confidence}")
            cache_route_result(text, route)

        # 신뢰도 낮거나 UNKNOWN → 폴백
        if route.intent == "UNKNOWN" or route.confidence < 0.55:
            missing = []
            if not route.entities.region:
                missing.append("지역")
            if route.entities.volume_m3 is None:
                missing.append("물량(m³)")
            if not route.entities.soil_type:
                missing.append("토질")
            
            missing_txt = ", ".join(missing) if missing else "핵심 정보"
            return jsonify({
                "error": False,
                "route": route.model_dump(),
                "followup": f"{missing_txt}를 더 구체적으로 알려주세요.",
                "suggestions": [
                    "지역: '예천군', '서울시 강남구' 등",
                    "물량: '500m³', '1000톤' 등", 
                    "토질: '점토', '자갈', '혼합' 등"
                ]
            })

        # MATCH_FIND 처리
        if route.intent == "MATCH_FIND":
            try:
                print(f"=== 매칭 요청 디버깅 ===")
                print(f"입력 텍스트: {text}")
                print(f"추출된 엔티티: {route.entities.model_dump()}")
                print(f"신뢰도: {route.confidence}")
                
                # 조건부 조회 시도
                entities = route.entities
                try:
                    candidates = query_candidates_by_conditions(
                        region=entities.region,
                        soil_type=entities.soil_type,
                        usage=entities.usage,
                        volume_m3=entities.volume_m3,
                        limit=50   # ULTRA EXTREME 메모리 절약을 위해 극단적으로 줄임 (100 → 50)
                    )
                    print(f"조건부 조회 완료: {len(candidates)}개")
                    
                    # 조건부 조회 결과가 적으면 전체 데이터 로드
                    if len(candidates) < 1:  # ULTRA EXTREME 적극적으로 조건부 조회 사용 (2 → 1)
                        print("조건부 조회 결과가 적어서 전체 데이터 로드...")
                        candidates = load_candidates()
                        print(f"전체 데이터 로드 완료: {len(candidates)}개")
                except Exception as e:
                    print(f"조건부 조회 실패, 전체 데이터 로드: {e}")
                    candidates = load_candidates()
                    print(f"후보 데이터 로드 완료: {len(candidates)}개")
                
                # 매칭 결과 캐시 확인
                cached_matching = get_cached_matching_result(route.entities.model_dump(), candidates)
                if cached_matching:
                    results_df, summary = cached_matching
                    applied_defaults = []  # 캐시된 결과에서는 기본값 적용 내역 없음
                else:
                    print("매칭 알고리즘 실행 중...")
                    
                    # 단순 랭킹 기반 매칭 실행
                    results_df, summary, applied_defaults = rank_candidates(route.entities.model_dump(), candidates)
                    print(f"매칭 결과: {len(results_df)}개 발견")
                    print(f"요약: {summary}")
                    print(f"기본값 적용: {applied_defaults}")
                    cache_matching_result(route.entities.model_dump(), candidates, (results_df, summary))
                
                if results_df.empty:
                    return jsonify({
                        "error": False,
                        "route": route.model_dump(),
                        "message": "조건에 맞는 토사 공급처를 찾을 수 없습니다.",
                        "suggestions": [
                            "지역 범위를 넓혀보세요",
                            "토질 조건을 완화해보세요",
                            "물량을 조정해보세요"
                        ]
                    })
                
                # 사용자 좌표도 함께 전송
                user_address = route.entities.region or ""
                user_lat, user_lon = geocode_user_address(user_address)
                
                # 출발지 좌표를 역지오코딩해서 구체적인 주소 얻기
                print(f"=== 역지오코딩 시작 ===")
                print(f"좌표: {user_lat}, {user_lon}")
                detailed_address = reverse_geocode(user_lat, user_lon)
                print(f"역지오코딩 결과: {detailed_address}")
                if not detailed_address:
                    detailed_address = f"{user_address} (대표좌표)"
                    print(f"역지오코딩 실패, 대체 주소 사용: {detailed_address}")
                print(f"=== 역지오코딩 완료 ===")
                
                # 출발지 정보를 더 구체적으로 제공
                origin_info = {
                    "address": user_address,
                    "lat": user_lat,
                    "lng": user_lon,
                    "detailed_address": detailed_address
                }
                
                return jsonify({
                    "error": False,
                    "route": route.model_dump(),
                    "table": results_df.to_dict(orient="records"),
                    "summary": summary,
                    "applied_defaults": applied_defaults,
                    "origin": origin_info,
                    "actions": ["조건추가", "담당자 연결", "지도보기", "상세정보"],
                    "user_location": {
                        "lat": user_lat,
                        "lng": user_lon,
                        "address": user_address
                    }
                })
            except Exception as e:
                import traceback
                print(f"=== 상세 에러 정보 ===")
                print(f"에러 타입: {type(e).__name__}")
                print(f"에러 메시지: {str(e)}")
                print(f"에러 위치: {traceback.format_exc()}")
                print(f"=== 에러 정보 끝 ===")
                
                # 지역 관련 오류 처리 (광범위한 지역명 + 지역 정보 없음)
                if "너무 광범위한 지역명" in str(e) or "구체적인 시/군/구를 입력해주세요" in str(e):
                    return jsonify({
                        "error": False,
                        "message": str(e),
                        "suggestions": [
                            "구체적인 시/군/구를 포함해주세요",
                            "예) '경기도 수원시', '서울시 강남구'"
                        ]
                    })
                
                return jsonify({
                    "error": True,
                    "message": f"데이터 처리 중 오류가 발생했습니다: {str(e)}",
                    "details": str(e)
                })

        # 기타 intent 처리
        return jsonify({
            "error": False,
            "route": route.model_dump(),
            "message": f"'{route.intent}' 기능은 준비 중입니다.",
            "available_features": ["토사 매칭 찾기"]
        })

    except Exception as e:
        import traceback
        error_msg = str(e)
        traceback_msg = traceback.format_exc()
        print(f"=== 에러 발생 ===")
        print(f"에러 메시지: {error_msg}")
        print(f"상세 에러: {traceback_msg}")
        
        return jsonify({
            "error": True,
            "message": f"서버 오류가 발생했습니다: {error_msg}",
            "details": str(e)
        }), 500

@app.get("/cache/stats")
def cache_stats():
    """캐시 통계 조회"""
    from cache import cache
    return jsonify({
        "cache_enabled": config.USE_CACHE,
        "stats": cache.stats()
    })

@app.post("/cache/clear")
def clear_cache():
    """캐시 초기화"""
    from cache import cache
    cache.clear()
    return jsonify({"message": "캐시가 초기화되었습니다."})

@app.get("/landuse")
def get_landuse_data():
    """토지 이용 현황 데이터 조회"""
    try:
        # Supabase 설정이 있으면 Supabase 사용, 없으면 CSV 방식 사용
        if config.SUPABASE_URL and config.SUPABASE_KEY:
            return _get_landuse_from_supabase()
        else:
            return _get_landuse_from_csv()
        
    except Exception as e:
        return jsonify({
            "error": True,
            "message": "토지 이용 현황 데이터를 처리하는 중 오류가 발생했습니다.",
            "details": str(e)
        }), 500

def _get_landuse_from_supabase():
    """Supabase에서 토지 이용 현황 데이터 조회"""
    try:
        # Supabase 클라이언트 생성
        supabase: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
        
        # landuse_data 테이블에서 모든 데이터 조회 (페이지네이션으로 전체 로드)
        print("Supabase에서 토지 이용 현황 데이터 조회 중...")
        all_data = []
        page_size = 1000
        offset = 0
        
        while True:
            response = supabase.table('landuse_data').select('*').range(offset, offset + page_size - 1).execute()
            if not response.data:
                break
            all_data.extend(response.data)
            if len(response.data) < page_size:
                break
            offset += page_size
        
        print(f"총 {len(all_data)}행 로드 완료")
        
        if not all_data:
            return jsonify({"error": True, "message": "토지 이용 현황 데이터를 찾을 수 없습니다."}), 404
        
        # DataFrame으로 변환
        df = pd.DataFrame(all_data)
        print(f"Supabase 토지 이용 현황 데이터 로드 성공: {len(df)}행")
        
        # 숫자 컬럼 정리
        numeric_columns = ['논', '밭', '과수', '초지', '임지', '합계']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 빈 값 처리
        df = df.dropna(subset=['구역단위1', '구역단위2'])
        
        # 시/도별 집계
        province_summary = df.groupby('구역단위1').agg({
            '논': 'sum',
            '밭': 'sum', 
            '과수': 'sum',
            '초지': 'sum',
            '임지': 'sum',
            '합계': 'sum'
        }).reset_index()
        
        # 시/군/구별 집계
        city_summary = df.groupby(['구역단위1', '구역단위2']).agg({
            '논': 'sum',
            '밭': 'sum',
            '과수': 'sum', 
            '초지': 'sum',
            '임지': 'sum',
            '합계': 'sum'
        }).reset_index()
        
        return jsonify({
            "error": False,
            "data": {
                "province": province_summary.to_dict(orient='records'),
                "city": city_summary.to_dict(orient='records'),
                "total_records": len(df)
            }
        })
        
    except Exception as e:
        print(f"Supabase 토지 이용 현황 조회 오류: {e}")
        # Supabase 실패 시 CSV 방식으로 폴백
        return _get_landuse_from_csv()

def _get_landuse_from_csv():
    """CSV 파일에서 토지 이용 현황 데이터 조회 (기존 방식)"""
    # landuse_data.csv 파일 경로
    landuse_file = os.path.join(os.path.dirname(__file__), 'landuse_data.csv')
    
    if not os.path.exists(landuse_file):
        return jsonify({"error": True, "message": "토지 이용 현황 데이터 파일을 찾을 수 없습니다."}), 404
    
    # CSV 파일 읽기
    df = pd.read_csv(landuse_file, encoding='utf-8')
    
    # 데이터 정리 (첫 번째 행은 헤더가 아님)
    df = df.iloc[1:]  # 첫 번째 행 제거
    df.columns = ['구역단위1', '구역단위2', '구역단위3', '구역단위4', '논', '밭', '과수', '초지', '임지', '합계', '검증용1', '검증용2']
    
    # 숫자 컬럼 정리
    numeric_columns = ['논', '밭', '과수', '초지', '임지', '합계']
    for col in numeric_columns:
        df[col] = df[col].astype(str).str.replace(',', '').str.replace('"', '').str.replace(' ', '')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 빈 값 처리
    df = df.dropna(subset=['구역단위1', '구역단위2'])
    
    # 시/도별 집계
    province_summary = df.groupby('구역단위1').agg({
        '논': 'sum',
        '밭': 'sum', 
        '과수': 'sum',
        '초지': 'sum',
        '임지': 'sum',
        '합계': 'sum'
    }).reset_index()
    
    # 시/군/구별 집계
    city_summary = df.groupby(['구역단위1', '구역단위2']).agg({
        '논': 'sum',
        '밭': 'sum',
        '과수': 'sum', 
        '초지': 'sum',
        '임지': 'sum',
        '합계': 'sum'
    }).reset_index()
    
    return jsonify({
        "error": False,
        "data": {
            "province": province_summary.to_dict(orient='records'),
            "city": city_summary.to_dict(orient='records'),
            "total_records": len(df)
        }
    })


@app.post("/directions")
def get_directions():
    """카카오모빌리티 Directions API를 통한 실제 도로 경로 조회"""
    try:
        data = request.get_json(silent=True) or {}
        origin_lat = data.get("origin_lat")
        origin_lng = data.get("origin_lng")
        dest_lat = data.get("dest_lat")
        dest_lng = data.get("dest_lng")
        
        if not all([origin_lat, origin_lng, dest_lat, dest_lng]):
            return jsonify({"error": True, "message": "좌표 정보가 부족합니다."}), 400
        
        # 카카오모빌리티 Directions REST API 호출
        url = "https://apis-navi.kakaomobility.com/v1/directions"
        headers = {
            "Authorization": f"KakaoAK {config.KAKAO_REST_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # origin, destination은 "경도,위도" 형식
        params = {
            "origin": f"{origin_lng},{origin_lat}",
            "destination": f"{dest_lng},{dest_lat}",
            "priority": "RECOMMEND",  # 최적 경로
            "car_fuel": "GASOLINE",
            "car_hipass": False,
            "alternatives": False,
            "road_details": False
        }
        
        print(f"Directions API 호출: {params['origin']} → {params['destination']}")
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            print(f"Directions API 성공: {response.status_code}")
            return jsonify({
                "error": False,
                "data": result
            })
        else:
            print(f"Directions API 실패: {response.status_code} - {response.text}")
            return jsonify({
                "error": True,
                "message": f"경로 조회 실패: {response.status_code}"
            }), response.status_code
            
    except Exception as e:
        print(f"Directions API 오류: {e}")
        return jsonify({
            "error": True,
            "message": f"경로 조회 중 오류 발생: {str(e)}"
        }), 500

@app.route("/landuse/<region>")
def get_landuse_by_region(region):
    """지역별 토지 이용 현황 조회"""
    try:
        print(f"=== 토지 이용 현황 API 디버깅 ===")
        print(f"요청 지역: {region}")
        
        # Supabase 설정이 있으면 Supabase 사용, 없으면 CSV 방식 사용
        if config.SUPABASE_URL and config.SUPABASE_KEY:
            return _get_landuse_by_region_from_supabase(region)
        else:
            return _get_landuse_by_region_from_csv(region)
        
    except Exception as e:
        print(f"토지 이용 현황 조회 오류: {e}")
        return jsonify({
            "error": True,
            "message": f"토지 이용 현황 조회 중 오류 발생: {str(e)}"
        }), 500

def _get_landuse_by_region_from_supabase(region):
    """Supabase에서 지역별 토지 이용 현황 조회"""
    try:
        # Supabase 클라이언트 생성
        supabase: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
        
        # 지역별 토지 이용 현황 데이터 직접 검색
        print(f"Supabase에서 '{region}' 지역 토지 이용 현황 데이터 조회 중...")
        
        # 지역명을 공백으로 분리
        region_parts = region.split()
        print(f"분리된 지역명: {region_parts}")
        
        # 단계별 검색 (가장 구체적인 것부터 시도)
        response_data = None
        
        # 1단계: 4단계 매칭 (도 시/군/구 읍/면/동 리/동)
        if len(region_parts) >= 4:
            print(f"4단계 매칭 시도: {region_parts}")
            response = supabase.table('landuse_data').select('*').eq('구역단위1', region_parts[0]).eq('구역단위2', region_parts[1]).eq('구역단위3', region_parts[2]).eq('구역단위4', region_parts[3]).execute()
            if response.data:
                response_data = response.data
                print(f"4단계 매칭 성공: {len(response_data)}개")
        
        # 2단계: 3단계 매칭 (도 시/군/구 읍/면/동)
        if not response_data and len(region_parts) >= 3:
            print(f"3단계 매칭 시도: {region_parts[:3]}")
            response = supabase.table('landuse_data').select('*').eq('구역단위1', region_parts[0]).eq('구역단위2', region_parts[1]).eq('구역단위3', region_parts[2]).execute()
            if response.data:
                response_data = response.data
                print(f"3단계 매칭 성공: {len(response_data)}개")
        
        # 3단계: 2단계 매칭 (도 시/군/구)
        if not response_data and len(region_parts) >= 2:
            print(f"2단계 매칭 시도: {region_parts[:2]}")
            response = supabase.table('landuse_data').select('*').eq('구역단위1', region_parts[0]).eq('구역단위2', region_parts[1]).execute()
            if response.data:
                response_data = response.data
                print(f"2단계 매칭 성공: {len(response_data)}개")
        
        # 4단계: 1단계 매칭 (도)
        if not response_data and len(region_parts) >= 1:
            print(f"1단계 매칭 시도: {region_parts[:1]}")
            response = supabase.table('landuse_data').select('*').eq('구역단위1', region_parts[0]).execute()
            if response.data:
                response_data = response.data
                print(f"1단계 매칭 성공: {len(response_data)}개")
        
        if not response_data:
            return jsonify({
                "error": False,
                "message": f"'{region}' 지역의 데이터를 찾을 수 없습니다.",
                "data": {
                    "논": 0, "밭": 0, "과수": 0, "초지": 0, "임지": 0, "합계": 0,
                    "논_비율": 0, "밭_비율": 0, "과수_비율": 0, "초지_비율": 0, "임지_비율": 0
                }
            })
        
        # DataFrame으로 변환
        df = pd.DataFrame(response_data)
        print(f"Supabase 토지 이용 현황 데이터 로드 성공: {len(df)}행")
        
        # 숫자 컬럼 정리
        numeric_columns = ['논', '밭', '과수', '초지', '임지', '합계']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 전체 합계 계산
        total_summary = {
            '논': int(df['논'].sum()),
            '밭': int(df['밭'].sum()),
            '과수': int(df['과수'].sum()),
            '초지': int(df['초지'].sum()),
            '임지': int(df['임지'].sum()),
            '합계': int(df['합계'].sum())
        }
        
        # 비율 계산
        total_area = total_summary['합계']
        if total_area > 0:
            논_비율 = round((total_summary['논'] / total_area) * 100, 1)
            밭_비율 = round((total_summary['밭'] / total_area) * 100, 1)
            과수_비율 = round((total_summary['과수'] / total_area) * 100, 1)
            초지_비율 = round((total_summary['초지'] / total_area) * 100, 1)
            임지_비율 = round((total_summary['임지'] / total_area) * 100, 1)
        else:
            논_비율 = 밭_비율 = 과수_비율 = 초지_비율 = 임지_비율 = 0
        
        result = {
            "error": False,
            "message": f"'{region}' 지역의 토지 이용 현황",
            "data": {
                "논": total_summary['논'],
                "밭": total_summary['밭'],
                "과수": total_summary['과수'],
                "초지": total_summary['초지'],
                "임지": total_summary['임지'],
                "합계": total_summary['합계'],
                "논_비율": 논_비율,
                "밭_비율": 밭_비율,
                "과수_비율": 과수_비율,
                "초지_비율": 초지_비율,
                "임지_비율": 임지_비율
            }
        }
        
        return jsonify(result)
        
    except Exception as e:
        print(f"Supabase 지역별 토지 이용 현황 조회 오류: {e}")
        # Supabase 실패 시 CSV 방식으로 폴백
        return _get_landuse_by_region_from_csv(region)

def _get_landuse_by_region_from_csv(region):
    """CSV 파일에서 지역별 토지 이용 현황 조회 (기존 방식)"""
    # landuse_data.csv 파일 로드 (첫 번째 행 건너뛰기)
    df = pd.read_csv('landuse_data.csv', encoding='utf-8-sig', skiprows=1)
    print(f"전체 데이터 행 수: {len(df)}")
    print(f"컬럼명: {list(df.columns)}")
    
    # 컬럼명 정리 (공백 제거)
    df.columns = df.columns.str.strip()
    print(f"정리된 컬럼명: {list(df.columns)}")
    
    # 모든 구역단위 컬럼의 공백 제거 (핵심 수정!)
    for col in ['구역단위1', '구역단위2', '구역단위3', '구역단위4']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            print(f"{col} 공백 제거 완료")
    
    # 구역단위4 데이터 정규화 (공란과 '-' 통일)
    if '구역단위4' in df.columns:
        df['구역단위4'] = df['구역단위4'].replace(['', '-', 'nan', 'NaN'], '')
    
    # 숫자 컬럼 정리 (공백, 쉼표, 따옴표 제거)
    numeric_columns = ['논', '밭', '과수', '초지', '임지', '합계']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').str.replace('"', '').str.replace(' ', '').str.replace('-', '0')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    print(f"데이터 샘플 (처음 3행):")
    print(df.head(3))
    
    # 구역단위4 데이터 샘플 확인
    if '구역단위4' in df.columns:
        print(f"구역단위4 샘플 데이터:")
        print(df['구역단위4'].value_counts().head(10))
    
    # 지역명을 공백으로 분리
    region_parts = region.split()
    print(f"분리된 지역명: {region_parts}")
    print(f"지역명 개수: {len(region_parts)}")
    
    # 단계별 필터링 (가장 구체적인 것부터 시도)
    filtered_df = None
    
    # 1단계: 구역단위4 (리/동) 매칭 시도
    if len(region_parts) >= 4 and region_parts[3]:
        print(f"구역단위4 매칭 시도: '{region_parts[3]}'")
        temp_df = df[df['구역단위4'] == region_parts[3]]
        if not temp_df.empty:
            filtered_df = temp_df
            print(f"구역단위4 매칭 성공: {len(filtered_df)}개")
        else:
            print(f"구역단위4 매칭 실패")
    
    # 2단계: 구역단위3 (읍/면/동) 매칭 시도
    if filtered_df is None and len(region_parts) >= 3 and region_parts[2]:
        print(f"구역단위3 매칭 시도: '{region_parts[2]}'")
        temp_df = df[df['구역단위3'] == region_parts[2]]
        if not temp_df.empty:
            filtered_df = temp_df
            print(f"구역단위3 매칭 성공: {len(filtered_df)}개")
        else:
            print(f"구역단위3 매칭 실패")
    
    # 3단계: 구역단위2 (시/군/구) 매칭 시도
    if filtered_df is None and len(region_parts) >= 2 and region_parts[1]:
        print(f"구역단위2 매칭 시도: '{region_parts[1]}'")
        temp_df = df[df['구역단위2'] == region_parts[1]]
        if not temp_df.empty:
            filtered_df = temp_df
            print(f"구역단위2 매칭 성공: {len(filtered_df)}개")
        else:
            print(f"구역단위2 매칭 실패")
    
    # 4단계: 구역단위1 (도) 매칭 시도
    if filtered_df is None and len(region_parts) >= 1 and region_parts[0]:
        print(f"구역단위1 매칭 시도: '{region_parts[0]}'")
        temp_df = df[df['구역단위1'] == region_parts[0]]
        if not temp_df.empty:
            filtered_df = temp_df
            print(f"구역단위1 매칭 성공: {len(filtered_df)}개")
        else:
            print(f"구역단위1 매칭 실패")
    
    # 매칭 실패 시 빈 DataFrame
    if filtered_df is None:
        filtered_df = pd.DataFrame()
    
    print(f"필터링된 데이터 행 수: {len(filtered_df)}")
    
    if filtered_df.empty:
        return jsonify({
            "error": False,
            "message": f"'{region}' 지역의 데이터를 찾을 수 없습니다.",
            "data": {
                "논": 0, "밭": 0, "과수": 0, "초지": 0, "임지": 0, "합계": 0,
                "논_비율": 0, "밭_비율": 0, "과수_비율": 0, "초지_비율": 0, "임지_비율": 0
            }
        })
    
    # 전체 합계 계산 (필터링된 데이터에서 직접)
    total_summary = {
        '논': int(filtered_df['논'].sum()),
        '밭': int(filtered_df['밭'].sum()),
        '과수': int(filtered_df['과수'].sum()),
        '초지': int(filtered_df['초지'].sum()),
        '임지': int(filtered_df['임지'].sum()),
        '합계': int(filtered_df['합계'].sum())
    }
    
    print(f"집계 결과: {total_summary}")
    
    # 비율 계산
    total_area = total_summary['합계']
    if total_area > 0:
        논_비율 = round((total_summary['논'] / total_area) * 100, 1)
        밭_비율 = round((total_summary['밭'] / total_area) * 100, 1)
        과수_비율 = round((total_summary['과수'] / total_area) * 100, 1)
        초지_비율 = round((total_summary['초지'] / total_area) * 100, 1)
        임지_비율 = round((total_summary['임지'] / total_area) * 100, 1)
    else:
        논_비율 = 밭_비율 = 과수_비율 = 초지_비율 = 임지_비율 = 0
    
    print(f"비율 계산: 논={논_비율}%, 밭={밭_비율}%, 과수={과수_비율}%, 초지={초지_비율}%, 임지={임지_비율}%")
    
    result = {
        "error": False,
        "message": f"'{region}' 지역의 토지 이용 현황",
        "data": {
            "논": total_summary['논'],
            "밭": total_summary['밭'],
            "과수": total_summary['과수'],
            "초지": total_summary['초지'],
            "임지": total_summary['임지'],
            "합계": total_summary['합계'],
            "논_비율": 논_비율,
            "밭_비율": 밭_비율,
            "과수_비율": 과수_비율,
            "초지_비율": 초지_비율,
            "임지_비율": 임지_비율
        }
    }
    
    print(f"최종 응답: {result}")
    return jsonify(result)

if __name__ == "__main__":
    # For local run
    app.run(host="0.0.0.0", port=5000, debug=True)
