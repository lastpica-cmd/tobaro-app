import os
import json
import re
from typing import Optional, Literal
from pydantic import BaseModel, Field
from openai import OpenAI
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

Intent = Literal["MATCH_FIND","REGISTER_SUPPLY","REGISTER_DEMAND","LAW_QA","DATA_UPLOAD_HELP","SMALLTALK","UNKNOWN"]

class Entities(BaseModel):
    region: Optional[str] = None
    volume_m3: Optional[int] = None
    soil_type: Optional[str] = None
    distance_km: Optional[int] = None
    date: Optional[str] = None
    purpose: Optional[str] = None  # 용도 (농업, 복구, 건설 등) - 기존 호환성
    urgency: Optional[str] = None  # 긴급도
    usage: Optional[str] = None    # 실제 토석공이스시스템 용도 (매립용, 되메우기용 등)

class RouteResult(BaseModel):
    intent: Intent = "UNKNOWN"
    confidence: float = Field(0.0, ge=0.0, le=1.0)
    entities: Entities = Field(default_factory=Entities)
    reason: str = ""

class PromptEngine:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
    def create_intent_classification_prompt(self, text: str) -> str:
        return f"""
당신은 대한민국 공공데이터 포털 토석정보공유시스템을 활용한 토사 매칭 전문가입니다.

사용자 입력: "{text}"

다음 의도 중 하나로 분류해주세요:
1. MATCH_FIND: 토사 매칭/찾기 요청 
   - "흙이 필요해", "토사 찾아줘", "매칭해줘", "어디서 구할 수 있어?"
   - "농사용 흙", "건설용 자갈", "복구용 토사" 등 용도별 요청
   - "예천군에서", "서울 근처에서" 등 지역별 요청
2. REGISTER_SUPPLY: 토사 공급 등록
   - "남는 흙 있어", "토사 공급하고 싶어", "팔고 싶어"
3. REGISTER_DEMAND: 토사 수요 등록  
   - "토사 필요해", "흙 구해줘", "등록하고 싶어"
4. LAW_QA: 법규/기준 문의
   - "법적 기준이 뭐야", "안전 규정", "허가 필요해?"
5. DATA_UPLOAD_HELP: 데이터 업로드 도움말
   - "엑셀 양식", "CSV 업로드", "데이터 입력 방법"
6. SMALLTALK: 인사말/일반 대화
   - "안녕", "고마워", "뭐해?", "도움말"
7. UNKNOWN: 분류 불가

의도만 간단히 답변해주세요 (예: MATCH_FIND)
"""

    def create_entity_extraction_prompt(self, text: str, intent: str) -> str:
        return f"""
당신은 대한민국 토석정보공유시스템을 위한 정보 추출 전문가입니다.

사용자 입력: "{text}"
분석된 의도: {intent}

다음 정보를 JSON 형태로 추출해주세요:

{{
    "region": "지역명 (예: 예천군, 응봉면, 서울시 강남구, 경기도 수원시 등)",
    "volume_m3": 숫자만 (물량, 단위는 m³로 가정, '톤'이나 '대'는 m³로 환산),
    "soil_type": "토질 (점토, 사질, 자갈, 혼합, 황토, 모래, 암석 등)",
    "distance_km": 숫자만 (거리 제한, km 단위),
    "date": "날짜/시기 (예: 2024년 3월, 내년 봄, 이번 주 등)",
    "purpose": "용도 (농업, 복구, 건설, 매립, 조경, 도로공사, 주택건설 등)",
    "usage": "실제 토석공이스시스템 용도 (매립용, 되메우기용, 조경식재용, 구조물되메우기용, 도로성토용, 기타유용 등)",
    "urgency": "긴급도 (긴급, 급함, 보통, 여유, 천천히 등)"
}}

추출 규칙:
- 지역: 시/군/구/읍/면/동 단위까지 추출
- 물량: '500톤' → 500, '100대' → 100 (대략적 환산)
- 토질: 기존 분류(점토, 사질, 자갈 등)와 새로운 분류(사토, 순성토, 리핑암 등) 모두 인식
- 용도: 맥락에서 추론
  * 농업: 밭, 고구마, 감자, 농사, 작물, 경작, 텃밭, 과수원, 논, 밭갈이, 씨앗, 모종 등
  * 조경: 정원, 화단, 가로수, 공원, 조경, 식재, 꽃, 나무, 잔디, 화분, 가든 등
  * 복구: 산사태, 하천, 복구, 되메우기, 정비, 침식, 붕괴, 수해, 자연재해 등
  * 건설: 공사, 건설, 기초, 도로, 매립, 구조물, 건물, 아파트, 상가, 공장 등
- usage: 실제 토석공이스시스템 용도로 매핑 (매립 → 매립용, 되메우기 → 되메우기용, 조경/농업 → 조경식재용, 건설 → 구조물되메우기용, 도로 → 도로성토용)
- 긴급도: '급해', '빨리' → 긴급, '급함' → 급함, '여유있게' → 여유

정보가 없으면 null로 표시하세요.
"""

    def classify_intent(self, text: str) -> str:
        try:
            prompt = self.create_intent_classification_prompt(text)
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,
                temperature=0.1
            )
            intent = response.choices[0].message.content.strip()
            return intent if intent in ["MATCH_FIND", "REGISTER_SUPPLY", "REGISTER_DEMAND", "LAW_QA", "DATA_UPLOAD_HELP", "SMALLTALK", "UNKNOWN"] else "UNKNOWN"
        except Exception as e:
            print(f"Intent classification error: {e}")
            return "UNKNOWN"

    def extract_entities(self, text: str, intent: str) -> dict:
        try:
            prompt = self.create_entity_extraction_prompt(text, intent)
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.1
            )
            entities_str = response.choices[0].message.content.strip()
            # JSON 파싱 시도
            try:
                entities = json.loads(entities_str)
            except:
                # JSON 파싱 실패 시 정규식으로 추출
                entities = self._fallback_extract_entities(text)
            return entities
        except Exception as e:
            print(f"Entity extraction error: {e}")
            return self._fallback_extract_entities(text)

    def _fallback_extract_entities(self, text: str) -> dict:
        """PE 실패 시 정규식 기반 폴백"""
        # 기존 정규식 로직
        SOIL_RX = r"(점토|사질|자갈|혼합|황토)"
        REGION_RX = r"([가-힣]{2,}(?:시|군|구|읍|면|동)|[가-힣]{2,}\s*근처|[가-힣]{2,})"
        VOL_RX = r"(\d{2,6})\s*(?:m3|㎥|M3|톤|t|대)?"
        DIST_RX = r"(\d{1,3})\s*km"
        
        m_soil = re.search(SOIL_RX, text)
        m_region = re.search(REGION_RX, text)
        m_vol = re.search(VOL_RX, text, re.IGNORECASE)
        m_dist = re.search(DIST_RX, text, re.IGNORECASE)
        
        return {
            "region": m_region.group(1) if m_region else None,
            "volume_m3": int(m_vol.group(1)) if m_vol else None,
            "soil_type": m_soil.group(1) if m_soil else None,
            "distance_km": int(m_dist.group(1)) if m_dist else None,
            "date": None,
            "purpose": None,
            "urgency": None
        }

def hybrid_route(text: str) -> RouteResult:
    """PE 기반 하이브리드 라우팅"""
    try:
        print(f"=== hybrid_route 시작 ===")
        print(f"입력 텍스트: {text}")
        
        t = (text or "").strip()
        if not t:
            print("빈 텍스트 입력")
            return RouteResult(intent="UNKNOWN", confidence=0.0, reason="empty")
        
        # PE 엔진 초기화
        print("PE 엔진 초기화 중...")
        pe = PromptEngine()
        
        # 1) 의도 분류
        print("의도 분류 중...")
        intent = pe.classify_intent(t)
        print(f"분류된 의도: {intent}")
        
        # 2) 엔티티 추출
        print("엔티티 추출 중...")
        entities_dict = pe.extract_entities(t, intent)
        print(f"추출된 엔티티: {entities_dict}")
        
    except Exception as e:
        print(f"hybrid_route 에러: {e}")
        import traceback
        print(traceback.format_exc())
        raise
    
    # 3) 신뢰도 계산 (PE 기반으로 개선)
    confidence = 0.6  # PE 기본 신뢰도
    if entities_dict.get("region"):
        confidence += 0.15
    if entities_dict.get("volume_m3"):
        confidence += 0.15
    if entities_dict.get("soil_type"):
        confidence += 0.1
    
    # 의도별 신뢰도 조정
    if intent in ["UNKNOWN", "SMALLTALK"]:
        confidence = min(confidence, 0.5)
    elif intent in ["MATCH_FIND", "REGISTER_DEMAND"]:
        confidence = min(confidence, 0.9)
    
    # Entities 객체 생성
    entities = Entities(
        region=entities_dict.get("region"),
        volume_m3=entities_dict.get("volume_m3"),
        soil_type=entities_dict.get("soil_type"),
        distance_km=entities_dict.get("distance_km"),
        date=entities_dict.get("date"),
        purpose=entities_dict.get("purpose"),
        urgency=entities_dict.get("urgency"),
        usage=entities_dict.get("usage")
    )
    
    return RouteResult(
        intent=intent if confidence >= 0.5 else "UNKNOWN",
        confidence=float(round(min(confidence, 0.95), 2)),
        entities=entities,
        reason="PE-based routing"
    )
