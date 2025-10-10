import json
import xml.etree.ElementTree as ET
from pathlib import Path

XML_PATH = r"C:\Users\admin\Desktop\오픈다트 회사 정보 관련 데이터 셋\corpCode.xml"
JSON_PATH = r"C:\Users\admin\Desktop\오픈다트 회사 정보 관련 데이터 셋\corpCode.json"

# 메모리 절약용 스트리밍 파서
records = []
for event, elem in ET.iterparse(XML_PATH, events=("end",)):
    # 회사 한 건은 <list> 태그로 내려옴
    if elem.tag == "list":
        def get(tag):
            node = elem.find(tag)
            return node.text.strip() if (node is not None and node.text) else ""

        rec = {
            "corp_code":      get("corp_code"),
            "corp_name":      get("corp_name"),
            "corp_eng_name":  get("corp_eng_name"),  # 없으면 ""로 저장
            "stock_code":     get("stock_code"),
            "modify_date":    get("modify_date"),
        }
        records.append(rec)
        elem.clear()  # 메모리 해제

# JSON 배열로 저장 (UTF-8 with BOM 필요 없으면 encoding='utf-8')
with open(JSON_PATH, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"done: {len(records)} rows -> {JSON_PATH}")