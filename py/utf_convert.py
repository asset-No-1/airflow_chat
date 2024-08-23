import json
import sys

File = sys.argv[1]

with open(File, 'r', encoding='utf-8') as file:
    data = file.read()

parsed_data = json.loads(data)

with open(File, 'w', encoding='utf-8') as file:
    json.dump(parsed_data, file, ensure_ascii=False, indent=4)

print("변환 완료. 파일명: <Filename>_converted.json")
