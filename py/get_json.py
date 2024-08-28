from kafka import KafkaConsumer, TopicPartition
from json import loads, dump, dumps
import os
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("GetMessageFromKafka").getOrCreate()

# File Constants

airflow_home = os.environ.get("AIRFLOW_HOME", "")
OFFSET_FILE = 'consumer_offset.txt'
OUTPUT_FILE = f'{airflow_home}/chat_messages.json'

# 마지막으로 처리된 offset 저장
def save_offset(offset):
    with open(OFFSET_FILE, 'w') as f:
        f.write(str(offset))

# 마지막으로 처리됐던 offset 읽어옴
def read_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return int(f.read().strip())
    return None

# kafka message를 json으로 저장
def write_message_to_json(message, output_file):
    with open(output_file, 'a', encoding='utf-8', errors="ignore") as f:
        
        dump(message, f, ensure_ascii=False)
        f.write("\n")

# 마지막으로 처리된 offest 읽기
saved_offset = read_offset()

# kafka consumer initialize
consumer = KafkaConsumer(
        bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=5000,
        #auto_offset_reset="earliest" # offset이 지정되어 있지 않을때 사용
        group_id="team1-chatlogs", #같은그룹으로 묶고 알려준다.
        enable_auto_commit=False # 비명시적 offset commit
        )

#consumer_timeout_ms=5000 // 타임아웃 5초 후
#auto_offset_reset=' '    // earliest 이전기록 전부 출력 후 실행, latest 요청 한번만 실행
print("[Start] get consumer")

# define the topic and partition
p = TopicPartition('Product', 0) # 0 은 파티션 넘버
consumer.assign([p])

# 마지막으로 처리된 offset찾거나, 처음부터 읽기
if saved_offset is not None:
    consumer.seek(p, saved_offset)
else:
    consumer.seek_to_beginning(p)

# 메세지 읽고 json으로 저
for msg in consumer:
    print(f"offset={msg.offset}, value={msg.value}")
    write_message_to_json(msg.value, OUTPUT_FILE)
    save_offset(msg.offset + 1)
    
    # message value가 exit이면 그만 읽기, 근데 아마 여러번 나갔다 들어왔을테니 지우자
    #if msg.value['str'] == 'exit':
        #print("Exit message received, closing consumer.")
        #break  # 루프 탈출

print("[End] get consumer")
# spark session 정지
spark.stop()
