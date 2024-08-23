# AIRFLOW

## To do

1. 영화 API 저장
2. airflow alarm

### How to save chat log
1번
```bash
$ bin/kafka-console-consumer.sh --bootstrap-server <localhost:9092> --topic <topic name> --from-beginning > <File name>.json
```

2번
해당 json file의 유니코드 이스케이프 시퀀스를 한글로 변환


