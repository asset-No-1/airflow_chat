from json import loads, dump, dumps
import os
from pyspark.sql import SparkSession

# visualization pdf로 export하기위한 module
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Initialize Spark Session
spark = SparkSession.builder.appName("ProcessJsonMessage").getOrCreate()

# File Constants
airflow_home = os.environ.get("AIRFLOW_HOME", "")
JSON_FILE_PATH = f'{airflow_home}/chat_messages.json'
OUTPUT_FILE_PATH = f'{airflow_home}/output.pdf'

print("[Start] process json")

# json file 읽기
df = spark.read.option("multiline","true").json(JSON_FILE_PATH)

# dataframe 구조 보기
df.printSchema()

# 쉽게 시각화를 뽑아내도록 pandas df로 변환
df_new = df.toPandas()

# pdf 안으로 시각화 저장
with PdfPages(OUTPUT_FILE_PATH) as pdf_pages:
    # 시각화1: 가장 많이 떠든 사람 히스토그램
    many = df.groupby(['user']).size().reset_index(name='counts')
    plt.figure(figsize=(8, 6))
    many.plot(kind='bar', color='skyblue')
    plt.xlabel('User')
    plt.ylabel('Number of Messages')
    plt.title('Message Count by User')
    pdf_pages.savefig()  # Save plot to PDF

print(f"Visualizations saved to {OUTPUT_FILE_PATH}")
print("[End] process json")

# spark session 정지
spark.stop()
