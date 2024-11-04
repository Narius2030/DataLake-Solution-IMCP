import sys
sys.path.append('/opt/airflow')

import pandas as pd
import polars as pl
import requests
import cv2
import io
import time
from core.config import get_settings
from utils.operators.database import MongoDBOperator
from utils.operators.storage import MinioStorageOperator
from utils.pre_proccess import tokenize, scaling_data, clean_text
from utils.images.yolov8_encoder import YOLOFeatureExtractor


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
yolo_extractor = YOLOFeatureExtractor('/opt/airflow/functions/images/yolo/model/yolov8n.pt')
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def load_refined_data():
    start_time = pd.to_datetime('now')
    affected_rows = 0
    try:
        for batch in mongo_operator.data_generator('huggingface'):
            data = list(batch)
            df = pl.DataFrame(data).drop('_id')
            lowered_df = df.with_columns(
                *[pl.col(col).str.to_lowercase().alias(col) for col in ['caption','short_caption']]
            )
            cleaned_df = lowered_df.with_columns(
                *[ pl.col(col).map_elements(lambda x: clean_text(x), return_dtype=pl.String).alias(col) for col in ['caption','short_caption']]
            )
            tokenized_df = cleaned_df.with_columns(
                *[ pl.col(col).map_elements(lambda x: tokenize(x), return_dtype=pl.List(pl.String)).alias(f'{col}_tokens') for col in ['caption','short_caption']]
            )
            refined_df = scaling_data(tokenized_df, ['url', 'caption', 'short_caption', 'caption_tokens', 'short_caption_tokens', 'publisher', 'created_time'])
            data = refined_df.to_dicts()
            mongo_operator.insert('refined', data)
            
            affected_rows += len(data)
            print('SUCCESS with', len(data))
    
        # Write logs
        mongo_operator.write_log('audit', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)
        
    except Exception as exc:
        aggregate = [{'$sort': {'created_time': -1}}, {'$project': {'_id': 1}}]
        data = mongo_operator.find_data_with_aggregate('refined', aggregate)
        affected_rows = len(data)
        # Write logs
        mongo_operator.write_log('audit', start_time=start_time, status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        
        #Raise error
        raise Exception(str(exc))
    
    print('''
            ===========================================================
            Number of rows were inserted: {}
            ===========================================================
        ''', affected_rows)


def upload_image(image_matrix, image_name):
    # Bước 1: Chuyển đổi ma trận ảnh sang byte
    _, encoded_image = cv2.imencode('.jpg', image_matrix)
    image_bytes = io.BytesIO(encoded_image)
    minio_operator.upload_object_bytes(image_bytes, 'mlflow', f'/refined_images/{image_name}', "image/jpeg")

def load_refined_image():
    # Khởi tạo một dictionary để lưu trữ các đặc trưng của ảnh
    for batch in mongo_operator.data_generator('huggingface'):
        batch_data = []
        for doc in batch:
            batch_data.append(doc)
            image_url = doc['url']
            image_name = doc['url'][-16:]
            print(image_name)
            try:
                image_repsonse = requests.get(image_url, timeout=1)
                image_rgb = yolo_extractor.cv2_read_image(image_repsonse.content)
                print(image_rgb.shape)
                upload_image(image_rgb, image_name)
            except Exception:
                for attempt in range(0, 2):
                    try:
                        image_repsonse = requests.get(image_url, timeout=1)
                        image_rgb = yolo_extractor.cv2_read_image(image_repsonse.content)
                        upload_image(image_rgb, image_name)
                        break  # Thành công, thoát khỏi vòng lặp thử lại
                    except Exception as e:
                        print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{2}): {e}")
                        time.sleep(2)  # Chờ đợi trước khi thử lại
            

if __name__=='__main__':
    # load_refined_data()
    load_refined_image()