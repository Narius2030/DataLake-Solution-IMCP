import sys
sys.path.append('/opt/airflow')

import pandas as pd
import warnings
import os
import requests
import cv2
import time
import io
from tqdm import tqdm
from core.config import get_settings
from utils.operators.database import MongoDBOperator
from utils.operators.storage import MinioStorageOperator
from utils.images.yolov8_encoder import YOLOFeatureExtractor


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
yolo_extractor = YOLOFeatureExtractor(f'{settings.WORKING_DIRECTORY}/utils/images/model/yolov8n.pt')
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def dowload_raw_data(bucket_name, file_path, parquet_engine):
    datasets = {}
    file_name = os.path.basename(file_path)
    try:
        minio_operator.download_file(bucket_name, file_path, f'{settings.RAW_DATA_PATH}/{file_name}')
        df = pd.read_parquet(f'{settings.RAW_DATA_PATH}/{file_name}', parquet_engine)
        df['created_time'] = pd.to_datetime('now')
        df['publisher'] = 'HuggingFace'
        df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
        datasets = df.to_dict('records')
    except Exception as ex:
        raise FileNotFoundError(str(ex))
    finally:
        os.remove(f'{settings.RAW_DATA_PATH}/{file_name}')
    return datasets


def upload_image(image_matrix, image_name, bucket_name, file_path):
    # Bước 1: Chuyển đổi ma trận ảnh sang byte
    _, encoded_image = cv2.imencode('.jpg', image_matrix)
    image_bytes = io.BytesIO(encoded_image)
    minio_operator.upload_object_bytes(image_bytes, bucket_name, f'{file_path}/{image_name}', "image/jpeg")


def load_raw_collection(params):
    datasets = dowload_raw_data(params['bucket_name'], params['file_path'], params['engine'])
    # start to load
    start_time = pd.to_datetime('now')
    affected_rows = 0
    try:
        if mongo_operator.is_has_data('huggingface') == False:
            warnings.warn("There is no documents in collection --> INGEST ALL")
            # If there is empty collection -> insert all
            affected_rows = mongo_operator.insert('hugging_ace', datasets)
        else:
            # If there are several documents -> check duplication -> insert one-by-one
            warnings.warn("There are documents in collection --> CHECK DUPLICATION")
            mongo_operator.insert_many_not_duplication('huggingface', datasets)
        # Write logs
        mongo_operator.write_log('huggingface', layer='bronze', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)

    except Exception as ex:
        mongo_operator.write_log('huggingface', layer='bronze', start_time=start_time, status="ERROR", error_message=str(ex), action="insert", affected_rows=affected_rows)
        raise Exception(str(ex))
        

def load_raw_image(params):
    # Khởi tạo một dictionary để lưu trữ các đặc trưng của ảnh
    for batch in mongo_operator.data_generator('huggingface'):
        batch_data = []
        for doc in tqdm(batch):
            batch_data.append(doc)
            image_url = doc['url']
            image_name = doc['url'][-16:]
            try:
                image_repsonse = requests.get(image_url, timeout=1)
                image_rgb = yolo_extractor.cv2_read_image(image_repsonse.content)
                upload_image(image_rgb, image_name, params['bucket_name'], params['file_path'])
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
    params = {
        'bucket_name': 'mlflow',
        'file_path': '/raw_data/lvis_caption_url.parquet',
        'engine': 'pyarrow'
    }
    # load_raw_collection(params)
    
    params = {
        'bucket_name': 'mlflow',
        'file_path': '/raw_data/raw_images',
    }
    load_raw_image(params)