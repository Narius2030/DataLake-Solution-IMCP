import sys
sys.path.append('/opt/airflow')

import os
import pymongo
from tqdm import tqdm
import time
import requests
import base64
from core.config import get_settings
from utils.images.yolov8_encoder import YOLOFeatureExtractor
from utils.operators.storage import MinioStorageOperator
from utils.operators.database import MongoDBOperator


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
yolo_extractor = YOLOFeatureExtractor('/opt/airflow/functions/images/yolo/model/yolov8n.pt')
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def encode_image(batch):
    features = {}
    for image_url, image_response in batch.items():
        # decode base64 to bytes
        image_bytes = base64.b64decode(image_response.encode('utf-8'))
        image_rgb = yolo_extractor.cv2_read_image(image_bytes)
        # extract feature by yolov8
        transformed_image = yolo_extractor.preprocess_image(image_rgb)
        feature_matrix = yolo_extractor.extract_features(transformed_image)
        features[image_url] = feature_matrix.tolist()
        
        # read existing file and update key-value pairs with new values
        temp = yolo_extractor.load_feature(settings.EXTRACT_FEATURE_PATH, f"extracted_features.pkl")
        if temp is not None:
            features.update(temp)
        # write updated data into pkl again
        yolo_extractor.save_feature(features, settings.EXTRACT_FEATURE_PATH, f"extracted_features.pkl")


def loop_images(batch:int, total_num:int):
    # Khởi tạo một dictionary để lưu trữ các đặc trưng của ảnh
    with pymongo.MongoClient(settings.DATABASE_URL) as client:
        db = client['imcp']
        documents = db['refined'].find({'url': {'$exists': True}}, {'url': 1, '_id': 0}) \
                                .batch_size(batch) \
                                .sort('url', pymongo.ASCENDING) \
                                .limit(total_num)
        for i in range(total_num // batch + 1):
            batch_data = []
            caches = {}
            for doc in tqdm(documents):
                batch_data.append(doc)
                image_url = doc['url']
                try:
                    image_repsonse = requests.get(image_url, timeout=1)
                    caches[image_url] = base64.b64encode(image_repsonse.content).decode('utf-8')
                except Exception:
                    for attempt in range(0, 2):
                        try:
                            image_repsonse = requests.get(image_url, timeout=1)
                            caches[image_url] = base64.b64encode(image_repsonse.content).decode('utf-8')
                            break  # Thành công, thoát khỏi vòng lặp thử lại
                        except Exception as e:
                            print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{2}): {e}")
                            time.sleep(2)  # Chờ đợi trước khi thử lại
                if len(batch_data) == batch:
                    print('============> Batch', i+1)
                    encode_image(batch=caches)
                    break


def load_encoded_data():
    try:
        for batch in mongo_operator.data_generator('huggingface'):
            datasets = list(batch)
            print('SUCCESS with', len(datasets))
        
    except Exception as exc:
        #Raise error
        raise Exception(str(exc))
     
                
def load_image_storage():
    try:
        minio_operator.upload_file('mlflow','extracted_features.pkl',f'{settings.EXTRACT_FEATURE_PATH}/yolov8_2024-10-24.pkl')
    except:
        raise Exception('Upload extracted feature file failed!')
    finally:
        os.remove(f'{settings.EXTRACT_FEATURE_PATH}/extracted_features')
            
            
if __name__=='__main__':
    params = {
        'batch': 100,
        'total': 8000
    }
    # load_encoded_data(batch=5, total_num=100)
    load_image_storage()
                 