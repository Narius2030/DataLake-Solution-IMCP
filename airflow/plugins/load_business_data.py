import sys
sys.path.append('/opt/airflow')

import os
from tqdm import tqdm
import time
import requests
from core.config import get_settings
from utils.images.yolov8_encoder import YOLOFeatureExtractor
from utils.operators.storage import MinioStorageOperator
from utils.operators.database import MongoDBOperator


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
yolo_extractor = YOLOFeatureExtractor(f'{settings.WORKING_DIRECTORY}/utils/images/model/yolov8n.pt')
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def encode_image(image_url, image_bytes):
    features = {}
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


def loop_batch_images(datasets):
    for doc in tqdm(datasets):
        image_url = doc['url']
        try:
            image_repsonse = requests.get(image_url, timeout=1)
            encode_image(image_url, image_repsonse.content)
        except Exception:
            for attempt in range(0, 2):
                try:
                    image_repsonse = requests.get(image_url, timeout=1)
                    encode_image(image_url, image_repsonse.content)
                    break  # Thành công, thoát khỏi vòng lặp thử lại
                except Exception as e:
                    print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{2}): {e}")
                    time.sleep(2)  # Chờ đợi trước khi thử lại


def load_encoded_data():
    try:
        for batch in mongo_operator.data_generator('refined'):
            datasets = list(batch)
            loop_batch_images(datasets)
            print('SUCCESS with', len(datasets))
            break
    except Exception as exc:
        #Raise error
        raise Exception(str(exc))
     
                
def load_image_storage():
    try:
        minio_operator.upload_file('mlflow','/features/extracted_features.pkl',f'{settings.EXTRACT_FEATURE_PATH}/extracted_features.pkl')
    except:
        raise Exception('Upload extracted feature file failed!')
    finally:
        os.remove(f'{settings.EXTRACT_FEATURE_PATH}/extracted_features')
            
            
if __name__=='__main__':
    load_encoded_data()
    # load_image_storage()
                 