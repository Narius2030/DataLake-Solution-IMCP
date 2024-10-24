import sys
sys.path.append('./airflow')

from publishers import send_images_bytes
from subcribers import encode_yolov8
from functions.transporter.KafkaComponents import Producer, Consumer
from core.config import get_settings
from datetime import date
import pymongo
from tqdm import tqdm
import time
import requests
import base64
from functions.images.yolo.yolov8_encoder import YOLOFeatureExtractor
from functions.storage.operators import MinioStorageOperator


settings = get_settings()
yolo_extractor = YOLOFeatureExtractor('./airflow/functions/images/yolo/model/yolov8n.pt')
minio_operator = MinioStorageOperator(endpoint='116.118.50.253:9000', access_key='minio', secret_key='minio123')


def transport(batch):
    prod_tasks = [
        Producer(topic="embeddings", batch=batch, generator=send_images_bytes),
    ]
    
    cons_tasks = [
        Consumer(topic="embeddings", group_id='yolo', path='./logs', function=encode_yolov8),
        Consumer(topic="embeddings", group_id='yolo', path='./logs', function=encode_yolov8),
    ]
    try:
        print("Transporting threads have been starting...\n")
        # Start threads and Stop threads
        for t in prod_tasks:
            t.start()
        time.sleep(2)
        for task in prod_tasks:
            task.stop()
        for t in cons_tasks:
            t.start()
        time.sleep(10)
        for task in cons_tasks:
            task.stop()
            
        for task in prod_tasks:
            task.join()
        for task in cons_tasks:
            task.join()
        print("Batch transporting threads have stopped ✔ \n")
        
    except Exception as exc:
        print(str(exc) + '❌')


def embedding(batch):
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
        temp = yolo_extractor.load_feature('./logs', f"yolov8_{date.today()}.pkl")
        if temp is not None:
            features.update(temp)
        # write updated data into pkl again
        yolo_extractor.save_feature(features, './logs', f"yolov8_{date.today()}.pkl")


def encoding_data(batch:int, total_num:int):
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
                            # caches[image_url] = image_repsonse
                            break  # Thành công, thoát khỏi vòng lặp thử lại
                        except Exception as e:
                            print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{2}): {e}")
                            time.sleep(2)  # Chờ đợi trước khi thử lại
                if len(batch_data) == batch:
                    print('============> Batch', i+1)
                    embedding(batch=caches)
                    # transport(batch=caches)
                    break
                
                
def load_image_storage():
    minio_operator.upload_file('mlflow','yolov8_features.pkl','./logs/yolov8_2024-10-24.pkl')
            
            
if __name__=='__main__':
    # encoding_data(batch=5, total_num=100)
    load_image_storage()
                 