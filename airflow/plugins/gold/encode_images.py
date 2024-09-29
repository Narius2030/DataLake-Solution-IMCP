import sys
sys.path.append('./airflow')

from publishers import encode_detr, encode_yolov8
from subcribers import write_json_logs
from functions.images.combine import combine_yolo_detr
from functions.transporter.KafkaComponents import Producer, Consumer
from core.config import get_settings
import pymongo
from tqdm import tqdm
import pickle
import time
import requests
import json



settings = get_settings()

def transport(batch):
    prod_tasks = [
        Producer(topic="yolov8", batch=batch, generator=encode_yolov8, key='yolo'),
        Producer(topic="detr", batch=batch, generator=encode_detr, key='detr'),
    ]
    
    cons_tasks = [
        Consumer(topic="yolov8", group_id='yolo', path='./logs', function=write_json_logs),
        Consumer(topic="detr", group_id='detr', path='./logs', function=write_json_logs),
    ]
    try:
        print("Transporting threads have been starting...")
        # Start threads and Stop threads
        for t in prod_tasks:
            t.start()
        # time.sleep(120)     # TODO: deal with time.sleep - make it more flexibile
        # for task in prod_tasks:
        #     task.stop()
        
        for t in cons_tasks:
            t.start()
        time.sleep(10)      # TODO: deal with time.sleep - make it more flexibile
        for task in cons_tasks:
            task.stop()
            
        for task in prod_tasks:
            task.join()
        for task in cons_tasks:
            task.join()
        print("Transporting threads have stopped ✔")
        
    except Exception as exc:
        print(str(exc) + '❌')


def encode_yolov8_detr():
    # Khởi tạo một dictionary để lưu trữ các đặc trưng của ảnh
    with pymongo.MongoClient(settings.DATABASE_URL) as client:
        db = client['imcp']
        documents = db['refined'].find({'url': {'$exists': True}}, {'url': 1, '_id': 0}) \
                                .batch_size(10) \
                                .sort('url', pymongo.ASCENDING) \
                                .limit(100)
        for i in range(20 // 5 + 1):
            batch_data = []
            caches = {}
            for doc in tqdm(documents):
                batch_data.append(doc)
                image_url = doc['url']
                try:
                    image_repsonse = requests.get(image_url, timeout=1)
                    caches[image_url] = image_repsonse
                except Exception:
                    for _ in range(0, 2):
                        try:
                            image_repsonse = requests.get(image_url, timeout=1)
                            caches[image_url] = image_repsonse
                            break  # Thành công, thoát khỏi vòng lặp thử lại
                        except Exception as e:
                            # print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{2}): {e}")
                            time.sleep(2)  # Chờ đợi trước khi thử lại
                if len(batch_data) == 5:
                    print('============> Batch', i+1)
                    transport(batch=caches)
                    break
            
    
    # # savae embeddings to pkl
    # with open(f"./airflow/data/HuggingFace/yolo_embedding.pkl", 'wb') as f:
    #     pickle.dump(yolo_caches, f)
        
    # with open(f"./airflow/data/HuggingFace/detr_embedding.pkl", 'wb') as f:
    #     pickle.dump(detr_caches, f)
            
            
if __name__=='__main__':
    # encode_yolov8_detr()
    pass
                 