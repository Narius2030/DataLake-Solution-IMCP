import sys
sys.path.append('./airflow')

from publishers import encode_detr, encode_yolov8
from subcribers import write_json_logs
from functions.transporter.KafkaComponents import Producer, Consumer
from core.config import get_settings
import pymongo
from tqdm import tqdm
import pickle
import time
import requests
import json



settings = get_settings()

def transport(batch, partition):
    prod_tasks = [
        Producer(topic="yolov8", batch=batch, generator=encode_yolov8, key='yolo'),
        Producer(topic="detr", batch=batch, generator=encode_detr, key='detr'),
    ]
    
    cons_tasks = [
        Consumer(topic="yolov8", group_id='yolo', path='./logs', function=write_json_logs, partition=partition),
        Consumer(topic="detr", group_id='detr', path='./logs', function=write_json_logs, partition=partition),
    ]
    try:
        print("Transporting threads have been starting...")
        # Start threads and Stop threads
        for t in prod_tasks:
            t.start()
        time.sleep(30)     # TODO: deal with time.sleep - make it more flexibile
        for task in prod_tasks:
            task.stop()
        
        for t in cons_tasks:
            t.start()
        time.sleep(30)       # TODO: deal with time.sleep - make it more flexibile
        for task in cons_tasks:
            task.stop()
            
        for task in prod_tasks:
            task.join()
        for task in cons_tasks:
            task.join()
        print("Batch transporting threads have stopped ✔")
        
    except Exception as exc:
        print(str(exc) + '❌')


def encode_yolov8_detr(batch:int, total_num:int):
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
                    caches[image_url] = image_repsonse
                except Exception:
                    for attempt in range(0, 2):
                        try:
                            image_repsonse = requests.get(image_url, timeout=1)
                            caches[image_url] = image_repsonse
                            break  # Thành công, thoát khỏi vòng lặp thử lại
                        except Exception as e:
                            print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{2}): {e}")
                            time.sleep(2)  # Chờ đợi trước khi thử lại
                if len(batch_data) == batch:
                    print('============> Batch', i+1)
                    transport(batch=caches, partition=i)
                    break
            
            
if __name__=='__main__':
    encode_yolov8_detr(5, 20)
    pass
                 