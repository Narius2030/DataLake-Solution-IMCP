import sys
sys.path.append('./airflow')

from publishers import send_images_bytes
from subcribers import encode_detr, encode_yolov8
from functions.transporter.KafkaComponents import Producer, Consumer
from core.config import get_settings
import pymongo
from tqdm import tqdm
import time
import requests



settings = get_settings()

def transport(batch, partition):
    prod_tasks = [
        Producer(topic="embeddings", batch=batch, generator=send_images_bytes),
    ]
    
    cons_tasks = [
        Consumer(topic="embeddings", group_id='yolo', path='./logs', function=encode_yolov8, partition=partition),
        Consumer(topic="embeddings", group_id='detr', path='./logs', function=encode_detr, partition=partition),
    ]
    try:
        print("Transporting threads have been starting...\n")
        # Start threads and Stop threads
        for t in prod_tasks:
            t.start()
        # while not all(t.is_stop() for t in prod_tasks):
        #     pass
        time.sleep(2)
        for task in prod_tasks:
            task.stop()
        
        for t in cons_tasks:
            t.start()
        # while not all(t.is_stop() for t in cons_tasks):
        #     pass
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
    encode_yolov8_detr(5, 100)
    pass
                 