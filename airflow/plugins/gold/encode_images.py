import sys
sys.path.append('./airflow')

from functions.images.detr.util.features import get_detr_model
from functions.images.yolo.util.features import get_yolov8_extractor
from functions.images.combine import combine_yolo_detr
from core.config import get_settings
import pymongo
from tqdm import tqdm
import pickle
import time


settings = get_settings()    

def encode_yolov8_detr():
    # Tải mô hình DETR
    detr_model, _ = get_detr_model(pretrained=True)
    yolo_model = get_yolov8_extractor(model_name='yolov8m.pt')
    # Khởi tạo một dictionary để lưu trữ các đặc trưng của ảnh
    yolo_caches = {}
    detr_caches = {}

    # Lặp qua từng ảnh trong thư mục
    with pymongo.MongoClient(settings.DATABASE_URL) as client:
        db = client['imcp']
        pipeline = [{
                '$sort': {'url': 1}
            }, {
                '$project': {'created_time': 0, 'publisher': 0, '_id': 0 }
            }, {
                '$limit': 8000
        }]
        documents = db['refined'].aggregate(pipeline)
        for doc in tqdm(documents):
            try:
                image_url = doc['url']
                yolov4_encode, detr_encode = combine_yolo_detr(image_url, yolo_model, detr_model)
            except ConnectionError:
                print("Lỗi tải dữ liệu...")
                for attempt in range(0, 2):
                    try:
                        yolov4_encode, detr_encode = combine_yolo_detr(image_url, yolo_model, detr_model)
                        break  # Thành công, thoát khỏi vòng lặp thử lại
                    except ConnectionError as e:
                        print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{3}): {e}")
                        time.sleep(2)  # Chờ đợi trước khi thử lại
            image_id = image_url[-16:-4]  
            yolo_caches[image_id] = yolov4_encode
            detr_caches[image_id] = detr_encode
        
        # savae embeddings to pkl
        with open(f"./airflow/data/HuggingFace/yolo_embedding.pkl", 'wb') as f:
            pickle.dump(yolo_caches, f)
            
        with open(f"./airflow/data/HuggingFace/detr_embedding.pkl", 'wb') as f:
            pickle.dump(detr_caches, f)
                 