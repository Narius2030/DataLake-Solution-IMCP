import sys
sys.path.append('./airflow')

import os
from functions.images.detr.util.features import get_detr_model
from functions.images.yolo.util.features import get_Net_yolov4
from functions.images.combine import combine_yolo_detr

import pymongo
import json
import requests
import numpy as np
from tqdm import tqdm
import pickle
import time



with open("./airflow/config/env.json", "r") as file:
    config = json.load(file)
    mongo_url = config['mongodb']['MONGO_ATLAS_PYTHON_GCP']
    hugg_index_key = config['elastic']['HUGGINGFACE_INDEX_KEY']
    hugg_host = config['elastic']['ELASTIC_HOST']
    

def encode():
    # Tải mô hình DETR
    detr_model, _ = get_detr_model(pretrained=True)
    yolo_model = get_Net_yolov4("./airflow/functions/images/yolo/model/yolov4.weights", "./airflow/functions/images/yolo//model/yolov4.cfg")
    # Khởi tạo một dictionary để lưu trữ các đặc trưng của ảnh
    yolo_caches = {}
    detr_caches = {}

    # Lặp qua từng ảnh trong thư mục
    with pymongo.MongoClient(mongo_url) as client:
        db = client['imcp']
        pipeline = [{
                '$sort': {'url': 1}
            }, {
                '$project': {'created_time': 0, 'publisher': 0, '_id': 0 }
            }, {
                '$limit': 8000
            }
        ]
        documents = db['refined'].aggregate(pipeline)
        for doc in tqdm(documents):
            try:                
                yolov4_encode, detr_encode = combine_yolo_detr(doc['url'], yolo_model, detr_model)
                
                image_id = doc['url'][-16:-4]
                yolo_caches[image_id] = yolov4_encode
                detr_caches[image_id] = detr_encode
                    
            except ConnectionError:
                print("Lỗi tải dữ liệu...")
                for attempt in range(0, 3):
                    try:
                        yolov4_encode, detr_encode = combine_yolo_detr(doc['url'])
                        
                        image_id = doc['url'][-16:-4]
                        yolo_caches[image_id] = yolov4_encode
                        detr_caches[image_id] = detr_encode
                        
                        break  # Thành công, thoát khỏi vòng lặp thử lại
                    except ConnectionError as e:
                        print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{3}): {e}")
                        time.sleep(2)  # Chờ đợi trước khi thử lại
        
        # savae embeddings to pkl
        with open(f"./airflow/data/HuggingFace/yolo_embedding.pkl", 'wb') as f:
            pickle.dump(yolo_caches, f)
            
        with open(f"./airflow/data/HuggingFace/detr_embedding.pkl", 'wb') as f:
            pickle.dump(detr_caches, f)
                 