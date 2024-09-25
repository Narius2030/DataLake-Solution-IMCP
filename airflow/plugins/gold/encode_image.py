import sys
sys.path.append('./airflow')

import os
from functions.images.detr.util.features import get_detr_model, extract_detr_features
from functions.images.yolo.util.features import extract_yolo_features, get_Net_yolov4
from functions.images.detr.util.transform import reshape

import pymongo
import json
import requests
import numpy as np
from tqdm import tqdm
import base64
from elasticsearch import Elasticsearch


with open("./airflow/config/env.json", "r") as file:
    config = json.load(file)
    mongo_url = config['mongodb']['MONGO_ATLAS_PYTHON_GCP']
    hugg_index_key = config['elastic']['HUGGINGFACE_INDEX_KEY']
    hugg_host = config['elastic']['ELASTIC_HOST']
    

def encode():
    # Tải mô hình DETR
    detr_model, postprocessor = get_detr_model(pretrained=True)
    yolo_model = get_Net_yolov4("./airflow/functions/images/yolo/model/yolov4.weights", "./airflow/functions/images/yolo//model/yolov4.cfg")
    # Khởi tạo một dictionary để lưu trữ các đặc trưng của ảnh
    caches = []

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
                response = requests.get(doc['url'], timeout=0.5)
                # Trích xuất đặc trưng từ YOLO
                yolo_features = extract_yolo_features(yolo_model, response.content)
                doc['yolov4_encode'] = base64.b64encode(yolo_features)
                
                # Trích xuất đặc trưng từ DETR
                image_tensor = reshape(response.content)
                detr_features = extract_detr_features(image_tensor, detr_model)
                doc['detr_encode'] = base64.b64encode(detr_features)
                caches.append(doc)
                
                with Elasticsearch(hosts=hugg_host, api_key=hugg_index_key) as es:
                    es.update(index="huggingface-index", doc=doc, id=doc['url'][-16:-4], doc_as_upsert=True, upsert=doc)    
                    
            except Exception as exc:
                continue