import sys
sys.path.append('./airflow')

from datetime import date
import json
import pickle
import os
import base64
from tqdm import tqdm
from functions.images.detr.util.transform import reshape
from functions.images.detr.util.features import extract_detr_features
from functions.images.yolo.util.features import extract_yolo_features


def encode_yolov8(message, path, topic, partition, yolo_model):
    print("write logs yolo\n")
    features = {}
    for _, datas in tqdm(message.items()):
        try:
            for data in datas:
                value = data.value.decode('utf-8')
                value = json.loads(value)
                
                image_response = base64.b64decode(list(value.values())[0].encode('utf-8'))
                yolo_feature = extract_yolo_features(image_response, yolo_model)
                features[list(value.keys())[0]] = yolo_feature.tolist()
                # read existing file and update key-value pairs with new values
                if os.path.exists(f"{path}/{topic}_{date.today()}.pkl"):
                    with open(f"{path}/{topic}_{date.today()}.pkl", 'rb') as file:
                        temp = pickle.load(file)
                    features.update(temp)
                # write updated data into pkl again
                with open(f"{path}/{topic}_{date.today()}.pkl", "wb") as file:
                    pickle.dump(features, file)
        except Exception as exc:
            raise Exception(str(exc))


def encode_detr(message, path, topic, partition, detr_model):
    print("write logs detr\n")
    features = {}
    for _, datas in tqdm(message.items()):
        try:
            for data in datas:
                value = data.value.decode('utf-8')
                value = json.loads(value)
                
                image_response = base64.b64decode(list(value.values())[0].encode('utf-8'))
                image_tensor = reshape(image_response)
                detr_features = extract_detr_features(image_tensor, detr_model)
                features[list(value.keys())[0]] = detr_features.tolist()
                # read existing file and update key-value pairs with new values
                if os.path.exists(f"{path}/{topic}_{date.today()}.pkl"):
                    with open(f"{path}/{topic}_{date.today()}.pkl", 'rb') as file:
                        temp = pickle.load(file)
                    features.update(temp)
                # write updated data into pkl again
                with open(f"{path}/{topic}_{date.today()}.pkl", "wb") as file:
                    pickle.dump(features, file)
        except Exception as exc:
            raise Exception(str(exc))