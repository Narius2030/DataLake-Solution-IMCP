import sys
sys.path.append('./airflow')

from datetime import date
import json
import pickle
import os
import base64
from tqdm import tqdm
from functions.images.yolo.yolov8_encoder import YOLOFeatureExtractor


yolo_extractor = YOLOFeatureExtractor('./airflow/functions/images/yolo/model/yolov8n.pt')

def encode_yolov8(message, path, topic):
    print("write logs yolo\n")
    features = {}
    for _, datas in tqdm(message.items()):
        try:
            for data in datas:
                value = data.value.decode('utf-8')
                value = json.loads(value)
                # decode base64 to bytes
                image_bytes = base64.b64decode(list(value.values())[0].encode('utf-8'))
                image_rgb = yolo_extractor.cv2_read_image(image_bytes)
                # extract feature by yolov8
                transformed_image = yolo_extractor.preprocess_image(image_rgb)
                feature_matrix = yolo_extractor.extract_features(transformed_image)
                features[list(value.keys())[0]] = feature_matrix.tolist()
                
                # read existing file and update key-value pairs with new values
                temp = yolo_extractor.load_feature(path, f"{topic}_{date.today()}.pkl")
                if temp is not None:
                    features.update(temp)
                # write updated data into pkl again
                yolo_extractor.save_feature(features, path, f"{topic}_{date.today()}.pkl")
        except Exception as exc:
            raise Exception(str(exc))