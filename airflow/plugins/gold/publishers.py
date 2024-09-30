import sys
sys.path.append('./airflow')

from functions.images.detr.util.features import extract_detr_features
from functions.images.yolo.util.features import extract_yolo_features
from functions.images.detr.util.transform import reshape
from tqdm import tqdm
import json


def encode_yolov8(batch, yolo_model):
    features = {}
    for image_url, image_repsonse in tqdm(batch.items()):
        try:
            yolo_feature = extract_yolo_features(image_repsonse.content, yolo_model)
            features[image_url] = yolo_feature.tolist()
        except Exception as e:
            print(f"Error YOLOv8 - Tải lại dữ liệu từ {image_url}: {e}")
    print(f"Finished Batch")
    return features, len(features.keys())


def encode_detr(batch, detr_model):
    features = {}
    for image_url, image_repsonse in tqdm(batch.items()):
        try:
            image_tensor = reshape(image_repsonse.content)
            detr_features = extract_detr_features(image_tensor, detr_model)
            features[image_url] = detr_features.tolist()
        except Exception as e:
            print(f"Error DETR - Tải lại dữ liệu từ {image_url}: {e}")
    print(f"Finished Batch")
    return features, len(features.keys())