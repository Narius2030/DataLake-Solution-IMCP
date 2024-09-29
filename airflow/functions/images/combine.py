import sys
sys.path.append('./airflow')

from functions.images.detr.util.features import extract_detr_features
from functions.images.yolo.util.features import extract_yolo_features
from functions.images.detr.util.transform import reshape
import requests


def combine_yolo_detr(image_path, yolo_model, detr_model):
    response = requests.get(image_path, timeout=1)
    # Trích xuất đặc trưng từ YOLO
    yolo_features = extract_yolo_features(response.content, yolo_model)

    # Trích xuất đặc trưng từ DETR
    image_tensor = reshape(response.content)
    detr_features = extract_detr_features(image_tensor, detr_model)

    return yolo_features, detr_features