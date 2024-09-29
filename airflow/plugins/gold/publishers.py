import sys
sys.path.append('./airflow')

from functions.images.detr.util.features import extract_detr_features
from functions.images.yolo.util.features import extract_yolo_features
from functions.images.detr.util.transform import reshape
from tqdm import tqdm


def encode_yolov8(batch, yolo_model):
    for image_url, image_repsonse in tqdm(batch.items()):
        try:
            yolo_features = extract_yolo_features(image_repsonse.content, yolo_model)
            yield yolo_features.shape, image_url
        except Exception as e:
            print(f"YOLOv8 - Tải lại dữ liệu từ {image_url}: {e}")
    print(f"Finished Batch")


def encode_detr(batch, detr_model):
    for image_url, image_repsonse in tqdm(batch.items()):
        try:
            image_tensor = reshape(image_repsonse.content)
            detr_features = extract_detr_features(image_tensor, detr_model)
            yield detr_features.shape, image_url
        except Exception as e:
            print(f"DETR - Tải lại dữ liệu từ {image_url}: {e}")
    print(f"Finished Batch")