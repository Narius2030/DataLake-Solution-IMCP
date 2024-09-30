import sys
sys.path.append('./airflow')

import torch
from PIL import Image
from io import BytesIO
from ultralytics.nn.tasks import attempt_load_one_weight
from tensorflow.keras.preprocessing.image import img_to_array
from functions.images.yolo.model.yolov8_extractor import YOLOv8DetectionAndFeatureExtractorModel


def get_yolov8_extractor(model_name="yolov8m.pt"):
    ckpt = None
    model_name_or_path = model_name
    if str(model_name_or_path).endswith('.pt'):
        _, ckpt = attempt_load_one_weight(model_name_or_path)
        cfg = ckpt['model'].yaml
    else:
        cfg = model_name_or_path

    model = YOLOv8DetectionAndFeatureExtractorModel(cfg, nc=None, verbose=True)
    return model


# Trích xuất đặc trưng từ YOLO
def extract_yolo_features(image_response, model):
    image = Image.open(BytesIO(image_response)).resize(size=(640, 640)).convert('RGB')
    image_array = img_to_array(image)
    image_array = image_array.reshape((1, image_array.shape[2], image_array.shape[0], image_array.shape[1]))
    image_tensor = torch.from_numpy(image_array)
    image_tensor = image_tensor.type(torch.float32)
    features, _ = model.custom_forward(image_tensor)

    yolo_features = features.detach().numpy().flatten()
    return yolo_features