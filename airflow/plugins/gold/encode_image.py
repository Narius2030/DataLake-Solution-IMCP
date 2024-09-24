import sys
sys.path.append('./airflow')

import os
from functions.images.detr.util.features import get_detr_model, extract_detr_features
from functions.images.yolo.util.features import extract_yolo_features
from functions.images.detr.util.transform import compose_shape


# Tải mô hình DETR
model, postprocessor = get_detr_model(pretrained=True)

# Khởi tạo một dictionary để lưu trữ các đặc trưng của ảnh
image_features = {}

# Lặp qua từng ảnh trong thư mục
for img_name in os.listdir(INPUT_DIR):
    img_path = os.path.join(INPUT_DIR, img_name)
    
    # Trích xuất đặc trưng từ YOLO
    yolo_features = extract_yolo_features(img_path)
    
    # Tải và chuyển đổi hình ảnh đầu vào cho DETR
    image_tensor = compose_shape(img_path)
    
    # Trích xuất đặc trưng từ DETR
    detr_features = extract_detr_features(image_tensor, model)
    
    # Lưu trữ các đặc trưng trong dictionary với ID ảnh là key
    image_id = img_name.split('.')[0]