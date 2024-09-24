import cv2
import numpy as np


# Trích xuất đặc trưng từ YOLO
def extract_yolo_features(img_path):
    net = cv2.dnn.readNet("./models/yolov4.weights", "./models/yolov4.cfg")
    layer_names = net.getLayerNames()
    output_layers = [layer_names[i - 1] for i in net.getUnconnectedOutLayers()]

    img = cv2.imread(img_path)
    if img is None:
        raise ValueError(f"Không thể đọc ảnh từ đường dẫn: {img_path}")
    
    blob = cv2.dnn.blobFromImage(img, 0.00392, (416, 416), (0, 0, 0), True, crop=False)
    net.setInput(blob)
    outs = net.forward(output_layers)
    
    # Làm phẳng từng phần tử của `outs` và kết hợp thành một mảng duy nhất
    yolo_features = np.concatenate([out.flatten() for out in outs], axis=0)
    return yolo_features