import cv2
import numpy as np
import tempfile


def get_Net_yolov4(cfg_file, weight_file):
    net = cv2.dnn.readNet(cfg_file, weight_file)
    return net


# Trích xuất đặc trưng từ YOLO
def extract_yolo_features(net, img_response):
    layer_names = net.getLayerNames()
    output_layers = [layer_names[i - 1] for i in net.getUnconnectedOutLayers()]
    img_array = np.frombuffer(img_response, np.uint8)
    img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
    # img = cv2.imread(img_path)
    if img is None:
        raise ValueError(f"Không thể đọc ảnh từ response")
    
    blob = cv2.dnn.blobFromImage(img, 0.00392, (416, 416), (0, 0, 0), True, crop=False)
    net.setInput(blob)
    outs = net.forward(output_layers)
    
    # Làm phẳng từng phần tử của `outs` và kết hợp thành một mảng duy nhất
    yolo_features = np.concatenate([out.flatten() for out in outs], axis=0)
    return yolo_features