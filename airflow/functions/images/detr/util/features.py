import sys
sys.path.append('./airflow/functions/images')

from detr.hubconf import detr_resnet50_panoptic
import torch


# Tải mô hình DETR với ResNet50
def get_detr_model(pretrained=True, num_classes=250):
    model, postprocessor = detr_resnet50_panoptic(pretrained=pretrained, num_classes=num_classes, return_postprocessor=True)
    return model, postprocessor

# Trích xuất đặc trưng từ mô hình DETR
def extract_detr_features(image_tensor, model):
    model.eval()
    with torch.no_grad():
        outputs = model(image_tensor)
    
    # Trích xuất đặc trưng từ output của DETR panoptic
    bbox_attention = outputs['bbox_attention'].squeeze().numpy().flatten()
    return bbox_attention