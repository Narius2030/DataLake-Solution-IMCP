import sys
sys.path.append('./airflow')

from tqdm import tqdm
import base64


def send_images_bytes(batch):
    for image_url, image_repsonse in tqdm(batch.items()):
        try:
            image_repsonse = base64.b64encode(image_repsonse.content).decode('utf-8')
            yield {image_url:image_repsonse}, image_url
        except Exception as e:
            print(f"Error YOLOv8 - Tải lại dữ liệu từ {image_url}: {e}")
