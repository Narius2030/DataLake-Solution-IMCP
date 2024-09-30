import sys
sys.path.append('./airflow')

from json import dumps
import threading
from datetime import date
from kafka import KafkaProducer, KafkaConsumer
from functions.images.detr.util.features import get_detr_model
from functions.images.yolo.util.features import get_yolov8_extractor


yolo_model = get_yolov8_extractor(model_name='./airflow/functions/images/yolo/model/yolov8m.pt')
detr_model, _ = get_detr_model(pretrained=True)

class Producer(threading.Thread):
    def __init__(self, topic:str, batch:dict, key:str=None, generator=None):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.topic = topic
        self.key = key
        self.batch = batch
        self.generator = generator

    def stop(self):
        self.stop_event.set()
        
    def run(self):
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 key_serializer=str.encode,
                                 value_serializer=lambda x: dumps(x).encode('utf-8'),
                                 max_request_size=524288000)
        if self.key == 'detr':
            model = detr_model
        else:
            model = yolo_model
        # send data to topic
        while not self.stop_event.is_set():
            # for data in self.generator(self.batch, model):
                # continue
                # type = topic
                # key = image_url
            data = self.generator(self.batch, model)
            producer.send(self.topic, value=data[0], key=f"{str(data[1])}")
            self.stop()
        producer.close()
    

class Consumer(threading.Thread):
    def __init__(self, topic:str, group_id:str=None, partition:str=0, path:str=None, function=None):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.topic = topic
        self.group_id = group_id
        self.path = path
        self.function = function
        self.partition = partition

    def stop(self):
        self.stop_event.set()
        
    def run(self):
        consumer = KafkaConsumer(self.topic, 
                                 bootstrap_servers=['localhost:9092'],
                                 auto_offset_reset='latest',
                                 auto_commit_interval_ms=2500,
                                 group_id=self.group_id,
                                 fetch_max_bytes=524288000,
                                 max_partition_fetch_bytes=524288000)
        consumer.subscribe([self.topic])
        
        while not self.stop_event.is_set():
            message = consumer.poll(timeout_ms=1000)
            # Processing Function
            if not message:
                continue
            self.function(message, self.path, self.topic, self.partition)
            # Termination Event
            if self.stop_event.is_set():
                break
        consumer.close()
