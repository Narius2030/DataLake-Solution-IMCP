import pickle
import numpy as np

with open('./airflow/data/extracted_features_20241202220400.pkl', 'rb') as file:
    data = pickle.load(file)
    print(len(data.keys()))
    print(data.keys())
    # print(np.array(data['http://images.cocodataset.org/val2017/000000400573.jpg']).shape)