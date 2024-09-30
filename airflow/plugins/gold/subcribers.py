import sys
sys.path.append('./airflow')

from datetime import date
import json
import pickle


def write_json_logs(message, path, topic, partition):
    ## TODO: write data into json/pkl
    print("write logs")
    for _, datas in message.items():
        try:
            for data in datas:
                value = data.value.decode('utf-8')
                value = json.loads(value)
                # read existing file and update key-value pairs with new values
                with open(f"{path}/{topic}_{date.today()}_{partition}.pkl", 'rb') as file:
                    data = pickle.load(file)
                data.update(value)
                # write updated data into pkl again
                with open(f"{path}/{topic}_{date.today()}_{partition}.pkl", "wb") as file:
                    pickle.dump(data, file)
        except Exception as exc:
            raise Exception(str(exc))