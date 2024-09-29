import sys
sys.path.append('./airflow')

from datetime import date


def write_json_logs(message, path, topic):
    ## TODO: write data into json/pkl
    print("write logs")
    for _, datas in message.items():
        try:
            for data in datas:
                value = data.value.decode('utf-8')
                with open(f"{path}/{topic}_{date.today()}.txt", "a", encoding="utf-8") as file:
                    file.write(f'{value}\n')
        except Exception as exc:
            raise Exception(str(exc))