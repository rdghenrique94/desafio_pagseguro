from kaggle.api.kaggle_api_extended import KaggleApi
import json

# obtenção do csv via aop kaggle


def get():
    kaggle = {"username": "rdghenrique94", "key": "96c22634e22ef197ad6decf719ffb8e4"}
    with open('kaggle.json', 'w') as file:
        json.dump(kaggle, file)
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_file('ealaxi/banksim1',
                                  file_name='bs140513_032310.csv')
#get()
