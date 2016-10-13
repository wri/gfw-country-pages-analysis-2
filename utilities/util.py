import os
import json


def load_json_from_token(file_name):

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    token_file = os.path.join(root_dir, 'tokens', file_name)

    with open(token_file) as data_file:
        data = json.load(data_file)

    return data