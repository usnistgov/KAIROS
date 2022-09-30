import json
import os


def load_json_file(directory_path: str, file_name: str):
    if directory_path.endswith('/'):
        directory_path = directory_path[:-1]

    # load only .json files
    if file_name.endswith('.json') and (not file_name.startswith('.')):
        with open(os.path.join(directory_path, file_name)) as json_file:
            json_data = json.load(json_file)
            return json_data
    else:
        return None


def load_json_directory(team_name: str, output_directory: str):
    json_dict = dict()
    if output_directory.endswith('/'):
        output_directory = output_directory[:-1]
    directory_path = os.path.join(output_directory, team_name)

    for file_name in os.listdir(directory_path):
        # load only .json files
        if file_name.endswith('.json') and (not file_name.startswith('.')):
            with open(os.path.join(directory_path, file_name)) as json_file:
                json_data = json.load(json_file)
                json_dict[file_name] = json_data

    return json_dict
