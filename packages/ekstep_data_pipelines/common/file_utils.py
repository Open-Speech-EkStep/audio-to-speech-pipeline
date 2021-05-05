import os
def get_file_name(file_path):
    return file_path.split("/").pop()

def check_file_exits(file_path):
    return os.path.isfile(file_path)

def ensure_path(path):
    os.makedirs(path, exist_ok=True)
