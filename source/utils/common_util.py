import yaml
import os


def get_config() -> dict:
    """Get the application config"""
    with open("../../config/spark_config.yaml", "r") as f:
        config = yaml.safe_load(f)

    return config


def parent_path(path, level=1):
    """Get the parent or grandparent of a path"""
    for _ in range(level):
        path = os.path.dirname(path)

    return path


def get_file_path(file_name):
    app_config = get_config()
    file_path = f"{parent_path(os.getcwd(), 2)}/{app_config['input_path']}/{file_name}"

    print(file_path)
    return file_path

