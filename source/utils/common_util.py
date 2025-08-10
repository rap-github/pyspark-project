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


def get_raw_file_path(folder_path: str = None,
                      file_name: str = None) -> str:
    app_config = get_config()
    if os.environ.get('ENV', 'LOCAL') == 'LOCAL':
        file_path = f'''{parent_path(os.getcwd(), 2)}/{app_config['input_path']}/{file_name}'''
    else:
        file_path = f'''app_config['s3_raw_path']/{folder_path}'''

    print(file_path)
    return file_path


def get_refined_file_path(folder_path: str = None,
                          file_name: str = None) -> str:
    app_config = get_config()
    if os.environ.get('ENV', 'LOCAL') == 'LOCAL':
        file_path = f'''{parent_path(os.getcwd(), 2)}/{app_config['output_path']}/{file_name}'''
    else:
        file_path = f'''app_config['s3_refined_path']/{folder_path}'''

    print(file_path)
    return file_path


def get_curated_file_path(folder_path: str = None,
                          file_name: str = None) -> str:
    app_config = get_config()
    if os.environ.get('ENV', 'LOCAL') == 'LOCAL':
        file_path = f'''{parent_path(os.getcwd(), 2)}/{app_config['output_path']}/{file_name}'''
    else:
        file_path = f'''app_config['s3_curated_path']/{folder_path}'''

    print(file_path)
    return file_path
