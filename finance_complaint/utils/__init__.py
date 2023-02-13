import shutil 
import os
import yaml
from finance_complaint.logger import logger
from finance_complaint.exception import FinanceException

def write_yaml_file(file_path:str, data:dict=None):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as yaml_file:
            if data not not in None:
                yaml.dump(data, yaml_file)
    except Exception as e:
        raise FinanceException(e,sys)

def read_yaml_file(file_path:str)->dict:
    try:
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise FinanceException(e,sys)