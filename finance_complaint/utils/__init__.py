import shutil 
import os
import sys
from marshmallow import pre_dump
from pyspark.ml.base import PredictionModel
import yaml
from finance_complaint.logger import logger
from finance_complaint.exception import FinanceException
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame

def write_yaml_file(file_path: str, data: dict = None):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as yaml_file:
            if data is not None:
                yaml.dump(data, yaml_file)
    except Exception as e:
        raise FinanceException(e, sys)

def read_yaml_file(file_path:str)->dict:
    try:
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise FinanceException(e,sys)

def get_score(dataframe:DataFrame, metric_name, label_col, prediction_col)->float:
    try:
        evaluator = MulticlassClassificationEvaluator(
            predictionCol=prediction_col, labelCol=label_col,
            metricName=metric_name
        )
        score = evaluator.evaluate(dataframe)
        print(f"{metric_name} score: {score}")
        logger.info(f"{metric_name} score: {score}")
        return score
    except Exception as e:
        raise FinanceException(e,sys)