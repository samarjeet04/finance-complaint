import os
import sys
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.entity.artifact_entity import DataTransfomrationArtifact, ModelTrainerArtifact, ModelEvaluationArtifact
from finance_complaint.entity.config_entity import ModelEvaluationConfig
from pyspark.sql import DataFrame
from pyspark.ml.pipeline import PipelineModel
from finance_complaint.utils import get_score
from finance_complaint.config.spark_manager import spark_session
from pyspark.ml.feature import StringIndexerModel
from pyspark.sql.types import StringType, FloatType, StructType, StructField
