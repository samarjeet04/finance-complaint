import os
import sys
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.entity.artifact_entity import DataValidationArtifact, ModelTrainerArtifact, ModelEvaluationArtifact
from finance_complaint.entity.config_entity import ModelEvaluationConfig
from finance_complaint.entity.schema import FinanceDataSchema
from pyspark.sql import DataFrame
from pyspark.ml.pipeline import PipelineModel
from finance_complaint.utils import get_score
from finance_complaint.config.spark_manager import spark_session
from pyspark.ml.feature import StringIndexerModel
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from finance_complaint.ml.estimator import S3FinanceEstimator
from finance_complaint.data_access.model_eval_artifact import ModelEvaluationArtifactData

class ModelEvaluation:
    def __init(self, data_validation_artifact:DataValidationArtifact,
                     model_trainer_artifact:ModelTrainerArtifact,
                     model_eval_config:ModelEvaluationConfig,
                     schema = FinanceDataSchema()):
        try:
            self.model_eval_artifact_data = ModelEvaluationArtifactData
            self.data_validation_artifact = data_validation_artifact
            self.model_trainer_artifact = model_trainer_artifact
            self.model_eval_config = model_eval_config
            self.schema = schema
            self.bucket_name = self.model_eval_config.bucket_name
            self.s3_model_dir_key = self.model_eval_config.model_dir
            self.s3_finance_estimator = S3FinanceEstimator()
        except Exception as e:
            raise FinanceException(e, sys)

    def read_data(self)->DataFrame:
        try:
            file_path = self.data_validation_artifact.accepted_dir
            dataframe:DataFrame = spark_session.read.parquet(file_path)
            return dataframe
        except Exception as e:
            raise FinanceException(e,sys)

    def evaluate_trained_model(self)->ModelEvaluationArtifact:
        try:
            is_model_accepted, is_model_active = False, False
            trained_model_file_path = self.model_trainer_artifact.model_trainer_ref_artifact.trained_model_file_path
            label_indexer_model_path = self.model_trainer_artifact.model_trainer_ref_artifact.label_indexer_model_file_path

            label_indexer_model = StringIndexerModel.load(label_indexer_model_path)
            trained_model = PipelineModel.load(trained_model_file_path)

            dataframe:DataFrame = self.read_data()
            dataframe = label_indexer_model.transform(dataframe)

            best_model_path = self.s3_finance_estimator.get_latest_model_path()
            trained_model_dataframe = trained_model.transform(dataframe)
            best_model_dataframe = self.s3_finance_estimator.transform(dataframe)

            trained_model_f1_score = get_score(dataframe=trained_model_dataframe , metric_name="f1", 
                                               label_col=self.schema.target_indexed_label, prediction_col=self.schema.prediction_column_name)

            
            best_model_f1_score = get_score(dataframe=best_model_dataframe, metric_name="f1",
                                             label_col=self.schema.target_indexed_label, prediction_col=self.schema.prediction_column_name)

            logger.info(f"Trained model f1 score: {trained_model_f1_score}, and Best model f1 score: {best_model_f1_score}")
            changed_acc = trained_model_f1_score - best_model_f1_score

            if changed_acc>=self.model_eval_config.threshold:
                is_model_accepted, is_model_active = True, True

            model_evaluation_artifact = ModelEvaluationArtifact(model_accepted = is_model_accepted,
                                                                changed_accuracy = changed_acc,
                                                                trained_model_path = trained_model_file_path,
                                                                best_model_path = best_model_path,
                                                                model_active = is_model_active)
            return model_evaluation_artifact
        except Exception as e:
            raise FinanceException(e,sys)

    def initiate_model_evluation(self)->ModelEvaluationArtifact:
        try:
            model_accepted = True
            is_model_active = True

            if not self.s3_finance_estimator.is_model_available(key=self.s3_finance_estimator.s3_key):
                latest_model_path = None
                trained_model_path = self.model_trainer_artifact.model_trainer_ref_artifact.trained_model_file_path
                model_evaluation_artifact = ModelEvaluationArtifact(model_accepted=model_accepted,
                                                                    changed_accuracy=0.0,
                                                                    trained_model_path=trained_model_path,
                                                                    best_model_path=latest_model_path,
                                                                    model_active=is_model_active
                                                                    )
            else:
                model_evaluation_artifact = self.evaluate_trained_model()

            logger.info(f"Model evaluation artifact: {model_evaluation_artifact}")
            self.model_eval_artifact_data.save_eval_artifact(model_eval_artifact=model_evaluation_artifact)
            return model_evaluation_artifact
        except Exception as e:
            raise FinanceException(e, sys)
                