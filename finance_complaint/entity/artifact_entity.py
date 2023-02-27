from collections import namedtuple
from datetime import datetime

DataIngestionArtifact = namedtuple("DataIngestionArtifact",
                                   ["feeature_store_file_path", "metadata_file_path", "download_dir"])

DataValidationArtifact = namedtuple("DataValidationArtifact",
                                    ["accepted_dir", "rejected_dir"])

DataTransfomrationArtifact = namedtuple("DataTransformationArtifact", ["transformed_train_file_path", "transformed_test_file_path",
                                        "exported_pipeline_file_path"])

PartialModelTrainerRefArtifact = namedtuple("PartialModelTrainerRefArtifact", ["trained_model_file_path","label_indexer_model_file_path"])

PartialModelTrainerMetricrtifact = namedtuple("PartialModelTrainerMetricArtifact", ["f1_score", "precision_score", "recall_score"])

