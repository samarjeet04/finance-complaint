from collections import namedtuple

from collections import namedtuple

TrainingPipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])
DataIngestionConfig = namedtuple("DataIngestionConfig", ["from_date",
                                                         "to_date",
                                                         "data_ingestion_dir",
                                                         "download_dir",
                                                         "file_name",
                                                        "feeature_store_dir",
                                                        "failed_dir",
                                                        "metadata_file_path",])

DataValidationConfig = namedtuple("DataValidationConfig",["accepted_data_dir", "rejected_data_dir", "file_name"])
DataTransformationConfig = namedtuple('DataTransformationConfig', ["file_name, export_pipeline_dir", 
                                                    "transformd_train_dir", "transformed_test_dir", "test_size"])