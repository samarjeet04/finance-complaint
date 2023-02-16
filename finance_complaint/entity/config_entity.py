from collections import namedtuple

TrainingPipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])
DataIngestionConfig = namedtuple("DataIngestionConfig", ["from_date",
                                                         "to_date",
                                                         "data_ingestion_dir",
                                                         "download_dir",
                                                         "file_name",
                                                        "feeature_store_dir",
                                                        "download_dir",
                                                        "failed_dir",
                                                        "metadata_file_path",



)