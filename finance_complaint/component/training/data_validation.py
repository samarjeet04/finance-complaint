import os,sys
from collections import namedtuple
from typing import List, Dict
from pyspark.sql import DataFrame, dataframe
from pyspark.sql.functions import col,lit
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.entity.schema import FinanceDataSchema
from finance_complaint.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from finance_complaint.entity.config_entity import DataValidationConfig

COMPLAINT_TABLE  = "complaint"
ERROR_MESSAGE= "error_msg"
MissingReport = namedtuple("MissingReport",["total_row", "missing_row", "missing_percentage"])

class DataValidation(FinanceDataSchema):

    def __init__(self,
                 data_validation_config: DataValidationConfig,
                 data_ingestion_artifact: DataIngestionArtifact,
                 table_name: str = COMPLAINT_TABLE,
                 schema=FinanceDataSchema()
                 ):
        try:
            super().__init__()
            self.data_ingestion_artifact: DataIngestionArtifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.table_name = table_name
            self.schema = schema
        except Exception as e:
            raise FinanceException(e, sys) from e

    def read_data(self)->DataFrame:
        try:
             dataframe:DataFrame = spark_session.read.parquet(
             self.data_ingestion_artifact.feeature_store_file_path).limit(10000)
             logger.info(f"Dataframe is created using the file: {self.data_ingestion_artifact.feeature_store_file_path}")
             logger.info(f"No. of rows: {dataframe.count()}, and columns: {len(dataframe.columns)}")
             return dataframe
        except Exception as e:
            raise FinanceException(e,sys)


    @staticmethod
    def get_missing_report(dataframe:DataFrame)->Dict[str, MissingReport]:
        try:
            missing_report:Dict[str:MissingReport]=dict()
            logger.info(f"Preparing missing reports for each column")
            no_of_rows= dataframe.count()

            for column in dataframe.columns:
                missing_row = dataframe.filter(f"{column} is null").count()
                missing_percentage = (missing_row*100)/no_of_rows
                missing_report[column] = MissingReport(
                                         total_row=no_of_rows,
                                         missing_row=missing_row,
                                         missing_percentage=missing_percentage
                )
            logger.info(f"Missing report prepared: {missing_report}")
            return missing_report
        except Exception as e:
            raise FinanceException(e,sys)

    
    def get_unwanted_and_hisgh_missing_value_cols(self, dataframe:DataFrame, threashlod: float=0.2)->List[str]:
        try:
            missing_report:Dict[str, MissingReport] = self.get_missing_report(dataframe=dataframe)
            unwanted_columns:List[str] = self.schema.unwanted_columns

            for column in missing_report:
                if missing_report[column].missing_percentage>(threashlod*100):
                    unwanted_columns.append(column)
                    logger.info(f"Missing report {column}: [{missing_report[column]}]")
            unwanted_columns=list(set(unwanted_columns))
            return unwanted_columns
        except Exception as e:
            raise FinanceException(e,sys)

    
    def drop_unwanted_columns(self, dataframe:DataFrame)->DataFrame:
        try:
            unwanted_columns:List=self.get_unwanted_and_hisgh_missing_value_cols(dataframe=dataframe)
            logger.info(f"Dropping feature: {','.join(unwanted_columns)}")
            unwanted_dataframe = dataframe.select(unwanted_columns)

            unwanted_dataframe = unwanted_dataframe.withColumn(ERROR_MESSAGE, lit("Contains too may missing values"))
            
            rejected_dir = os.path.join(self.data_validation_config.rejected_data_dir, "missing data")
            os.makedirs(rejected_dir, exist_ok=True)
            file_path = os.path.join(rejected_dir, self.data_validation_config.file_name)
            unwanted_dataframe.write.mode("append").parquet(file_path)
            dataframe:DataFrame=dataframe.drop(*unwanted_columns)
            logger.info(f"Remaining number of columns: [{dataframe.columns}]")
            return dataframe
        except Exception as e:
            raise FinanceException(e,sys)

    
    @staticmethod
    def get_unique_values_for_each_cols(dataframe:DataFrame)->None:
        try:
            for column in dataframe.columns:
                n_unique = dataframe.select(col(column)).distinct().count()
                n_missing = dataframe.filter(col(column)).isNull().count()
                missing_percentage:float = (n_missing*100)/dataframe.columns
                logger.info(f"Column {column} contains {n_unique} values and missing percentage of {missing_percentage}%")
        except Exception as e:
            raise FinanceException(e,sys)

    
    def is_required_columns_exist(self, dataframe: DataFrame):
        try:
            columns = list(filter(lambda x: x in self.schema.required_columns,
                                  dataframe.columns))

            if len(columns) != len(self.schema.required_columns):
                raise Exception(f"Required column missing\n\
                 Expected columns: {self.schema.required_columns}\n\
                 Found columns: {columns}\
                 ")
        except Exception as e:
            raise FinanceException(e,sys)

    
    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            logger.info(f"Initiating data preprocessing")
            dataframe:DataFrame = self.read_data()

            logger.info(f"Dropping unwanted columns")
            dataframe:DataFrame = self.drop_unwanted_columns(dataframe=dataframe)

            """
            validation to ensure that all required columns exist
            """
            self.is_required_columns_exist(dataframe=dataframe)

            logger.info(f"Saving preprocessed data.")
            print(f"Row: [{dataframe.count()}] Column: [{len(dataframe.columns)}]")
            print(f"Expected Column: {self.required_columns}\nPresent Columns: {dataframe.columns}")

            os.makedirs(self.data_validation_config.accepted_data_dir, exist_ok=True)
            accepted_file_path = os.path.join(self.data_validation_config.accepted_data_dir,
                                              self.data_validation_config.file_name
                                              )
            dataframe.write.parquet(accepted_file_path)

            artifact = DataValidationArtifact(accepted_file_path=accepted_file_path,
                                              rejected_dir=self.data_validation_config.rejected_data_dir
                                              )
            logger.info(f"Data validation artifact: [{artifact}]")
            return artifact
        except Exception as e:
            raise FinanceException(e,sys)

    


