import imp
import os
import re
import sys
import time
import uuid
from collections import namedtuple
from typing import List

import json
import pandas as pd
import requests

from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
from finance_complaint.entity.config_entity import DataIngestionConfig
from finance_complaint.entity.metdata_entity import DataIngestionMetadata
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from datetime import datetime

DownloadUrl = namedtuple("DownloadUrl", ["url", "file_path", "n_retry"])


class DataIngestion:
    # Used to download data in chunks.
    def __init__(self, data_ingestion_config: DataIngestionConfig, n_retry: int = 5, ):
        """
        data_ingestion_config: Data Ingestion config
        n_retry: Number of retry filed should be tried to download in case of failure encountered
        n_month_interval: n month data will be downloded
        """
        try:
            logger.info(f"{'>>' * 20}Starting data ingestion.{'<<' * 20}")
            self.data_ingestion_config = data_ingestion_config
            self.failed_download_urls: List[DownloadUrl] = []
            self.n_retry = n_retry

        except Exception as e:
            raise FinanceException(e, sys)

    def get_required_interval(self):
        start_date = datetime.strptime(self.data_ingestion_config.from_date, "%Y-%m-%d")
        end_date = datetime.strptime(self.data_ingestion_config.to_date, "%Y-%m-%d")
        n_diff_days = (end_date - start_date).days
        freq = None
        if n_diff_days > 365:
            freq = "Y"
        elif n_diff_days > 30:
            freq = "M"
        elif n_diff_days > 7:
            freq = "W"
        logger.debug(f"{n_diff_days} hence freq: {freq}")
        if freq is None:
            intervals = pd.date_range(start=self.data_ingestion_config.from_date,
                                      end=self.data_ingestion_config.to_date,
                                      periods=2).astype('str').tolist()
        else:

            intervals = pd.date_range(start=self.data_ingestion_config.from_date,
                                      end=self.data_ingestion_config.to_date,
                                      freq=freq).astype('str').tolist()
        logger.debug(f"Prepared Interval: {intervals}")
        if self.data_ingestion_config.to_date not in intervals:
            intervals.append(self.data_ingestion_config.to_date)
        return intervals

    def download_files(self, n_day_interval_url: int = None):
       
        try:
            required_interval = self.get_required_interval()
            logger.info("Started downloading files")
            for index in range(1, len(required_interval)):
                from_date, to_date = required_interval[index - 1], required_interval[index]
                logger.debug(f"Generating data download url between {from_date} and {to_date}")
                datasource_url: str = self.data_ingestion_config.datasource_url
                url=datasource_url.replace("<todate>", to_date).replace("<fromdate",from_date)
                logger.debug(f"URL:{url}")
                file_name = f"{self.data_ingestion_config.file_name}_{from_date}_{to_date}.json"
                file_path = os.path.join(self.data_ingestion_config.download_dir, file_name)
                download_url = DownloadUrl(url,file_path=file_path,n_retry=self.n_retry)
                self.download_data(download_url=download_url)
                logger.info(f"Download Completed Successfully")
        except Exception as e:
            raise FinanceException(e,sys)

    def convert_files_to_parquet(self, )->str:
        try:
             json_data_dir = self.data_ingestion_config.download_dir
             data_dir = self.data_ingestion_config.feeature_store_dir
             output_file = self.data_ingestion_config.file_name

             os.makedirs(data_dir, exist_ok=True)
             file_path = os.path.join(data_dir, f"{output_file}")
             if not os.path.exists(json_data_dir):
                return file_path
             for file_name in os.listdir(file_path)
             json_file_path = os.join.path(json_data_dir, file_name)
             logger.debug(f"Converting {json_file_path} files to parquet at {file_path}")
             df = spark_session.read.json(json_file_path)
             if df.count()>0:
                df.write.mode('append').parquet(file_path)
                return file_path
        except Exception as e:
            raise FinanceException(e,sys)
    
    def download_data(self, download_url=DownloadUrl):
        try:
            pass
        except Exception as e:
            raise FinanceException(e,sys)
            

