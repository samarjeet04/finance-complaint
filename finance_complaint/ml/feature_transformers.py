from pyspark import keyword_only
from pyspark.ml import Transformer, param
from pyspark.ml.param.shared import HasInputCols, HasOutputCols, Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml import Estimator
from pyspark.sql import DataFrame
from finance_complaint.exception import FinanceException 
from finance_complaint.logger import logger
from pyspark.sql.functions import desc, col, abs
from typing import List
from pyspark.sql.types import TimestampType, LongType
from finance_complaint.config.spark_manager import spark_session

class FrequencyEncoder(Estimator, HasInputCols, HasOutputCols,
                       DefaultParamsReadable, DefaultParamsWritable):
    frequencyInfo = Param(Params._dummy(), "getfrequencyInfo", "getfrequencyInfo",
                          typeConverter=TypeConverters.toList)

    @keyword_only
    def __init__(self, inputCols:List[str]=None, outputCols:List[str]=None):
        super(FrequencyEncoder, self).__init__()
        kwargs = self._input_kwargs

        self.frequencyInfo = Param(self, "frequencyInfo","")
        self._setdefault(frequencyInfo = "")

        self.setParams(**kwargs)

    def setfrequencyInfo(self, frequencyInfo:List):
        return self._set(frequencyInfo=frequencyInfo)

    def getfrrequencyInfo(self):
        return self.getOrDefault(self.frequencyInfo)