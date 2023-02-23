from calendar import day_abbr
from h11 import Data
from pyspark import keyword_only
from pyspark.ml import Transformer, param
from pyspark.ml.param.shared import HasInputCols, HasOutputCols, Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml import Estimator
from pyspark.sql import DataFrame
from finance_complaint.exception import FinanceException 
from finance_complaint.logger import logger
from pyspark.sql.functions import count, desc, col, abs
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

    @keyword_only
    def setParams(self, inputCols:List[str]=None, outputCols:List[str]=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setinputCols(self, value:List[str]):
        """
        sets the value for thr att:'inputCol'
        """
        return self._set(inputCol=value)

    def setoutputCol(self, value:List[str]):
        """
        sets the value for the att:'ouputcol'
        """
        return self._set(ouputCols=value)

    def _fit(self, dataframe:DataFrame):
        input_columns = self.getInputCols()
        print(f"Input Cols: {input_columns}")
        output_columns = self.getOutputCols()
        print(f"Output Cols: {output_columns}")
        replace_info = []
        for column, new_column in zip(input_columns, output_columns):
            freq = dataframe.select(col(column).
                                      alias(f'g_{column}')).groupby(f'g_{column}').count().withColumn(new_column, col('count'))
        self.setfrequencyInfo(frequencyInfo=replace_info)
        estimator = FrequencyEncoderModel(inputCols=input_columns, outputCols=output_columns)
        estimator.setfrequencyInfo(frequencyInfo=replace_info)
        return estimator

    
class FrequencyEncoderModel(FrequencyEncoder, Transformer):
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None):
        super(FrequencyEncoder, self).__init__(inputCols=inputCols, outputCols=inputCols)

    def _transform(self, dataframe:DataFrame):
        inputCols = self.getInputCols()
        outputCols = self.getOutputCols()

        print(f" Input Cols: {inputCols}")
        print(f"OuputCols: {outputCols}")

        freqInfo = self.getfrrequencyInfo()
        for in_col, out_col, freq_info in zip(inputCols, outputCols, freqInfo):
            frequeny_dataframe:DataFrame = spark_session.createDataFrame(freq_info)
            columns = frequeny_dataframe.columns
            dataframe = dataframe.join(frequeny_dataframe, on=dataframe[in_col]==frequeny_dataframe[columns[0]])

            dataframe=dataframe.drop(columns[0])
            if out_col not in dataframe:
                dataframe = dataframe.withColumn(out_col, col(columns[1]))
                dataframe = dataframe.drop(columns[1])
                return dataframe



class DerivedFeatureGenerator(Transformer, HasInputCols, HasOutputCols,
                              DefaultParamsReadable, DefaultParamsWritable):

    @keyword_only
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        super(DerivedFeatureGenerator, self).__init__()
        kwargs = self._input_kwargs
        # self._set(**kwargs)
        self.second_within_day = 60 * 60 * 24
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCols=value)

    def _fit(self, dataframe: DataFrame):
        return self

    def _transform(self, dataframe: DataFrame):
        inputCols = self.getInputCols()

        for column in inputCols:
            dataframe = dataframe.withColumn(column,
                                             col(column).cast(TimestampType()))

        dataframe = dataframe.withColumn(self.getOutputCols()[0], abs(
            col(inputCols[1]).cast(LongType()) - col(inputCols[0]).cast(LongType())) / (
                                             self.second_within_day))
        return dataframe


class FrequencyImputer(
    Estimator, HasInputCols, HasOutputCols,
    DefaultParamsReadable, DefaultParamsWritable):
    topCategorys = Param(Params._dummy(), "getTopCategorys", "getTopCategorys",
                         typeConverter=TypeConverters.toListString)

    @keyword_only
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        super(FrequencyImputer, self).__init__()
        self.topCategorys = Param(self, "topCategorys", "")
        self._setDefault(topCategorys="")
        kwargs = self._input_kwargs
        print(kwargs)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setTopCategorys(self, value: List[str]):
        return self._set(topCategorys=value)

    def getTopCategorys(self):
        return self.getOrDefault(self.topCategorys)

    def setInputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCols=value)

    def setOutputCols(self, value: List[str]):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCols=value)

    def _fit(self, dataset: DataFrame):
        inputCols = self.getInputCols()
        topCategorys = []
        for column in inputCols:
            categoryCountByDesc = dataset.groupBy(column).count().filter(f'{column} is not null').sort(
                desc('count'))
            topCat = categoryCountByDesc.take(1)[0][column]
            topCategorys.append(topCat)

        self.setTopCategorys(value=topCategorys)

        estimator = FrequencyImputerModel(inputCols=self.getInputCols(),
                                          outputCols=self.getOutputCols())

        estimator.setTopCategorys(value=topCategorys)
        return estimator


class FrequencyImputerModel(FrequencyImputer, Transformer):

    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None, ):
        super(FrequencyImputerModel, self).__init__(inputCols=inputCols, outputCols=outputCols)

    def _transform(self, dataset: DataFrame):
        topCategorys = self.getTopCategorys()
        outputCols = self.getOutputCols()

        updateMissingValue = dict(zip(outputCols, topCategorys))

        inputCols = self.getInputCols()
        for outputColumn, inputColumn in zip(outputCols, inputCols):
            dataset = dataset.withColumn(outputColumn, col(inputColumn))
            # print(dataset.columns)
            # print(outputColumn, inputColumn)

        dataset = dataset.na.fill(updateMissingValue)

        return dataset