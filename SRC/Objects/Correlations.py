#Created 2021-09-05
#Copyright Spencer W. Leifeld

#Public Library Imports
from typing import ClassVar
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from abc import ABC
from abc import abstractclassmethod as abstract

class Vector_Builder(ABC):

    @abstract
    def __init__(self, aInputColumns, aOutputColumn, aSuperDF):
        assembly = VectorAssembler(
            inputCols=aInputColumns,
            outputCol=aOutputColumn )
        self.__DF = assembly.transform(aSuperDF)

    
    def GetVector(self):
        return self.__DF
            

class Farming_Vector(Vector_Builder):

    def __init__(self, aDF):
        super().__init__(['avg(Pop_Loss)', 'avg(Farming)'], 'Vector', aDF)
        

class Manufacturing_Vector(Vector_Builder):

    def __init__(self, aDF):
        super().__init__(['avg(Pop_Loss)', 'avg(Manufacturing)'], 'Vector', aDF)

class Recreation_Vector(Vector_Builder):

    def __init__(self, aDF):
        super().__init__(['avg(Pop_Loss)', 'avg(Recreation)'], 'Vector', aDF)

class Mining_Vector(Vector_Builder):

    def __init__(self, aDF):
        super().__init__(['avg(Pop_Loss)', 'avg(Mining)'], 'Vector', aDF)

class Corr_Director:
    def __init__(self, aDF, aCorrelationType='pearson'):
        vectors = []
        self.__correlations = []

        vectors.append(Farming_Vector(aDF))
        vectors.append(Manufacturing_Vector(aDF))
        vectors.append(Recreation_Vector(aDF))
        vectors.append(Mining_Vector(aDF))

        for v in vectors:
            self.__correlations.append(Correlation.corr(v.GetVector(), 'Vector', aCorrelationType).collect())

    def __str__(self):
        rString = 'Farming: '
        rString += str(self.__correlations[0]).split(',')[4] + '\n'
        rString += 'Manufacturing: '
        rString += str(self.__correlations[1]).split(',')[4] + '\n'
        rString += 'Recreation: '
        rString += str(self.__correlations[2]).split(',')[4] + '\n'
        rString += 'Mining: '
        rString += str(self.__correlations[3]).split(',')[4] + '\n'

        return rString


