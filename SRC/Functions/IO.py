#Created 2021-09-03
#Copyright Spencer W. Leifeld


class CSV_File:
    
    def __init__(self, aSparkSession,  aPath, aSchema=None):
        self.spark = aSparkSession
        print('Now Reading: ' + aPath)
        self.csvFile = self.spark.read.csv(aPath, schema=aSchema, header=True, inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True, nullValue='NULL', nanValue=0, maxCharsPerColumn=128)

    def GetSparkDF(self):
        return self.csvFile

    def Schema(self):
        self.csvFile.printSchema()