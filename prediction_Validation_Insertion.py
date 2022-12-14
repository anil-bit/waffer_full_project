from prediction_raw_data_validation.predictiondatavalidation import prediction_data_validation
from datatransformation_prediction.datatransformationprediction import datatransformpredict
from datatypevalidation_insertion_prediction.datatypevalidationprediction import dboperation


class pred_validation:
    def __init__(self,path):
        self.raw_data = prediction_data_validation(path)
        self.dataTransform = datatransformpredict()
        self.dboperation = dboperation()

    def prediction_validation(self):

        #extract the values from predicton schema
        LengthOfDateStampInFile,LengthOfTimeStampInFile,NumberofColumns,column_names = self.raw_data.valuesfromschema()
        print(LengthOfDateStampInFile)

        #getting the regex defined to validate filename
        regex = self.raw_data.manualRegexCreation()
        print(regex)
       
        #validating files name of prediction file
        self.raw_data.validationfilenameraw(regex, LengthOfDateStampInFile,LengthOfTimeStampInFile)

        #validating column length in the file
        self.raw_data.validatecloumnlength(NumberofColumns)
        #validating mmissing value in columns
        self.raw_data.validatemissingvalueinwholecolumn()

        #replacing missing values with null
        #self.dataTransform.replacingmisingwithnull()
        #create the database with given name if present open the connection and create a table as per givn schema
        self.dboperation.createtabledb("prediction",column_names)








