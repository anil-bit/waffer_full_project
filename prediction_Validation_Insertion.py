from prediction_raw_data_validation.predictiondatavalidation import prediction_data_validation
from datatransformation_prediction.datatransformationprediction import datatransformpredict


class pred_validation:
    def __init__(self,path):
        self.raw_data = prediction_data_validation(path)
        self.dataTransform = datatransformpredict()
        self.dboperation = dboperation()


