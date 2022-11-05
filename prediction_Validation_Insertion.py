from prediction_raw_data_validation.predictiondatavalidation import prediction_data_validation


class pred_validation:
    def __init__(self,path):
        self.raw_data = prediction_data_validation(path)

