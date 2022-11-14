from application_logging.logger import app_logger

class datatransformpredict:

        #this class is used to transform the good raw training data
        #beform entetring into database


      def __init__(self):
          self.gooddatapath = "Prediction_Raw_Files_Validated/Good_Raw"
          self.logger = app_logger()


