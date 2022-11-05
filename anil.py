



from application_logging.logger import App_Logger
import json


#this class will handel all the validation done by the raw prediction data

class prediction_data_validation:

    def __init__(self):
        #self.batch_directory = path
        self.schema_path = "schema_prediction.json"
        self.logger = App_Logger()

    #this method extracts all the relevemty defivnation from the schema file
    def valuesFromSchema(self):
        try:
            with open(self.schema_path, 'r') as f:
                 dic = json.load(f)
                 f.close()

            pattern = dic["SampleFileName"]
            LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]
            LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]
            NumberofColumns = dic["NumberofColumns"]
            column_names = dic["ColName"]
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = "LengthofDateStampFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" %LengthOfTimeStampInFile + "\t" + "NumberofColumns:: %s" %NumberofColumns + "\n"
            self.logger.log(file,message)


        except ValueError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()

        except KeyError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"KeyError:Key value error incorrect key passed")
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt","a+")
            self.logger.log(file,str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns













d =prediction_data_validation()
d.valuesFromSchema()

