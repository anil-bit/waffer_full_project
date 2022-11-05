import json

from application_logging.logger import app_logger



class prediction_data_validation:
    def __init__(self,path):
        # This class shall be used for handling all the validation done on the Raw Prediction Data!!.
        self.batch_directory = path
        self.schema_path = "schema_prediction.json"
        self.logger = app_logger()
    def valuesfromschema(self):
        #this method describes all the relevent information from the predifined schema file
        #outputfile: LengthOfDateStampInFile, LengthOfTimeStampInFile, NumberofColumns
        try:
            with open(self.schema_path,"r") as f:
                dic = json.load(f)
                print(dic)
                f.close()
            pattern = dic["SampleFileName"]
            LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]
            LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]
            NumberofColumns = dic["NumberofColumns"]
            column_names = dic["ColName"]

            file = open("Training_logs/valuesfromSchemaValidationLog.txt","a+")
            message = "LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "LengthOfTimeStampInFile:: %s" %LengthOfTimeStampInFile + "NumberofColumns:: %s" %NumberofColumns + "\n"
            self.logger.log(file,message)
            file.close()

        except ValueError:
            file = open("prediction_logs/valuesfromSchemaValidationLog.txt","a+")
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("prediction_logs/valuesfromSchemaValidationLog.txt","a+")
            self.logger.log(file,"KeyError:Key value error incorrect key passed")
            raise KeyError
        except Exception as e:
            file = open("prediction_logs/valuesfromSchemaValidationLog.txt","a+")
            self.logger.log(file,str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile,LengthOfTimeStampInFile,NumberofColumns,column_names











