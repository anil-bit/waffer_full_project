import json
import os
import re
import shutil
from datetime import datetime
from application_logging.logger import App_Logger
import pandas as pd
from os import listdir



class prediction_data_validation:
    def __init__(self,path):
        # This class shall be used for handling all the validation done on the Raw Prediction Data!!.
        self.batch_directory = path
        self.schema_path = "schema_prediction.json"
        self.logger = App_Logger()
    def valuesfromschema(self):
        #this method describes all the relevent information from the predifined schema file
        #outputfile: LengthOfDateStampInFile, LengthOfTimeStampInFile, NumberofColumns
        try:
            with open(self.schema_path,"r") as f:
                dic = json.load(f)
                #print(dic)
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

    def manualRegexCreation(self):

            """
                                          Method Name: manualRegexCreation
                                          Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                                      This Regex is used to validate the filename of the prediction data.
                                          Output: Regex pattern
                                          On Failure: None

                                           Written By: iNeuron Intelligence
                                          Version: 1.0
                                          Revisions: None

                                                  """
            regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
            return regex
    def validationfilenameraw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        #this function verfy the prediction csv file by regex function if nnot okm it will mov to bad raw data
        #delete the exsisting good trainig data  and bad training data
        self.deleteexsistingbaddatatrainingfolder()
        self.deleteexsistinggooddatatrainingfolder()
        self.createdirectoryforgoodbadrawdata()
        onlyfiles = [f for f in listdir(self.batch_directory)]
        try:
            f = open("prediction_logs/nameValidationLog.txt", 'a+')
            for file_name in onlyfiles:
                #print(file_name)
                if (re.match(regex,file_name)):
                    splitadot = re.split(".csv",file_name)
                    splitadot = (re.split("_", splitadot[0]))
                    if len(splitadot[1]) == LengthOfDateStampInFile:
                        if len(splitadot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Prediction_Batch_files/" + file_name, "Prediction_Raw_Files_Validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % file_name)
                        else:
                            shutil.copy("Prediction_Batch_files/" + file_name, "Prediction_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % file_name)
                    else:
                        shutil.copy("Prediction_Batch_files/" + file_name, "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f,"invalid file name file moved to bad rawfolder:: %s" % file_name)
                else:
                    shutil.copy("Prediction_Batch_files/" + file_name, "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f,"invalid file name file moved to bad rawfolder:: %s" % file_name)
            f.close()
        except Exception as e:
            f = open("prediction_logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e




    def deleteexsistingbaddatatrainingfolder(self):
        # this method delete exsisting bad data trainig folder

        try:
            path = "Prediction_Raw_Files_Validated/"
            if os.path.isdir(path+"Bad_Raw/"):
                shutil.rmtree(path + "Bad_Raw/")
                file = open("prediction_logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"succesfully deleted the file")
                file.close()

        except OSError as s:
            file = open("prediction_logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"error while deleting the directory : %s" %s)
            file.close()
            raise OSError

    def deleteexsistinggooddatatrainingfolder(self):
        #this metrhod deletes the directory made to store the bad data

        try:
            path = "Prediction_Raw_Files_Validated/"
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path + "Good_Raw/")
                file = open("prediction_logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"badraw directory deleted before starting validation!!!")
                file.close()

        except OSError as s:
            file = open("prediction_logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"error during deleting directory: %s"%s)
            file.close()
            raise OSError








    def createdirectoryforgoodbadrawdata(self):

        #description: this method create directories for good data ands bad data for prediction data
        #output: none

        try:
            path = os.path.join("Prediction_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Prediction_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"error while creating file %s:" % ex)
            file.close()
            raise OSError

    

'''    

    def moveBadFilesToArchiveBad(self):

        #this method dekets the bad data after it is send to bad data folder from raw data
        now = datetime.now()
        date = now.date()
        time = now.strftime("%h%m%s")

        try:
            path = "PredictionArchivedBadData"
            if not os.path.isdir(path):
                os.makedirs(path)
            source = "Prediction_Raw_Files_Validated/bad_raw/"
            dest = "PredictionArchivedBadData/BadData_" + str(date)+"_"+str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source + f,dest)

            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"bad raw file has moved to archieve")
            path = "Prediction_Raw_Files_Validated/"
            if os.path.isdir(path + "bad_raw"):
                shutil.rmtree(path + "bad_raw")
            self.logger.log(file,"bad raw data folder has been deleted")
            file.close()
        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"error while moving the file: %s" %e)
            file.close()
            raise OSError

    








    def validatecloumnlength(self,NumberofColumns):
        #have to check the no of columns is present or not
        #if present send the data to good raw data
        #if not presernt send it bad raw data
        #output: none
        try:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"coloumn length validation started")
            for file in os.listdir("Prediction_Raw_Files_Validated/Good_Raw/"):
                csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/"+file)

'''




























