from application_logging.logger import app_logger
import sqlite3

class dboperation:

    #this class is usefull to handel all sql operation
    def __init__(self):
        self.path = "prediction_database"
        self.badfilepath = ""
        self.goodfilepath = ""
        self.logger = app_logger()

    def databaseconnection(self,databasename):

        #this method create the data base ,if already exsisst then opens teh connection
        #output: connection  to db
        #on failure: raisew connection error
        try:
            conn = sqlite3.connect(self.path+databasename+".db")
            file = open("prediction_logs/DataBaseConnectionLog.txt","a+")
            self.logger.log(file,"opened %s database sucesfully" %databasename)
            file.close()
        except ConnectionError:
            file = open("prediction_logs/DataBaseConnectionLog.txt","a+")
            self.logger.log(file,"error while connecting the database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn


    def createtabledb(self,databasename,column_names):
        #this method creates the table in database which is used to insert gooddata after raw data validation
        #output: none
        try:
            conn = self.databaseconnection(databasename)
            conn`.execute`('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type = column_names[key]





