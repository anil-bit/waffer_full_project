from application_logging.logger import App_Logger
import sqlite3

class dboperation:

    #this class is usefull to handel all sql operation
    def __init__(self):
        self.path = "prediction_database"
        self.badfilepath = ""
        self.goodfilepath = ""
        self.logger = App_Logger()

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
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')
            #print(column_names.keys())
            for key in column_names.keys():
                type = column_names[key]
                #print(type)
                try:
                    conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {datatype}'.format(column_name=key,datatype = type))
                except:
                    conn.execute('CREATE TABLE Good_Raw_Data ({column_name} {datatype})'.format(column_name=key,datatype=type))
            conn.close()
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % databasename)
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % databasename)
            file.close()
            raise e








