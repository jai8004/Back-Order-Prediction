import sqlite3
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from applicationLogger.logger import AppLogger


class RawDataValidation:
    """
    This Class Handles all the validation done on the  raw training data.

    """

    def __init__(self, path):
        self.batch_directory = path
        self.schema_path = 'schema_training.json'
        self.logger = AppLogger()

    def valuesFromSchema(self):
        """
        This method is used to get the values from schema

        :return:
        """
        try:
            with open(self.schema_path,'r') as f:
                dic = json.load(f)
                f.close()

            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberOfColumns = dic['NumberOfColumns']
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t " + "NumberofColumns:: %s" % NumberOfColumns + "\n"
            self.logger.log(file, message)

            file.close()


        except ValueError:

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')

            self.logger.log(file, "ValueError:Value not found inside schema_training.json")

            file.close()

            raise ValueError


        except KeyError:

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')

            self.logger.log(file, "KeyError:Key value error incorrect key passed")

            file.close()

            raise KeyError


        except Exception as e:

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')

            self.logger.log(file, str(e))

            file.close()

            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names ,NumberOfColumns

    def manualRegexCreation(self):
        """
        Manaually  defined regex for checking the file name
        :return:
        """
        regex = "['backorder']+['\_'']+[\d]+[\d]+\.csv"

        return  regex

    def createDirectoryForGoodDataBadData(self):
        """
        Creates directory for good data and bad data after validation test is performed
        :return:
        """
        try:
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while creating Good/Bad Directory %s:" % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):
        """
        This method deletes the directory made  to store the Good Data
        after loading the data in the table. Once the good files are
        loaded in the DB,deleting the directory ensures space optimization.

        :return:
        """
        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting GoodRaw Directory : %s" %s)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):

        """
        This method deletes the directory made to store the bad training data.
        """

        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Bad Raw Directory : %s" %s)
            file.close()
            raise OSError

    def moveBadFilestoArchivesBad(self):
        """
        This files send the bad data to archive bad data folder which is sent to the client
        for invalid data issue.

        :return:
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")

        try :
            source = 'Training_Raw_File_Validated/Bad_Raw'
            if os.path.isdir(source):
                path = 'TrainingArchiveBadData'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_'+str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source +f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Delted successfully")
                file.close()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while moving bad files to archive :: %s" %e)
            file.close()
            raise e










