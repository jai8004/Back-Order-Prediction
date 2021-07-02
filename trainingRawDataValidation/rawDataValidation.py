import json
import os
import re
import shutil
from datetime import datetime
from os import listdir

import pandas as pd

from applicationLogger.logger import AppLogger


# from trainingRawDataValidation.rawDataValidation import RawDataValidation
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
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()

            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberOfColumns = dic['NumberOfColumns']
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t " + "NumberofColumns:: %s" % NumberOfColumns + "\n" + "Column Names:: %s" % column_names + "\n"
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

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberOfColumns

    def manualRegexCreation(self):
        """
        Manaually  defined regex for checking the file name
        :return:
        """
        regex = "['backOrder']+['\_'']+[\d_]+[\d]+\.csv"

        return regex

    def createDirectoryForGoodBadRawData(self):
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
                self.logger.log(file, "GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting GoodRaw Directory : %s" % s)
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
                self.logger.log(file, "BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Bad Raw Directory : %s" % s)
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

        try:
            source = 'Training_Raw_File_Validated/Bad_Raw'
            if os.path.isdir(source):
                path = 'TrainingArchiveBadData'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date) + "_" + str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file, "Bad Raw Data Folder Delted successfully")
                file.close()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive :: %s" % e)
            file.close()
            raise e



    def validationFileNameRaw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        """

        :param regex:
        :param LengthOfDateStampInFile:
        :param LengthOfTimeStampInFile:
        :return:

        This function validates the name of the training csv files as per given name in the schema!
        Regex pattern is used to do the validation.If name format do not match the file is moved
        to Bad Raw Data folder else in Good raw data.
        """

        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        # create new directories
        self.createDirectoryForGoodBadRawData()
        onlyFiles = [f for f in listdir(self.batch_directory)]
        try:
            f = open("Training_Logs/nameValidation.txt", 'a+')
            for filename in onlyFiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_Validated/Good_Raw")
                            self.logger.log(f, "Valid File Name ! Moving file to Good Raw Folder :: %s" % filename)
                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f,
                                            "Invalid File Name! TimeStamp Error! File moved to Bad Raw Folder :: %s" % filename)

                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,
                                        "Invalid File Name! DateStamp Error ! File moved to Bad Raw Folder :: %s" % filename)

                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f,
                                    "Invalid File Name! Regex Match Error! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def validateColumnLength(self, NumberOfColumns):
        """
        Description: This function validates the number of columns in the csv files.
                                   It is should be same as given in the schema file.
                                   If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                   If the column number matches, file is kept in Good Raw Data for processing.


        :param self:
        :param NumberOfColumns:
        :return:
        """
        try:
            f = open("Training_Logs/columnValidationLog.txt", "a+")
            self.logger.log(f, "Columns Lenght Validation Started !")
            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == NumberOfColumns:
                    pass
                else:
                    shutil.move("Training_Raw_files_Validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Lenght for the file !! File moved to Bad_Raw folder ::%s" % file)
            self.logger.log(f, "Column Length Validation Completed !")
        except OSError:
            f = open("Training_Logs/columnValidationLof.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError

        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured :: %s" % e)
            f.close()
            raise e
        f.close()

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               Such files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception


        """
        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,
                                        "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count == 0:
                    csv.rename(columns={"Unnamed: 0": "Index"}, inplace=True)
                    csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)

        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()
