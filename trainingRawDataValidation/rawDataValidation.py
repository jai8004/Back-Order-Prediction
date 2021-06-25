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
            LengthOfDateStampInFile = dic ['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic ['LengthOfTimeStampInFile']
            column_names = dic ['ColName']
            NumberOFColumns = dic ['NumberOFColumns']
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
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

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns




