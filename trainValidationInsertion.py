from applicationLogger.logger import AppLogger
from trainingDataTransformation.dataTransformation import DataTransform
from trainingDataTypeValidationDb.dataTypeValidation import DbOperation
from trainingRawDataValidation.rawDataValidation import RawDataValidation


class TrainValidation:

    def __init__(self, path):
        self.raw_data = RawDataValidation(path)
        self.data_transform = DataTransform()
        self.dbOperation = DbOperation
        self.log_file = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.logger = AppLogger()

    def trainingDataValidation(self):
        try:
            self.logger.log(self.log_file, "Starting raw batch data validation for training !")

            # extracting values from  training schema
            lengthDateStampInFile, LengthOfTimeStampInFile, column_names, no_of_columns = self.raw_data.valuesFromSchema()


            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()

            # validating filenames of prediction files
            self.raw_data.validationFileNameRaw(regex, lengthDateStampInFile, LengthOfTimeStampInFile)

            # validating column length in the file
            self.raw_data.validateColumnLength(no_of_columns)

            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()

            self.logger.log(self.log_file, "Raw batch data validation for training Completed !")

            self.logger.log(self.log_file, "Creating the database on the given schema !")

            # creating database with given name , if present open the connection !
            # creating table with columns given in schema
            print("db")
            print(column_names)

            self.dbOperation.createTableDb('Training', column_names)
            self.logger.log(self.log_file, "Table Creation Completed !")
            self.logger.log(self.log_file, "Insertion of Data into Table Started ...")

            # insert csv files in the table
            self.dbOperation.insertIntoTableGoodData('Training')
            self.dbOperation.insertIntoTableGoodData('Training')
            self.logger.log(self.log_file, "Insertion in Table completed!!!")
            self.logger.log(self.log_file, "Deleting Good Data Folder!!!")

            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.logger.log(self.log_file, "Good_Data folder deleted!!!")
            self.logger.log(self.log_file, "Moving bad files to Archive and deleting Bad_Data folder!!!")

            # Move the bad files to archive folder
            self.raw_data.moveBadFilestoArchivesBad()
            self.logger.log(self.log_file, "Bad files moved to archive!! Bad folder Deleted!!")
            self.logger.log(self.log_file, "Validation Operation completed!!")
            self.logger.log(self.log_file, "Extracting csv file from table")
            # export data in table to csvfile
            self.dbOperation.selectingDatafromtableintocsv('Training')
            self.log_file.close()

        except Exception as e:
            raise e
