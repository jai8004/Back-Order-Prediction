from os import listdir
import pandas as pd
from applicationLogger.logger import AppLogger


class DataTransform:
    """
    This Class is responsible for transforming the good data so that it can be loaded in to database.

    """

    def __init__(self):

        self.good_data_path = "Training_Raw_files_validated/Good_Raw"

        self.logger = AppLogger()

    def replaceMissingWithNull(self):
        """
        This method replace all the null values with 'NULL' so that it can be saved to database
        :return:
        """

        log_file = open("Training_Logs/dataTransformLog.txt", 'a+')

        try:
            onlyfiles = [f for f in listdir(self.good_data_path)]
            for file in onlyfiles:
                csv = pd.read_csv(self.good_data_path + "/" + file)
                csv.fillna("'NULL'", inplace=True)
                csv.to_csv(self.good_data_path + "/" + file, index=None, header=True)
                self.logger.log(log_file, " %s: File Transformed successfully!!" % file)
        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
            log_file.close()
        log_file.close()

    def addQuotesToStringValuesInColumn(self):
        """
        This function adds quotes to all the string values so that it can be pushed to the database.
        :return:
        """
        log_file = open("Training_Logs/addQuotesToStringValuesInColumn.txt", 'a+')
        try:
            onlyfiles = [f for f in listdir(self.good_data_path)]
            for file in onlyfiles:
                data = pd.read_csv(self.good_data_path + "/" + file)
                str_column = ["potential_issue", "deck_risk", "oe_constraint", "ppap_risk", "stop_auto_buy", "rev_stop",
                              "went_on_backorder"]

                for col in data.columns:
                    if col in str_column:  # add quotes in string value
                        data[col] = data[col].apply(lambda x: "'" + str(x) + "'")

                data.to_csv(self.good_data_path + "/" + file, index=None, header=True)
                self.logger.log(log_file, " %s: Quotes added successfully!!" % file)

        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)

            log_file.close()
        log_file.close()
