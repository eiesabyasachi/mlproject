import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd
from sklearn.model_selection import train_test_split
from dataclasses import dataclass

## Here the @dataclass will be act as a decorator, it will be mainly used for processing 
## large volume of data. 
## storing the train/test and raw.csv files in the artifacts folder. 
@dataclass
class DataIngestionConfig:
    train_data_path: str=os.path.join('artifacts','train.csv')
    test_data_path: str=os.path.join('artifacts','test.csv')
    raw_data_path: str=os.path.join('artifacts','data.csv')


## Here the values for each train / test and raw data are stored in the 
## class DataIngestion with the defination as ingestion_config.

## The method initiate_data_ingestion is called to take the dataset from the 
## train / test / raw csv 
## First, it will create the artifact dir with the makedirs command.
## 2nd, it will create a raw.csv file in the "artifact" dir
## 3rd, it will perform the train_test split from the raw data. 
## 4th, once train/ test splitting is done, it will create a tran.csv and test.csv 
## in the artifact dir again. 
## once all the steps are done, the path of the respective train test csv will be returned. 

class DataIngestion:
    def __init__(self):
        self.ingestion_config=DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("Enter the data ingestion method or component")
        try:
            df=pd.read_csv(r'notebook\data\stud.csv')
            logging.info('Read the dataset from dataframe')
            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path),exist_ok=True)

            df.to_csv(self.ingestion_config.raw_data_path,index=False,header=True)

            logging.info('Train test split initiated')
            train_set,test_set=train_test_split(df,test_size=0.2,random_state=42)

            train_set.to_csv(self.ingestion_config.train_data_path,index=False,header=True)
            test_set.to_csv(self.ingestion_config.test_data_path,index=False,header=True)
            logging.info('Ingestion of the data is completed')

            return(
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )
        except Exception as e:
            raise CustomException(e,sys)

if __name__=="__main__":
    obj=DataIngestion()
    obj.initiate_data_ingestion()
            

        
