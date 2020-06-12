import json
import time
from pathlib import Path

import requests
import argparse
from typing import List
from abc import ABC, abstractmethod
from bounded_pool_executor import BoundedProcessPoolExecutor

""" 
This tool allows the the user to download all the data fro a given summary level. These files will be download as csv files for each state and
table your provide. This data is being buld downloaded from http://census.ire.org/data/bulkdata.html
"""


class AbstractDownloader(ABC):
    def __init__(self):
        super().__init__()
        
    @abstractmethod
    def download(self):
        pass

class SF1_Downloader(AbstractDownloader):
    def __init__(self, data_folder: str,  summary_levels: List[str], table_names: List[str], fips_list: List[str], process_num: int):
        super().__init__()
        self.summary_levels: List[str] = summary_levels
        self.table_names: List[str] = table_names
        self.fips_list: List[str] = [str(fip).zfill(2) for fip in fips_list]
        self.process_num = process_num
        
        self.url_pattern = "http://censusdata.ire.org/{fips}/all_{summary_level}_in_{fips}.{table_name}.csv"
        self.folder_path = data_folder + "/table_{table_name}/sumLevel_{summary_level}"
        self.file_path = self.folder_path + "/all_{summary_level}_in_{fips}.csv"
        
    def download(self):
        with BoundedProcessPoolExecutor(max_workers=self.process_num) as worker:
            for table_name in self.table_names:
                for summary_level in self.summary_levels:
                    for fip in self.fips_list:
                        worker.submit(self.download_table, fip, table_name, summary_level)

    def download_table(self, fip: int, table_name: str, summary_level: str):
        Path(self.folder_path.format(table_name=table_name, summary_level=summary_level)).mkdir(parents=True, exist_ok=True)
        current_requests = requests.get(self.url_pattern.format(fips=fip, summary_level=summary_level, table_name=table_name))
        with open(self.file_path.format(table_name=table_name, summary_level=summary_level, fips=fip), "wb") as f:
            f.write(current_requests.content)


def read_config(config_file_location: str):
    with open(config_file_location) as config_file:
        return json.load(config_file)
        
if __name__ == "__main__":
    class_mapper = {
        "SF1_Downloader": SF1_Downloader
    }
    parser = argparse.ArgumentParser(description="Tool to mass donwlaod data from http://census.ire.org/data/bulkdata.html")
    parser.add_argument("--config_file", type=str, required=True)
    args = parser.parse_args()
    
    config = read_config(config_file_location=args.config_file)
    
    for download in config['download_list']:
        download_instance = class_mapper[download['download_class']](**download['download_class_variables'])
        download_instance.download()
