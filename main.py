import json
import time
from pathlib import Path

import requests
import argparse
from typing import List


""" 
This tool allows the the user to download all the data from a given summary level. These files will be download as csv files for each state and
table your provide. This data is being buld downloaded from http://census.ire.org/data/bulkdata.html
"""

def download_table(fip: int, table_name: str, file_path: str, url_pattern: str, folder_path: str, summary_level: str):
    Path(folder_path.format(table_name=table_name, summary_level=summary_level)).mkdir(parents=True, exist_ok=True)
    fip_string = str(fip).zfill(2)
    current_requests = requests.get(url_pattern.format(fips=fip_string, summary_level=summary_level, table_name=table_name))
    with open(file_path.format(table_name=table_name, summary_level=summary_level, fips=fip_string), "wb") as f:
        f.write(current_requests.content)
        
def read_config(config_file_location: str):
    with open(config_file_location) as config_file:
        return json.load(config_file)

def main(summary_levels: List[str], table_names: List[str], fips_list: List[str]):
    url_pattern = "http://censusdata.ire.org/{fips}/all_{summary_level}_in_{fips}.{table_name}.csv"

    folder_path = "data/table_{table_name}/sumLevel_{summary_level}"
    file_path = folder_path + "/all_{summary_level}_in_{fips}.csv"
    
    for table_name in table_names:
        for summary_level in summary_levels:
            for fip in fips_list:
                download_table(fip=fip, table_name=table_name, file_path=file_path, url_pattern=url_pattern, 
                               folder_path=folder_path, summary_level=summary_level)
                
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tool to mass donwlaod data from http://census.ire.org/data/bulkdata.html")
    parser.add_argument("--config_file", type=str, required=True)
    args = parser.parse_args()
    
    config = read_config(config_file_location=args.config_file)
    print(config)
    
    main(summary_levels=config["summary_levels"], table_names=config["table_names"], fips_list=config['fips_list'])
