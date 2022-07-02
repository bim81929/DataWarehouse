import sys
import os

sys.path.append(os.getcwd())

from config import config
from common.sql import sql
from datetime import datetime
from pprint import pprint
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

list_column = {
    "list":["id", "domain", "url", "category", "title", "summary", "created_date"],
    "article":["id", "raw_id", "domain", "url", "category", "title", "author", "summary", "description", "date_submitted", "created_date"]
}

def matching_data(primary_domain, secondary_domain):
    primary_data = pd.DataFrame(sql.sql_read_table(sql.get_connect(), "article" , ["*"], f"domain='{primary_domain}'"), columns=list_column["article"])
    primary_data = primary_data.reset_index()
    secondary_data = pd.DataFrame(sql.sql_read_table(sql.get_connect(), "article" , ["*"],  f"domain='{secondary_domain}'"), columns=list_column["article"])
    secondary_title = secondary_data["title"].to_list()

    for index, row in primary_data.iterrows():
        list_matches = process.extract(row["title"], secondary_title)
        for item in list_matches:
            if item[1] >= 90:
                secondary_data.drop(secondary_data.loc[secondary_data['title']==item[0]].index, inplace=True)
                print(secondary_data.shape[0])
            else:
                break
    result = pd.concat([primary_data, secondary_data], ignore_index=True, sort=False)
    return result
        
matching_data("dantri.com.vn", "vietnamnet.vn")



    
    
