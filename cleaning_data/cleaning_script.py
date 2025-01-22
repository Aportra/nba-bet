import pandas as pd
import pandas_gbq
from google.cloud import bigquery
import regex as re


def clean_past_data():



    tables = ['NBA_Season_2021-2022_uncleaned']

# ,'NBA_Season_2022-2023_uncleaned','NBA_Season_2023-2024_uncleaned','NBA_Season_2024-2025_uncleaned']

    for table in tables:
        query = f"""select *
                    from `capstone_data.{table}`
                    order by PTS desc
                    limit 10"""
        data = pd.DataFrame(pandas_gbq.read_gbq(query, project_id = 'miscellaneous-projects-444203'))

        data.dropna(inplace = True, ignore_index = True)

#\b([A-Z][a-z]+(?:-[A-Z][a-z]+)?(?:\s[A-Z][a-z]+(?:-[A-Z][a-z]+)*)?)(?:\s(?:Jr\.|Sr\.|III|IV))?(?=\s|$|[^a-z]|[A-Z][^a-z])
        
        print(data)


clean_past_data()