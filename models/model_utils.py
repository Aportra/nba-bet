import yaml
import os
import psycopg2
import requests
import pandas as pd
from io import StringIO
# import chromedriver_autoinstaller


def establish_requests(url, params=False):
    # Headers to mimic a real browser request (prevents bot blocking)

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0",
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nba.com/stats/",
        "Origin": "https://www.nba.com"
    }
    if not params:
    # Send request
        response = requests.get(url, headers=headers)
    else:
       response = requests.get(url, headers=headers,params=params) 

    print(response.status_code)
    return response


class psql:
    def __init__(self):
        os.chdir('..')
        base_dir = os.path.dirname(os.path.abspath(__file__))
        print(base_dir)
        config = os.path.join(base_dir, 'config.yaml')
        print(config)
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)

        try:
            print("database connection successful")
            self.connect = (psycopg2.connect(database=config['database'],
                                             user=config['user'],
                                             password=config['password'],
                                             host=config['host']))
        except psycopg2.operationalerror:
            print("database connection failed")
            return None

    def create_table(self, table, table_name):
        cur = self.connect.cursor()
        if cur is None:
            return

        dtype_converter = {
            "int64": "bigint",
            "int32": "integer",
            "float64": "double precision",
            "float32": "real",
            "bool": "boolean",
            "boolean": "boolean",
            "object": "text",
            "string": "text",
            "datetime64[ns]": "timestamp",
            "datetime64[ns, utc]": "timestamptz",
        }
        d = ([(col, dtype_converter[str(table[col].dtype)])
             for col in list(table.columns)])

        cols = ',\n'.join([f'\t{col} {typ}' for col, typ in d])

        query = f"""
        create table {table_name}(
        {cols}
        );
        """
        cur.execute(query)
        self.connect.commit()

        cur.close()

    def upload_data(self, table, table_name):
        cur = self.connect.cursor()
        if cur is None:
            return

        buffer = StringIO()

        table.to_csv(buffer, index=False, header=False)

        buffer.seek(0)

        cols = ',\n'.join([f'\t{col}' for col in table.columns])

        cur.copy_expert(
                f"""copy {table_name}
                ({cols})
                from stdin with (format csv)""", buffer)

        self.connect.commit()

    def query(self, query):
        cur = self.connect.cursor()

        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        data = pd.DataFrame(cur.fetchall(), columns=columns)

        return data

    def close(self):

        self.connect.close()

#Makes it so we are not connecting to driver on import
