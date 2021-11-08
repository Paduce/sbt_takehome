import sqlite3 as sql
from secret import glassnode
import requests
import pandas as pd
import json

llama = requests.get(f'https://api.llama.fi/protocols').json()

conn = sql.connect('RAW.sqlite3')
cursor = conn.cursor()
# Create 3 tables, for storing all the incoming data, llama_tokens stores name of the tokens availables, llama_tvl
# stores the json raw data of Llama API, and glassnode stores both the mcap and price data of everytoken
conn.executescript(
    """
    DROP TABLE IF EXISTS llama_tokens;
    DROP TABLE IF EXISTS glassnode;
    DROP TABLE IF EXISTS llama_tvl;
    CREATE TABLE llama_tokens(
    id varchar(3)
    );
    CREATE TABLE llama_tvl(
    id varchar(3),
    data json
    );
    CREATE TABLE glassnode(
    id varchar(3),
    price json,
    mcap json
    );
    """
)

for token in llama:
    cursor.execute(
        """
        INSERT INTO llama_tokens values (?)
        """,
        [token['name']]

    )
    conn.commit()

query = cursor.execute("SELECT id FROM llama_tokens")
tokens = [i[0] for i in query]

TVL = {}

for t in tokens:
    try:
        cursor.execute(
            """
            INSERT INTO llama_tvl values (?,?)
            """,
            [t,json.dumps(requests.get(f'https://api.llama.fi/protocol/{t}').json())]
        )
        conn.commit()

        json_ = json.loads(cursor.execute(f"SELECT json(data) FROM llama_tvl WHERE id='{t}'").fetchall()[0][0])

        TVL[f'{t}'] = {}
        chains_tvl = {}

        for key in json_['chainTvls']:
            chains_tvl[key] = pd.DataFrame(json_['chainTvls'][key]['tvl'])

        TVL[f'{t}'] = pd.concat(chains_tvl.values(), ignore_index=True)
        d_min, d_max = TVL[f'{t}']['date'].min(), TVL[f'{t}']['date'].max()

        mcap = requests.get(f'https://api.glassnode.com/v1/metrics/market/marketcap_usd',
                             params={'a': json_['symbol'],
                                     's': d_min,
                                     'u': d_max,
                                     'i': '24h',
                                     'f': 'JSON',
                                     'timestamp_format': 'humanized',
                                     'api_key': glassnode
                                     }).json()

        price = requests.get(f'https://api.glassnode.com/v1/metrics/market/price_usd_close',
                             params={'a': json_['symbol'],
                                     's': d_min,
                                     'u': d_max,
                                     'i': '24h',
                                     'f': 'JSON',
                                     'timestamp_format': 'humanized',
                                     'api_key': glassnode
                                     }).json()

        cursor.execute(
            """
            INSERT INTO glassnode values (?,?,?)
            """,
            [t, json.dumps(price),json.dumps(mcap)]
        )
        conn.commit()

    except:
        continue

