import sqlite3 as sql
import json
import numpy as np
import plotly.express as px
import pandas as pd
import datetime as dt
import dash
from dash import dcc
from dash import html

#Initiallize the Dash app
app = dash.Dash(serve_locally = False)


conn = sql.connect('RAW.sqlite3')
cursor = conn.cursor()

# Here either you add argparser for token list or you implement callbacks with the dash app in order to query
# the database and have new results, neither are implemented here

protocols = ['Curve','MakerDAO','AAVE','WBTC','Compound','Lido','SushiSwap','Uniswap']
TVL = {}

for i in protocols:
    # Load the TVL raw json data
    json_ = json.loads(cursor.execute(f"SELECT json(data) FROM llama_tvl WHERE id='{i}'").fetchall()[0][0])

    TVL[f'{i}'] = {}

    chains_tvl = {}

    for key in json_['chainTvls']:
        chains_tvl[key] =  pd.DataFrame(json_['chainTvls'][key]['tvl'])
    # Sum TVL data over chains
    TVL[f'{i}'] = pd.concat(chains_tvl.values(),ignore_index=True)
    d_min,d_max = TVL[f'{i}']['date'].min(),TVL[f'{i}']['date'].max()
    TVL[f'{i}']['date'] = pd.to_datetime(TVL[f'{i}']['date'],unit='s')
    TVL[f'{i}'] = TVL[f'{i}'].groupby(pd.Grouper(key='date',freq='D')).sum().reset_index()

    # Load price and mcap raw data from glassnode API, and insert it in the main DF
    price = json.loads(cursor.execute(f"SELECT json(price) FROM glassnode WHERE id='{i}'").fetchall()[0][0])
    price = pd.DataFrame(price)
    mcap = json.loads(cursor.execute(f"SELECT json(mcap) FROM glassnode WHERE id='{i}'").fetchall()[0][0])
    mcap = pd.DataFrame(mcap)
    mcap['t'] = pd.to_datetime(mcap['t'])
    price['t'] = pd.to_datetime(price['t'])
    TVL[f'{i}']['mcap'] = mcap['v']
    TVL[f'{i}']['price'] = price['v']

    # Clean the data, remove NA, fill the data by interpolating
    TVL[f'{i}'] = TVL[f'{i}'].fillna(method='ffill')
    TVL[f'{i}']['date'] = (TVL[f'{i}']['date'] - dt.datetime(1970,1,1)).dt.total_seconds()
    TVL[f'{i}']['ratio'] = TVL[f'{i}']['totalLiquidityUSD']/TVL[f'{i}']['mcap']
    TVL[f'{i}'] = TVL[f'{i}'].round(3)
    TVL[f'{i}'].replace(0, np.nan, inplace=True)
    TVL[f'{i}'] = TVL[f'{i}'].interpolate(method='linear')

    # Add rolling correlation
    TVL[f'{i}']['change'] = TVL[f'{i}']['ratio'].pct_change(60)
    TVL[f'{i}']['date'] = pd.to_datetime(TVL[f'{i}']['date'],unit='s').dt.date
    TVL[f'{i}']['corr'] = TVL[f'{i}']['totalLiquidityUSD'].rolling(100).corr(TVL[f'{i}']['price'] )

# Add ranking over time
df = pd.concat(TVL).reset_index()
df['rank'] = df.groupby(['date'])['ratio'].rank("dense",ascending=False)

# Plot everything
fig1 = px.area(df,x='date',y='ratio',color='level_0',labels={'level_0':'Protocols'})
fig2 = px.line(df,x='date',y='corr',color='level_0',labels={'level_0':'Protocols'})
fig3 = px.line(df,x='date',y='rank',color='level_0',labels={'level_0':'Protocols'})
fig3.update_yaxes(autorange="reversed")

# This line wasn't asked but it gives information about the pct change of the ratio
fig4 = px.line(df,x='date',y='change',color='level_0',labels={'level_0':'Protocols'})

# Finally create the dash app to display all the data, no data range is needed because Plotly let the user zoom over
# the desired data ranges

app.layout = html.Div(children=[
    html.Div([
        html.H1(children='TVL to Mcap ratio'),
        dcc.Graph(
            id='graph1',
            figure=fig1
        ),
    ]),
    html.Div([
        html.H1(children='100 days corr'),
        dcc.Graph(
            id='graph2',
            figure=fig2
        ),
    ]),

    html.Div([
        html.H1(children='Rank over time'),
        dcc.Graph(
            id='graph3',
            figure=fig3
        ),
    ]),

    html.Div([
        html.H1(children='Ratio change over time'),
        dcc.Graph(
            id='graph4',
            figure=fig4
        ),
    ])
])

if __name__ == '__main__':

    app.run_server(debug=False)