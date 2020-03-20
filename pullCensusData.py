import pandas as pd
import numpy as np
import requests
import sys
from datetime import date


today = date.today()

## Load state and county data
states = pd.read_csv("Data/state_codes.txt",sep=" 	", engine='python')
counties = pd.read_csv("Data/county_codes.txt",sep=" 	", engine='python')

## Load key
key = "124e1b1d5ceca3b46b3f6bd97d95ea8d4d780e28"

## Pull state data
url = f"https://api.census.gov/data/2020/dec/responserate?get=GEO_ID,CAVG&for=state:*&key={key}"
response = requests.get(url)
if response:
    print('State: Success!')
else:
    print('State: An error has occurred.')
    sys.exit("State: "+response.text)

state_df = pd.DataFrame.from_records(response.json()[1:])

state_df.columns = ["GEO_ID","CAVG","state"]
state_df['state'] = state_df.state.astype(int)

state_df['state_name'] = state_df.state.map(states.set_index('code')['state'])

state_df['state_short'] = state_df.state.map(states.set_index('code')['state_code'])

columns = list(state_df.columns)
state_df["date"] = str(today)

state_df = state_df[["date"]+columns]
state_df.sort_values(by="state",inplace=True)
state_df = state_df.reset_index(drop=True)

state_df.to_csv("Data/sates_response_rates.csv",mode = 'a', header = False, index = False)

## Pull counties data
data=[]
url = f"https://api.census.gov/data/2020/dec/responserate?get=GEO_ID,CAVG&for=county:*&key={key}"
response = requests.get(url)
if response:
    print('County: Success!')
    data = response.json()
else:
    print('An error has occurred.')
    sys.exit("County: "+response.text)

county_df = pd.DataFrame.from_records(data[1:])
county_df.columns = data[0]
county_df['state_county'] = county_df.state + county_df.county
county_df['state_county'] = county_df.state_county.astype(int)
county_df['state'] = county_df.state.astype(int)

county_df['state_name'] = county_df.state.map(states.set_index('code')['state'])
county_df['state_short'] = county_df.state.map(states.set_index('code')['state_code'])

county_df['county_name'] = county_df.state_county.map(counties.set_index('code')['county'])

columns = list(county_df.columns)
county_df["date"] = str(today)

county_df = county_df[["date"]+columns]
county_df.sort_values(by=["state_county"],inplace=True)
county_df = county_df.reset_index(drop=True)

county_df.to_csv("Data/counties_response_rates.csv",mode = 'a', header = False, index = False)

## Pull tract data
data=[]
c=0
for i in range(len(state_df)):
    s = state_df.iloc[i,:]
    if s.state <10:
        num = f"0{s.state}"
    else:
        num = s.state
    url = f"https://api.census.gov/data/2020/dec/responserate?get=GEO_ID,CAVG&for=tract:*&in=state:{num}&key={key}"
    response = requests.get(url)
    if  not response:
        print('Tract: An error has occurred.')
        print(response.text)
        sys.exit("Tract: "+response.text)
    else:
        start = 0 if c==0 else 1
        data = data + response.json()[start:]
        c=c+1
        
print("Tract: Done!")

tract_df = pd.DataFrame.from_records(data[1:])
tract_df.columns = data[0]
tract_df['state_county'] = tract_df.state + tract_df.county
tract_df['state_county'] = tract_df.state_county.astype(int)
tract_df['state'] = tract_df.state.astype(int)

tract_df['state_name'] = tract_df.state.map(states.set_index('code')['state'])
tract_df['state_short'] = tract_df.state.map(states.set_index('code')['state_code'])

tract_df['county_name'] = tract_df.state_county.map(counties.set_index('code')['county'])

columns = list(tract_df.columns)
tract_df["date"] = str(today)

tract_df = tract_df[["date"]+columns]
tract_df.sort_values(by=["state_county","tract"],inplace=True)
tract_df = tract_df.reset_index(drop=True)

tract_df.to_csv("Data/tracts_response_rates.csv",mode = 'a', header = False,  index = False)