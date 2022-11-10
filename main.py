import requests, zipfile, io
import pandas as pd
from bs4 import BeautifulSoup
import re
import sqlite3

#set target url and send GET request
url = "https://www.data.gouv.fr/fr/datasets/base-de-donnees-nationale-des-batiments/"
response = requests.get(url)

#instantiate BeautifulSoup onject
soup = BeautifulSoup(response.content, features="html.parser")

#isolate the script tag containing download links
script = soup.find("script")

#iterate through script contents and build a string capable of string methods
generated_string = ""
for x in list(script.string):
    generated_string = generated_string + x

#find all links
links = re.findall(r"https://[\w\d\./-]*", generated_string)

#isolate links for regions 93 ad 75
links_of_interest = [x for x in links if re.search(r"-93\.|-75\.", x)]

#download data from links
for i in links_of_interest:
    r = requests.get(i, stream = True)
    if r.ok:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print(f"Downloading {i.split('/')[-1].split('.')[0]}...")
        z.extractall(i.split("/")[-1].split(".")[0])
        print(f"Downloaded!")
    else:
        print("Bad response from server")

#connect to both databases
print("Connecting to databases")
conn = sqlite3.connect("bnb-export-93/bnb_export_93.gpkg")
conn2 = sqlite3.connect("bnb-export-75/bnb_export_75.gpkg")

#create dataframes of both databases
print("Creating dataframes")
df = pd.read_sql_query("SELECT * FROM batiment", conn)
df2 = pd.read_sql_query("SELECT * FROM batiment", conn2)

#combine dataframes
print("Merging dataframes")
df = pd.concat([df, df2], ignore_index=True, sort=False)

#this dataframe would take a signifcant amount of time to clean completely 
#so here's an example of what I'd do for machine learning purposes

#clean df example
df_eg = df[["adedpe202006_logtype_ch_gen_lib"]]

#replace null values with 0
df_eg = df_eg.fillna(0)

#define set to collect categories
categories = set()

#loop through column df and collect unique categories
print("Extracting categories from an example column")
for i in df_eg["adedpe202006_logtype_ch_gen_lib"]:
    if i != 0:
        i = re.split(r",|:", i)
        i.pop(0)
        for j in i:
            categories.add(j.replace(")", ""))

#make column from each category
for i in categories:
    df_eg[i] = 0
    
#define function to check values from master column and populate new columns with 1 if present
def check_row(row, i):
    values = row["adedpe202006_logtype_ch_gen_lib"]
    if values != 0:
        if re.search(i, values):
            return 1
        else:
            return 0
    else:
        return 0
    
#apply function
for i in categories:
    df_eg[i] = df_eg.apply(lambda row: check_row(row, i), axis = 1)

#drop master column
df_eg = df_eg.drop("adedpe202006_logtype_ch_gen_lib", axis = 1)

#export all data
print("Exporting all data")
df.to_csv("all_data.csv", index_label='index')
print("Done!")

#export example
print("Exporting ML example data")
df_eg.to_csv("ML_example.csv")
print("Done!")