import os
import sys

import pandas as pd
import dotenv
import streamlit as st

dotenv.load_dotenv(dotenv.find_dotenv())

sys.path.insert(0, os.getenv("DOTASCIENCE"))

def create_team_label(row, team="radiant_team"):
    if team=="radiant_team":
        return row[team] + " (" + str(row["proba_radiant"] * 100) + "%)"
    else:
        return row[team] + " (" + str(1 - row["proba_radiant"] * 100) + "%)"


@st.cache
def load_data():
    df = pd.read_csv( "data/df_predict.csv")
    df = df.dropna(subset=["proba_radiant"], how="any")
    df = df.fillna("-")
    df = df.sort_values(by="match_id", ascending=False).reset_index(drop=True)

    df = df.rename(columns={"match_id": "Match ID"})

    df["Radiant Team"] = df["radiant_team"] + " (" + (100 * df["proba_radiant"]).round(2).astype(str) + "%)"
    df["Dire Team"] = df["dire_team"] + " (" + (100 - 100 * df["proba_radiant"]).round(2).astype(str) + "%)"
    
    columns = ["Match ID", "Radiant Team", "Dire Team", "update_at"]
    
    return df[columns]

st.write("""
# Predições de partidas de Dota2
""")

data = load_data()

st.dataframe(data[data.columns[:-1]])

update_at = data['update_at'][0]

st.write(f"Última atualização: {update_at}")