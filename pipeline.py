import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
import os


def extract_data():
    source_url = "https://shelterdata.s3.amazonaws.com/shelter1000.csv"
    df = pd.read_csv(source_url)
    return df

def transform_data(data):
    transformation_data = data.copy()
    transformation_data[['Month', 'Year']] = transformation_data['MonthYear'].str.split(' ', expand=True)
    transformation_data[['Name']] = transformation_data[['Name']].fillna('Name_less')
    transformation_data[['Outcome Subtype']] = transformation_data[['Outcome Subtype']].fillna('Not_Available')
    transformation_data.drop(['MonthYear', 'Age upon Outcome'], axis=1, inplace=True)
    
    mapping = {
        'Animal ID': 'animal_id',
        'Name': 'animal_name',
        'DateTime': 'timestmp',
        'Date of Birth': 'dob',
        'Outcome Type': 'outcome_type',
        'Outcome Subtype': 'outcome_subtype',
        'Animal Type': 'animal_type',
        'Breed': 'breed',
        'Color': 'color',
        'Month': 'monthh',
        'Year': 'yearr',
        'Sex upon Outcome': 'sex'
    }
    transformation_data.rename(columns=mapping, inplace=True)
    transformation_data[['reprod', 'gender']] = transformation_data.sex.str.split(' ', expand=True)
    transformation_data.drop(columns=['sex'], inplace=True)
    return transformation_data

def load_data(transformed_data):
    db_addr = "postgresql+psycopg2://santosh:Adabala%401505@db:5432/shelter"
    conn = create_engine(db_addr)
    
    time_df = transformed_data[['monthh', 'yearr']].drop_duplicates()
    time_df.to_sql("timing_dim", conn, if_exists="append", index=False)
    
    transformed_data[['animal_id', 'animal_type', 'animal_name', 'dob', 'breed', 'color', 'reprod', 'gender', 'timestmp']].to_sql("animal_dim_info", conn, if_exists="append", index=False)
    
    outcome_df = transformed_data[['outcome_type', 'outcome_subtype']].drop_duplicates()
    outcome_df.to_sql("outcome_dim", conn, if_exists="append", index=False)
    
    transformed_data.to_sql("temp_table", conn, if_exists="append", index=False)
    
    sql_statement = text("""
    INSERT INTO outcome_fact (outcome_dim_key, animal_dim_key, time_dim_key)
    SELECT od.outcome_dim_key, adi.animal_dim_key, td.time_dim_key
    FROM temp_table tt
    JOIN outcome_dim od ON tt.outcome_type = od.outcome_type AND tt.outcome_subtype = od.outcome_subtype
    JOIN timing_dim td ON tt.monthh = td.monthh AND tt.yearr = td.yearr
    JOIN animal_dim_info adi ON adi.animal_id = tt.animal_id AND adi.animal_type = tt.animal_type AND adi.timestmp = tt.timestmp;
    """)
    
    with conn.begin() as connect:
        connect.execute(sql_statement)

if __name__ == "__main__":
    print("Start processing....")
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
    print("Finished processing.....")
