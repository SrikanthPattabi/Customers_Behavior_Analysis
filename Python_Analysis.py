import pandas as pd

# Loading the CSV file
df = pd.read_csv(r'C:\Users\srika\OneDrive\Documents\Data Analist\PowerBI\Complet Project\customer_shopping_behavior.csv')
print(df.head())


print(df.info())
print(df.describe())

# Checking the Null Values
print(df.isnull().sum())

# Replacing the Null values into Median()
df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(lambda X: X.fillna(X.median()))
print(df.isnull().sum())

# Converting the lower Case
df.columns = df.columns.str.lower()

# Replaceing the values
df.columns = df.columns.str.replace(' ','_')

# Rename the Columns names
df = df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})

# Printing the column Names
print(df.columns)

# Creating the New Column with the name is Age_Groups
labels = ['Young Adult','Adult','Middle-aged','Senior']
df['age_group'] = pd.qcut(df['age'],q=4, labels = labels)

print(df[['age','age_group']].head(10))

# Create Column purchase_frequency_days
frequency_mapping = {
    'Fortnightly':14,
    'Weekly':7,
    'Monthly':30,
    'Quarterly':90,
    'Bi-weekly':14,
    'Annually':365,
    'Every 3 Months':90
}

df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)

print(df[['purchase_frequency_days','frequency_of_purchases']].head(10))


print(df[['discount_applied','promo_code_used']].head(10))

# Checking Two Columns values is Same or Not
print((df['discount_applied'] == df['promo_code_used']).all())

# Drop or Remove the Columns
df = df.drop('promo_code_used', axis = 1)

print(df.columns)

# Install required libraries

from sqlalchemy import create_engine
from urllib.parse import quote_plus

# sql server Connection
username = "DESKTOP-GPKHQ7K\srika"
password = ""
host ="localhost\\SQLEXPRESS"
port = ""
database = "customer_behavior"


# Note : Requires Microsoft ODBC Drivers installed separately on your machine
driver = quote_plus("ODBC Driver 17 for SQl Server")
from sqlalchemy import create_engine

engine = create_engine("mssql+pyodbc:// localhost\\SQLEXPRESS/master?driver=ODBC+Driver+17+for+SQL+Server")
conn = engine.connect()

# Write Data Frame to mysql
table_name = "customers"
df.to_sql(table_name,engine,if_exists="replace",index=False)

# Read Back Sample
print(pd.read_sql("SELECT TOP 5 * FROM customers;", engine))

from sqlalchemy import create_engine
from urllib.parse import quote_plus

server = "localhost\\SQLEXPRESS"
database = "customer_behavior"

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)

engine = create_engine(
    "mssql+pyodbc:///?odbc_connect=" + quote_plus(connection_string)
)

print("Connected successfully")

df.to_sql("customers", engine, if_exists="replace", index=False)