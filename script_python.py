import requests
import pandas as pd  
import sqlalchemy
import pyodbc


# pull data from API
url = 'https://api.coingecko.com/api/v3/coins/markets'
params = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 100,
    'page': 1,
    'sparkline': False
}
headers = {
    "Content-Type": "application/json",
    "Accept-Encoding": "deflate"
}

response = requests.get(url, headers=headers, params=params)
print(response)  # Should be <Response [200]>

responseData = response.json()
print(responseData[:2])  # print first two objects to avoid overload

df = pd.json_normalize(responseData)
print(df.head())

# test database connection
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=APIDATA;'
    'Trusted_Connection=yes;'
    'TrustServerCertificate=yes;'
)
cursor = conn.cursor()
cursor.execute("SELECT 1")
print(cursor.fetchone())

# Pull data from API and write to SQL Server
if response.status_code == 200:
    try:
        engine = sqlalchemy.create_engine(
            "mssql+pyodbc://localhost/APIDATA?"
            "driver=ODBC+Driver+17+for+SQL+Server&"
            "Trusted_Connection=yes&"
            "TrustServerCertificate=yes"
        )
        df.to_sql('coins', con=engine, if_exists='replace', index=False)
        print("Data was successfully written to the database!")
    except Exception as e:
        print(f"Failed to write to the database: {e}")
else:
    print(f"Failed to retrieve data from API: {response.status_code}")