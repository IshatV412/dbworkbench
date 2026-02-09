import psycopg2
import yaml
import json

with open("config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

# connect to the db. Values can be changed in the config.yaml
conn = psycopg2.connect(
    host = CONFIG["POSTGRES_CREDS"]["HOST"],
    database = CONFIG["POSTGRES_CREDS"]["DATABASE"],
    user = CONFIG["POSTGRES_CREDS"]["USER"],
    password = CONFIG["POSTGRES_CREDS"]["PASSWORD"],
    port = CONFIG["POSTGRES_CREDS"]["PORT"]
)

cursor = conn.cursor()

cursor.execute("select * from hello")

results = cursor.fetchall()

for result in results:
    print(result)

conn.close()