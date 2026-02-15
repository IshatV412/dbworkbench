import psycopg2
import boto3
import yaml
import os

# Get paths relative to this script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))
config_path = os.path.join(project_root, "config.yaml")
cert_path = os.path.join(project_root, "certs", "global-bundle.pem")

def load_config():
    """Load database configuration from config.yaml"""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

config = load_config()

password = config["AWS_CREDS"]["RDS"]["PASSWORD"]

conn = None
try:
    conn = psycopg2.connect(
        host=config["AWS_CREDS"]["RDS"]["HOST"],
        port=config["AWS_CREDS"]["RDS"]["PORT"],
        database=config["AWS_CREDS"]["RDS"]["DATABASE"],
        user=config["AWS_CREDS"]["RDS"]["USER"],
        password=password,
        sslmode='verify-full',      
        sslrootcert=cert_path
    )
    cur = conn.cursor()
    cur.execute('SELECT version();')
    print(cur.fetchone()[0])
    cur.close()
except Exception as e:
    print(f"Database error: {e}")
    raise
finally:
    if conn:
        conn.close()