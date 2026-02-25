import psycopg2
import boto3
import yaml
from pathlib import Path

# Load configuration
config_path = Path(__file__).parent.parent.parent / 'config.yaml'
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

rds_config = config['AWS_CREDS']['RDS']
ssl_cert_path = Path(__file__).parent.parent.parent / rds_config['SSL_CERT']

conn = None
try:
    conn = psycopg2.connect(
        host=rds_config['HOST'],
        port=int(rds_config['PORT']),
        database=rds_config['DATABASE'],
        user=rds_config['USER'],
        password=rds_config['PASSWORD'],
        sslmode=rds_config['SSL_MODE'],
        sslrootcert=str(ssl_cert_path)
    )
    cur = conn.cursor()
    cur.execute(f"CREATE USER sweproj WITH PASSWORD '{rds_config['PASSWORD']}';")
    cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {rds_config['DATABASE']} TO sweproj;")
    conn.commit()
    cur.close()
    conn.close()
except Exception as e:
    print(f"Database error: {e}")
    raise
finally:
    if conn:
        conn.close()