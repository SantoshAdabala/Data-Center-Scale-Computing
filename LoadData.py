import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine


class GCPDataLoader:

    def __init__(self):
        self.bucket_name = 'datacenterscale'

    def getcredentials(self):
        credentials = {
                        "type": "service_account",
                        "project_id": "databricks-403018",
                        "private_key_id": "bc783244153b87113544a502c11ea64d85cfb364",
                        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCnQVTNWTWAu+cD\nNbqWCSoQQp0sWGHvvM1n3kkRUiUOeDHVaHv/Wv5eZLXyrEby0uCGc8H/oEaHYEGz\nrm6NPfN3E7l6hCz8MOBOyJRqnYyut3xJ8+BRwsYAwENq2uGvMZlAtAY37r5qQLQN\ntze/e4LfumSLIkSbBHJlZkFwb6Ql0Xp5VlEXCwXHSsJCU5otCD/6vp2w24CacF+2\n9BAsaoWaHXllNl1ptgjHBJEsG8w04U7PT3UwuVZOLnKJ+ndzUJWL1YHyqd/t+Xyx\nKKqmjwp0RWfQIe6NJasFDco0MEPhBP72RYY5Aa01x4bzwxeJLrCnF5PDKYnNt4BP\nwYfoNZHzAgMBAAECggEAKK3DldQqJp7M32N8d4xQDjddadEvNLM7VBVkuRE9EQlb\nuXdZnTBA6K1e8WBEOz/duuXBm84cCUX4hN6aZcDZC6+g73LhTA8JJzAypMGf20im\nMohdmRUhukIIw57rwAP0rZ97WbQ33LjS8fkLVoeQO6DmXk6d2AQbXJ0jslh82HN2\n9ndXcw/0CxtwaBlqZ40OD2Xlv5Hsrx21LQLeCwOKVROWoo04tJ0SPsDL9gDQv8bC\na3YWtAm4CRhmYgUpxcFM5DLmYuuMVJbRAnYT94kFuRND+Ws8TmDvXrQZJC+gkY+i\nwWycycfw3pNZOwbkfuFqpVWHTiyk7bMxyUbwx6R43QKBgQDgRz95MWikjLH4Cojz\nufUZFVAPv+qD1BPv0gm9q+TH8oHN8bUVIwnZGR1jz8FD4rp129xoB0P4VBlNRZ+m\nv76/FpIW3yoeTG4aGq0qsXDUce34frHubpMNy8raw1Qen3uji+0TjloNIa2kc/dv\npvynYg9ucALCcPIkLcLzV7mOTwKBgQC+6V6QWyTpZAq1XMpUM/sbdCXu+wpwiCrq\ny6CDqnSli7IglHCZYJsyPmqz/jee1imrxtEHM43LVl5b17gflcSQLSNRzKoMXAHW\n5Smbecmv7r9lKCA9QLS3EjnzqxV0Q4RK+e44BceZjLCJmuVifSJ9LxcOUpUGpvYl\nA0he1PadHQKBgQC5GfSICMBNtpUXm1JGnX5tEkr+hK1/2eQOdXJ2qUzrvPlmyItk\ng4OYOwMnoormhVTQ+wFglkaByJ8NSF4omG5MdctitxKi6P+h6cxrxQDTRahbA+3E\nVFxn8X9dqAJgN84qP80N69nkppwSc2ePLemuF9+WjTmp0t/1/hK9FfTePwKBgCXV\nogQBzCLkzKp8pSxi0NT0A8CEx+DdW4QErt7pHQzhzn9ea6I53wfqDsN1EhjMYJ3G\nXQ6MTQGLMFALRFYeHEJmb6V6ZgjlAwhPihth23KeYhfuB9WbyTSuzAVaVnogF0u0\nlF1N5+yqUSI0LFmEax1cA7m27AnRdN6I9AK3OdiRAoGBAMicaLak7aA27COzAm1+\n36446hMH0mn5qWGfqDD+12UdZgBKCI4PgQT1VjI3HaxGesYXbMTyP+xhyTUvZNtZ\nCEwVuJXzoGIiijEc6GjndX1/4Rmgrgla2U6yVP1IKYBx+PmgeHKfbB81yKAXR7wB\n9vrRf9P9ZYkSgwcv8FtGGSuN\n-----END PRIVATE KEY-----\n",
                        "client_email": "santosh1502@databricks-403018.iam.gserviceaccount.com",
                        "client_id": "114987527657142028330",
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/santosh1502%40databricks-403018.iam.gserviceaccount.com",
                        "universe_domain": "googleapis.com"
                        }
        return credentials

    def connect_to_gcp_and_get_data(self, file_name):
        gcp_file_path = f'transformed_data/{file_name}'

        credentials_info = self.getcredentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

        # Read the CSV file from GCP into a DataFrame
        blob = bucket.blob(gcp_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def get_data(self, file_name):
        df = self.connect_to_gcp_and_get_data(file_name)
        return df


class PostgresDataLoader:

    def __init__(self):
        self.db_config = {
            'dbname': 'shelter_outcomes_db',
            'user': 'postgres',
            'password': 'pgadmin',
            'host': '34.27.8.179',
            'port': '5432',
        }

    def get_queries(self, table_name):

        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"

        if table_name =="animaldimension":
            query = """CREATE TABLE IF NOT EXISTS animaldimension (
                            animal_key INT PRIMARY KEY,
                            animal_id VARCHAR,
                            name VARCHAR,
                            dob DATE,
                            reprod VARCHAR,
                            gender VARCHAR, 
                            animal_type VARCHAR NOT NULL,
                            breed VARCHAR,
                            color VARCHAR,
                            datetime TIMESTAMP
                        );
                        """
            alter_table_query = """ALTER TABLE animaldimension
                                ADD CONSTRAINT animal_key_unique UNIQUE (animal_key);
                                """
        elif table_name =="outcomedimension":
            query = """CREATE TABLE IF NOT EXISTS outcomedimension (
                            outcome_type_key INT PRIMARY KEY,
                            outcome_type VARCHAR NOT NULL
                        );
                        """
            alter_table_query = """ALTER TABLE outcomedimension
                                ADD CONSTRAINT outcometype_key_unique UNIQUE (outcome_type_key);
                                """
        elif table_name =="datedimension":
            query = """CREATE TABLE IF NOT EXISTS datedimension (
                            date_key INT PRIMARY KEY,
                            year_recorded INT2  NOT NULL,
                            month_recorded INT2  NOT NULL
                        );
                        """
            alter_table_query = """ALTER TABLE datedimension
                                ADD CONSTRAINT date_key_unique UNIQUE (date_key);
                                """
        else:
            query = """CREATE TABLE IF NOT EXISTS outcomesfact (
                            outcome_id SERIAL PRIMARY KEY,
                            animal_key INT,
                            date_key INT,
                            outcome_type_key INT,
                            FOREIGN KEY (animal_key) REFERENCES animaldimension(animal_key),
                            FOREIGN KEY (date_key) REFERENCES datedimension(date_key),
                            FOREIGN KEY (outcome_type_key) REFERENCES outcomedimension(outcome_type_key)
                        );
                        """
            alter_table_query = ";"
        return f"{drop_table_query}\n{query}\n{alter_table_query}".strip()
        #return f"{query}"

    def connect_to_postgres(self):
        connection = psycopg2.connect(**self.db_config)
        return connection

    def create_table(self, connection, table_query):
        print("Executing Create Table Queries...")
        cursor = connection.cursor()
        cursor.execute(table_query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def load_data_into_postgres(self, connection, gcp_data, table_name):
        cursor = connection.cursor()
        print(f"Dropping Table {table_name}")
        truncate_table = f"DROP TABLE {table_name};"
        cursor.execute(truncate_table)
        connection.commit()
        cursor.close()

        print(f"Loading data into PostgreSQL for table {table_name}")
        # Specify the PostgreSQL engine explicitly
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        # Write the DataFrame to PostgreSQL using the specified engine
        gcp_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Number of rows inserted for table {table_name}: {len(gcp_data)}")

def load_data_to_postgres(file_name, table_name):
    gcp_loader = GCPDataLoader()
    table_data_df = gcp_loader.get_data(file_name)

    postgres_dataloader = PostgresDataLoader()
    table_query = postgres_dataloader.get_queries(table_name)
    postgres_connection = postgres_dataloader.connect_to_postgres()

    postgres_dataloader.create_table(postgres_connection, table_query)
    postgres_dataloader.load_data_into_postgres(postgres_connection, table_data_df, table_name)
