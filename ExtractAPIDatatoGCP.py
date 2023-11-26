import pytz
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage


# Set the time zone to Mountain Time
mountain_time_zone = pytz.timezone('US/Mountain')


def extract_api_data(limit=50000, order='animal_id'):
    """
    Function to extract data from data.austintexas.gov API.
    """
    api_url = 'https://data.austintexas.gov/resource/9t4d-g238.json'

    api_key = 'cx1ax1xme7m1r8bler55ab7jk'

    headers = { 
        'accept': "application/json", 
        'apikey': api_key,
    }

    loop = 0
    data = []

    while loop < 157000:  # Iterating through all the records
        params = {
            '$limit': str(limit),
            '$offset': str(loop),
            '$order': order,
        }

        api_response = requests.get(api_url, headers=headers, params=params)
        print("response : ", api_response)
        latest_data = api_response.json()

        # Break the loop if there is no data further
        if not latest_data:
            break

        data.extend(latest_data)
        loop += limit

    return data


def create_data(data):
    columns = [
        'animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
        'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
        'breed', 'color'
    ]
    
    df_list = []
    for entry in data:
        row_df = [entry.get(column, None) for column in columns]
        df_list.append(row_df)

    df = pd.DataFrame(df_list, columns=columns)
    return df


def gcs_upload(dataframe, bucket_name, file_path):
    """
    Upload a DataFrame to a Google Cloud Storage bucket using service account credentials.
    """
    print("Writing data to GCS.....")
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

    client_info = storage.Client.from_service_account_info(credentials)
    csv_df = dataframe.to_csv(index=False)

    bucket = client_info.get_bucket(bucket_name)

    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    file_path_formatted = file_path.format(current_date, current_date)

    blob = bucket.blob(file_path_formatted)
    blob.upload_from_string(csv_df, content_type='text/csv')
    print(f"Completed writing data to GCS with date: {current_date}.")


def main():
    data_extracted = extract_api_data(limit=50000, order='animal_id')
    shelter_data = create_data(data_extracted)

    gcs_bucket_name = 'data_center_lab3'
    gcs_file_path = 'data/{}/outcomes_{}.csv'

    gcs_upload(shelter_data, gcs_bucket_name, gcs_file_path)
 