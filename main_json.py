import json
import uuid
import pandas as pd
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
import openpyxl
import pandas as pd

def configure_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        filename='app.log',
                        filemode='w')

def load_config(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config

def get_api_token(config, user_value):
    user_token_map = {
        "unhcr_prod": "api_token_UNHCR_PROD"
    }
    token_key = user_token_map.get(user_value)
    if token_key is None:
        logging.error(f'Unknown user value: {user_value}')
        raise ValueError(f'Unknown user value: {user_value}')
    return config[token_key]

def send_request(endpoint, headers, payload):
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(['POST'])  
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.post(endpoint, headers=headers, json=payload)
    return response

def safe_str(value):
    """
    Convert value to string safely, handling None and NaN values.
    
    Args:
        value (any): The value to convert to string.

    Returns:
        str: The value as a string, or an empty string if the value is None or NaN.
    """
    if pd.isna(value):
        return ''
    return str(value)

def format_date(value):
    """
    Format a date value to the desired string format.
    
    Args:
        value (any): The date value to format.

    Returns:
        str: The formatted date as a string, or an empty string if the value is None or NaN.
    """
    if pd.isna(value):
        return ''
    date = pd.to_datetime(value)
    return date.strftime('%Y-%m-%d')

def create_payload(parent_row, all_child_rows, project_uuid):
    """
    Create the payload for API submission, including data from repeat groups.

    Args:
        row (pd.Series): A row from the parent dataframe.
        project_uuid (str): The project UUID.
        child_data (pd.DataFrame): The child dataframe containing repeat group data.

    Returns:
        dict: The payload for API submission.
    """
            
    # Filter child rows to include only those that match the parent row's ID
    matching_child_rows = all_child_rows[all_child_rows['Parent_ID'] == parent_row['ID']]

    outflows = []
    for child_row in matching_child_rows.itertuples(index=False):
        outflow = {
            # Fields from child_row
            "Name": safe_str(child_row.Name),
            "Age": safe_str(child_row.Age),
            "Gender": safe_str(child_row.Gender)
        }
        outflows.append(outflow)

    payload = {
        "id": project_uuid,
        "submission": {
            "formhub": {
                "uuid": "5172fbc3b4584e15a26549c1d417c8bf"
            },
            "Family_ID": safe_str(parent_row['Family_ID']),
            "Address": safe_str(parent_row['Address']),
            "repeat_group": outflows,
            "__version__": "vFUMvJuvkQ7taaH4ragPXK",
            "meta": {
                "instanceID": f"uuid:{safe_str(uuid.uuid4())}"
            }
        }
    }
    return payload


def main():
    configure_logging()
    config = load_config('config.json')
    
    df_root = pd.read_excel(config['parent_data_path'])
    df_child = pd.read_excel(config['child_data_path'])  # Load the child data
    
    endpoint = 'https://kobocat.unhcr.org/api/v1/submissions'
    failed_logs = []  

    for i, row in tqdm(df_root.iterrows(), total=df_root.shape[0]):
        api_token = get_api_token(config, 'unhcr_prod')
        headers = {'Authorization': f"Token {api_token}", 'Content-Type': 'application/json'}
        
        # Corrected the order of arguments here
        payload = create_payload(row, df_child, config['project_uuid'])  
                
        try:
            response = send_request(endpoint, headers, payload)
        except requests.RequestException as e:
            logging.error(f'Submission {i} failed: {safe_str(e)}')
            failed_logs.append((i, None, safe_str(e)))
            continue
        
        if response.status_code != 201:
            logging.error(f'Submission {i}: {response.status_code} {response.text}')
            failed_logs.append((i, response.status_code, response.text))
        else:
            logging.info(f'Submission {i}: Success')
    
    if failed_logs:
        with pd.ExcelWriter('failed_logs.xlsx') as writer:
            pd.DataFrame(failed_logs, columns=['Row', 'Status_Code', 'Response']).to_excel(writer, index=False)

if __name__ == '__main__':
    main()

