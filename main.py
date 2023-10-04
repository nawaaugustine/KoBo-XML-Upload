import json
import logging
import pandas as pd
import requests
from lxml import etree as ET

def load_config(config_file):
    """
    Load and validate the JSON configuration file.
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError as e:
        logging.error(f"Configuration file not found: {e}")
        raise

    required_keys = ['parent_data_path', 'api_token', 'project_uuid']
    for key in required_keys:
        if key not in config:
            logging.error(f"Missing required key in configuration: {key}")
            raise ValueError(f"Missing required key in configuration: {key}")

    return config

def process_submission(row, project_uuid):
    """
    Create an XML tree structure for the given row and project_uuid.
    """
    root = ET.Element('data', {
        'id': project_uuid,
        'xmlns:orx': 'http://openrosa.org/xforms',
        'xmlns:jr': 'http://openrosa.org/javarosa'
    })

    root.extend([
        ET.Element('POR', text=str(row['POR'])),
        ET.Element('start_date', text=str(row['start_date'])),
        ET.Element('Amount', text=str(row['Amount'])),
        ET.Element('meta', {
            'instanceID': project_uuid
        })
    ])

    return root

def post_submission(xml_root, endpoint, headers):
    """
    Post the XML submission data to the KoBoToolbox server.
    """
    xml_string = ET.tostring(xml_root, encoding='utf-8', pretty_print=True)
    payload = {'xml_submission_file': ('data.xml', xml_string, 'text/xml')}
    try:
        response = requests.post(endpoint, headers=headers, files=payload)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to post submission: {e}")
        raise

def setup_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def main(config_file='config.json'):
    setup_logging()
    
    config = load_config(config_file)
    df_root = pd.read_excel(config['parent_data_path'], chunksize=100)

    endpoint = 'https://kobocat.unhcr.org/api/v1/submissions'
    headers = {'Authorization': f"Token {config['api_token']}"}

    for chunk in df_root:
        for i, row in chunk.iterrows():
            xml_root = process_submission(row, config['project_uuid'])
            post_submission(xml_root, endpoint, headers)
            logging.info(f'Successfully submitted row {i}')

if __name__ == '__main__':
    main()
