import json
import pandas as pd
import requests
import xml.etree.ElementTree as ET


def load_config(config_file):

    with open(config_file, 'r') as f:
        config = json.load(f)
    return config


def process_submission(row, project_uuid):
    root = ET.Element('data')
    root.set('id', project_uuid)
    root.set('xmlns:orx', 'http://openrosa.org/xforms')
    root.set('xmlns:jr', 'http://openrosa.org/javarosa')

    ET.SubElement(root, 'POR').text = str(row['POR'])
    ET.SubElement(root, 'start_date').text = str(row['start_date'])
    ET.SubElement(root, 'Amount').text = str(row['Amount'])

    meta = ET.SubElement(root, 'meta')
    meta_instanceID = ET.SubElement(meta, 'instanceID')
    meta_instanceID.text = project_uuid

    return root

def main():
  
    config = load_config('config.json')

    df_root = pd.read_excel(config['parent_data_path'])

    endpoint = 'https://kobocat.unhcr.org/api/v1/submissions'
    headers = {'Authorization': f"Token {config['api_token']}"}

    for i, row in df_root.iterrows():
        root = process_submission(row, config['project_uuid'])

        xml_string = ET.tostring(root, encoding='utf-8', method='xml')
 
        payload = {'xml_submission_file': ('data.xml', xml_string, 'text/xml')}
        response = requests.post(endpoint, headers=headers, files=payload)

        print(f'Submission {i}: {response.status_code} {response.text}')

if __name__ == '__main__':
    main()
