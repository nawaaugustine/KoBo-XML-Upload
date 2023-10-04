import json
import pandas as pd
import requests
import xml.etree.ElementTree as ET

# Load the configuration file.
def load_config(config_file):
    """
    Load the JSON configuration file and return the configuration dictionary.
    
    Args:
        config_file (str): Path to the JSON configuration file.

    Returns:
        dict: Configuration dictionary.
    """
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config

# Process the submission data and create an XML tree structure.
def process_submission(row, project_uuid):
    """
    Create an XML tree structure for the given row, matching_rows, and project_uuid.
    Add various elements to the XML tree based on the input data and return the root
    of the XML tree.

    Args:
        row (pd.Series): A row from the parent dataframe.
        matching_rows (pd.DataFrame): Matching rows from the child dataframe.
        project_uuid (str): The project UUID.

    Returns:
        xml.etree.ElementTree.Element: Root of the XML tree structure.
    """
    
    root = ET.Element('data')
    root.set('id', project_uuid)
    root.set('xmlns:orx', 'http://openrosa.org/xforms')
    root.set('xmlns:jr', 'http://openrosa.org/javarosa')

    # set root level elements
    #ET.SubElement(root, 'start').text = str(row['start'])
    #ET.SubElement(root, 'end').text = str(row['end'])
    #ET.SubElement(root, 'today').text = str(row['today'])
   
    ET.SubElement(root, 'POR').text = str(row['POR'])
    ET.SubElement(root, 'start_date').text = str(row['start_date'])
    ET.SubElement(root, 'Amount').text = str(row['Amount'])

    
    # Although kobo may accept the submission without this meta element it is highly recommended by openrosa
    meta = ET.SubElement(root, 'meta')
    meta_instanceID = ET.SubElement(meta, 'instanceID')
    meta_instanceID.text = project_uuid

    return root

# Main function that drives the script.
def main():
    """
    Main function that reads Excel data into Pandas dataframes, iterates through
    the rows, creates XML entries, and posts them to the KoBoToolbox server.

    Steps:
    1. Load the configuration from the 'config.json' file.
    2. Read Excel data into Pandas dataframes for parent and child data.
    3. Construct the endpoint URL and headers for API requests to KoBoToolbox.
    4. Iterate through the rows in the parent dataframe, find the matching rows 
       in the child dataframe, and process the submissions by creating XML entries.
    5. Send the XML submission data to the KoBoToolbox server using HTTP POST requests.
    6. Print the submission status and response from the server.
    """
        
    config = load_config('config.json')
    
    #loads excel data into pandas dataframe
    df_root = pd.read_excel(config['parent_data_path'])

    endpoint = 'https://kobocat.unhcr.org/api/v1/submissions'
    headers = {'Authorization': f"Token {config['api_token']}"}

    # Iterate, create and post xml entry to KoBo
    for i, row in df_root.iterrows():
        root = process_submission(row, config['project_uuid'])

        xml_string = ET.tostring(root, encoding='utf-8', method='xml')
 
        payload = {'xml_submission_file': ('data.xml', xml_string, 'text/xml')}
        response = requests.post(endpoint, headers=headers, files=payload)

        print(f'Submission {i}: {response.status_code} {response.text}')

# Entry point for the script.
if __name__ == '__main__':
    main()