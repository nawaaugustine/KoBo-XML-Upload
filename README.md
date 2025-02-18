# KoboCAT Data Submission Script

This repository contains a Python script that automates data submissions to KoboCAT. The script reads parent and child data from Excel files, builds a payload that supports multiple independent repeat groups, and sends the data via an API call with retries and error handling.

## Features

- **Multiple Repeat Groups:** Supports independent repeat groups within a single submission.
- **Configurable:** All key parameters (API token, file paths, formhub details, etc.) are defined in a JSON config file.
- **Robust Error Handling:** Includes logging, retries for API requests, and error logging to Excel.
- **Modular Code:** Well-structured functions with inline documentation.

## Prerequisites

- Python 3.6 or above
- Required Python packages:
  - `pandas`
  - `requests`
  - `openpyxl`
  - `tqdm`

You can install the dependencies using:

```bash
pip install -r requirements.txt
```

## Configuration

A sample configuration file (`config.sample.json`) is provided. Copy this file to `config.json` and update the values as needed.

### Sample `config.sample.json`

```json
{
    "api_token_UNHCR_PROD": "your_api_token_here",
    "parent_data_path": "./data/sample_parent_data.xlsx",
    "child_data_path": "./data/sample_child_data.xlsx",
    "project_uuid": "your_project_uid_here",
    "formhub_uuid": "your_formhub_id_here",
    "formhub_version": "your_form_current_version_here",
    "repeat_groups": {
      "group1": {
        "data_path": "./data/sample_child_data.xlsx",
        "filter_column": "Parent_ID",
        "fields": {
          "Name": "Name",
          "Age": "Age",
          "Gender": "Gender"
        }
      },
      "group2": {
        "data_path": "./data/another_child_data.xlsx",
        "filter_column": "Parent_ID",
        "fields": {
          "phone": "phone",
          "email": "email",
          "address": "address",
          "country": "country",
          "region": "region",
          "postalZip": "postalZip"
        }
      }
    }
  }
```

**Note:**  
- Ensure that all file paths, tokens, and formhub details are correctly updated.
- If you are using a single repeat group, you can omit the `"repeat_groups"` key and the script will default to using `"child_data_path"` with the default field mapping.

## Running the Script

To run the script, simply execute:

```bash
python your_script_name.py
```

The script will:
- Load the configuration from `config.json`.
- Read the parent and child Excel files.
- Build and send the payload for each parent record.
- Log successful and failed submissions in `app.log` and `failed_logs.xlsx`, respectively.

## Troubleshooting

- **Logging:** Check the `app.log` file for detailed error messages.
- **Excel Files:** Verify that your Excel files contain the required columns (e.g., `ID`, `Family_ID`, `Address` for parent data; `Parent_ID` and other fields for child data).
- **API Issues:** Ensure the API token and endpoint URL in `config.json` are correct.
- **Network Issues:** The script includes retries for API calls. If submissions are failing, check your network connection or API server status.

## License

[MIT License](LICENSE)

## Contact

For any issues or suggestions, please open an issue in this repository.

