# KoBoToolbox Submission Automation

This project automates the process of submitting data to KoBoToolbox by reading data from an Excel file, creating XML entries, and posting them to a KoBoToolbox server using HTTP POST requests.

## Description

The script is designed to read data from an Excel file, convert each row of data into an XML entry following a specific structure, and then send these XML entries to a KoBoToolbox server. This automation facilitates efficient data submission, especially when dealing with a large volume of data.

## Installation

Clone the repository to your local machine:

```bash
git clone https://github.com/your-username/your-repo.git
```

Navigate to the project directory:

```bash
cd your-repo
```

Install the required dependencies using pip:

```bash
pip install -r requirements.txt
```

## Usage

1. Update the `config.json` file with the necessary information including the path to the Excel file, your KoBoToolbox API token, and project UUID.

    ```json
    {
        "parent_data_path": "path/to/your/excel/file.xlsx",
        "api_token": "your-api-token",
        "project_uuid": "your-project-uuid"
    }
    ```

2. Run the script:

```bash
python main.py
```

## Configuration

The `config.json` file contains the following fields:

- `parent_data_path`: The path to the Excel file containing the data to be submitted.
- `api_token`: Your KoBoToolbox API token.
- `project_uuid`: The UUID of your KoBoToolbox project.

Make sure to add `config.json` to your `.gitignore` file to prevent sharing sensitive data.

## Contributing

If you wish to contribute to this project, feel free to fork the repository, make your changes, and open a pull request.

## License

MIT

## Contact

If you have any questions or feedback, feel free to open an issue or contact the project maintainer.
