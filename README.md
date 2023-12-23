# YouTube Data Harvesting and Warehousing

## Overview

This Python application automates data extraction from YouTube, leveraging SQL, MongoDB, and Streamlit. The project is designed for quick and efficient extraction of channel data, video details, and comments, offering insights for analysis.

## Features

- Utilizes the YouTube API for extracting channel data.
- Retrieves video details and comments, storing them in MongoDB.
- Populates a MySQL database with the extracted data.
- Streamlit interface with tabs for easy navigation.
- Provides insightful queries in the 'Insights' tab for data analysis.

## Prerequisites

Before running the application, make sure you have the following:

- Python installed (version 3.6 or later)
- Required Python libraries (urllib.parse, re, pandas, datetime, streamlit, pymongo, googleapiclient, mysql.connector)

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/nirmalinigoa/Capstone_1-GUVI.git
    cd Capstone_1-GUVI
    ```

2. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. Set up your YouTube API key and MongoDB credentials.

2. Update MySQL connection details in the code.

3. Run the application:

    ```bash
    streamlit run your_application.py
    ```

4. Navigate to the 'Home' tab, enter a YouTube channel ID, and click 'EXTRACT' to fetch data.

5. Explore extracted data in the 'Insights' tab by selecting specific questions.

6. For additional details, check the 'About' tab.

## Configuration

Adjust the configuration parameters in the code to customize the application according to your requirements.

## Issues and Contributions

If you encounter any issues or have suggestions for improvement, please open an issue or create a pull request. Contributions are welcome!

## License

This project is licensed under the [MIT License](LICENSE).

