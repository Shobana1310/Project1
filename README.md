
# Project Title

Youtube Analysis Tool 
[**Click Here To See The App**](https://youtube-analysis-tool-1.streamlit.app/)

## Use Cases

**For YouTube Content Creators**
Understand your audience and optimize the content strategy.Identify top-performing videos also and aim to achieve similar success

**For Marketers**
Analyze competitor channels and refine your marketing strategy.Measure the effectiveness of video campaigns and promotions

**For Analyst**
Gain in-depth insights into YouTube channel performance.Use data-driven insights to make informed decisions

## Tech Stack

Python,
Youtube Api Integration,
MongoDB Cloud Atlas,
SQL,
Streamlit,
Azure 


## 🛠 Skills
Programming: Proficient in Python .

API Integration: Experienced with YouTube API.

Data Management: MongoDB (Atlas) and MySQL.

Data Processing: Pandas, SQL for analysis.

ETL Process: PyMongo and SqlAlchemy for MongoDB and MySQL.

UI Development: Streamlit for user-friendly interfaces.

Data Warehousing: Transforming and loading data between MongoDB and MySQL.

IDE: Visual Studio Code.

Cloud Management:Azure

Problem-Solving:
Ability to identify and resolve issues that may arise during the data extraction, transformation, and loading processes.






## Installation 

To run the YouTube Data Harvesting and Warehousing project, follow these steps:

1.Install Python: Ensure that the Python programming language is installed on your machine.

Install Required Libraries:

```pip install streamlit pymongo sqlalchemy PyMySQL pandas google-api-python-client dotenv```

Set Up Google API:

Create a Google API project on the `Google Cloud Console`.

Obtain API credentials (JSON file) with access to the YouTube Data API v3.
Place the API credentials file in the project directory under the name `google_api_credentials.json`.

Configure Database:

Set up a MongoDB database and ensure it is running.
Set up a MySQL database and ensure it is running.
Configure Application:

Copy the `config.sample.ini` file and rename it to `config.ini`.
Update the `config.ini` file with your Google API credentials, MongoDB, and MySQL connection details.
Run the Application:

streamlit run yourfilename.py
Access the Streamlit application at http://localhost:8501 in your web browser.
    
## methods
## Data Collection from YouTube API:

Utilized YouTube API with a valid API key.
Gathered data including channel details, video details, and comments.

## Function Creation:

Developed specific functions for handling different aspects of the data.
Functions for obtaining channel details, video details, and comments details were created.

## Data Storage in MongoDB Atlas:

Established a connection to MongoDB Atlas, a cloud-based MongoDB service.
Pushed the collected JSON data into MongoDB Atlas.

## MySQL Table Creation:

Established a connection to MySQL.
Created a table structure in MySQL for storing the YouTube data.

## Data Transfer from MongoDB to MySQL:

Used SQL Alchemy, a SQL toolkit for Python, to facilitate the data transfer.
Transferred data from MongoDB to MySQL using appropriate methods.

## SQL Query Writing:

Developed SQL queries to retrieve, manipulate, or analyze data stored in the MySQL database.

## Integration with Streamlit:

Connected the project with Streamlit, a Python library for creating web applications with data visualization.
Utilized Streamlit to present and interact with the YouTube data.




## Confidential Credintials

Ensure that confidential credentials are securely managed. Replace the placeholder values in the configuration files with your actual credentials. Do not expose sensitive information in public repositories.

Note: Ensure to replace placeholder text with specific details about your project. Note: Follow ethical scraping practices, obtain necessary permissions, and comply with YouTube's terms of service when using the YouTube API.



## Contribution Guidelines

If you wish to contribute to the project, you are always welcome. If you encounter any issues or have suggestions for improvements, please feel free to reach me.




## Reference

[Python Documentation](https://docs.python.org/3/)

[Streamlit Documentation](https://docs.streamlit.io/library/api-reference)

[Youtube Api Documentation](https://developers.google.com/youtube/v3/docs/)

[MongoDB Documentation](https://www.mongodb.com/docs/)

[MySQL Documentation](https://dev.mysql.com/doc/)

[SQLAlchemy Documentation](https://docs.sqlalchemy.org/en/20/)

[PyMySQL Documentation](https://pymysql.readthedocs.io/en/latest/)

[Pandas Documentation](https://pandas.pydata.org/docs/)

## Demo Video
![Project](https://github.com/Shobana1310/Youtube-Analysis-Tool/raw/main/images/presentation.gif)


## 🔗 Contact

[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/shobana-v-534b472a2/)

