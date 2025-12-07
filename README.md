Movie Pipeline — MovieLens + OMDb ETL

A simple data pipeline that loads MovieLens data, enriches it using the OMDb API, and stores everything in an SQLite database for analysis.

Table of Contents 
1.Introduction
2.How the System Works
3.Data Flow Diagram
4.Installation
5.Configuration
6.Running the Pipeline
7.Project Structure
8.Outputs

1. Introduction
The Movie Pipeline reads MovieLens data, adds extra movie details from the OMDb API, and saves everything in an SQLite database.
This documentation explains how to set up and run the full pipeline.

2. How the System Works (Simple Explanation)
The system performs the following steps:
 • Extract – Reads MovieLens CSV files
 • Transform – Cleans and prepares the data
 • Enrich – Fetches additional movie details from the OMDb API
 • Load – Stores all processed data into movies.db
 • Analyze – Runs SQL queries to generate insights

4. Data Flow Diagram
MovieLens CSVs
movies.csv
ratings.csv
      |
      v
   etl.py
(Clean + Load)
      |
      v
enrich_missing.py ----> OMDb API
(Add Metadata)
      |
      v
 movies.db
(Final Database)
      |
      v
run_queries.py
(Insights)

5. Installation
 • Follow these steps:
1. Download the Project
git clone https://github.com/KrishnaVeni31/Movie-Pipeline.git
cd movie-pipeline

2. Create a Virtual Environment
python -m venv venv
venv\Scripts\activate     # Windows

3. Install Dependencies
pip install -r requirements.txt

5. Configuration
The pipeline requires an OMDb API key.
 • Add your API key inside the .env file:
OMDB_API_KEY=your_api_key_here
 • This key is used to fetch additional movie details such as director, plot, runtime, box office, and release date.

6. Running the Pipeline
Run the scripts in this order:
1. Load MovieLens Data
python etl.py

2. Fetch OMDb Metadata
python enrich_missing.py

3. Run SQL Queries
python run_queries.py
 • These scripts will build the database, enrich it, and display analysis results.

7. Project Structure
movie-pipeline/
│
├── etl.py               - ETL process
├── enrich_missing.py    - Fetch OMDb metadata
├── run_queries.py       - Run SQL insights
├── schema.sql           - Database schema
├── queries.sql          - Analysis queries
├── requirements.txt     - Python dependencies
├── .env                 - API key
└── movies.db            - Final generated database

8. Outputs
After running the entire pipeline, you will have:
 • A complete SQLite database (movies.db)
 • Enriched movie details including director, plot, runtime, box office, and release date
 • SQL query results printed in the terminal
 • A fully working ETL process that integrates with the OMDb API

