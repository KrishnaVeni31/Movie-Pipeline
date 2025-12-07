📘 Movie Pipeline — MovieLens + OMDb ETL

A simple data pipeline that loads MovieLens data, enriches it using the OMDb API, and stores everything in an SQLite database for analysis.

📑 Table of Contents

1️⃣ Introduction
2️⃣ How the System Works
3️⃣ Data Flow Diagram
4️⃣ Installation
5️⃣ Configuration
6️⃣ Running the Pipeline
7️⃣ Project Structure
8️⃣ Outputs

🟦 1️⃣ Introduction

The Movie Pipeline reads MovieLens data, adds extra movie details from the OMDb API, and saves everything in an SQLite database.

This document will help you run the full pipeline from start to finish.

🟩 2️⃣ How the System Works (Simple Explanation)

The system does 5 main things:

🔹 Extract – Reads MovieLens files
🔹 Transform – Cleans & prepares the data
🔹 Enrich – Calls OMDb API to get more movie info
🔹 Load – Saves everything into movies.db
🔹 Analyze – Runs SQL queries to show insights


 🟧 3️⃣ Data Flow Diagram 
   
   MovieLens CSVs
  movies.csv
  ratings.csv
      │
      ▼
    etl.py
  (Clean + Load)
      │
      ▼
enrich_missing.py ──► OMDb API
  (Add Metadata)
      │
      ▼
   movies.db
  (Final Database)
      │
      ▼
run_queries.py
  (Insights)


🟨 4️⃣ Installation

Follow these simple steps:

📥 1. Download the Project
git clone https://github.com/KrishnaVeni31/Movie-Pipeline.git
cd movie-pipeline

🧪 2. Create a Virtual Environment
python -m venv venv
venv\Scripts\activate     # Windows

📦 3. Install Dependencies
pip install -r requirements.txt

🟪 5️⃣ Configuration

You need an OMDb API Key.

🔑 Create and Add Your API Key to .env
OMDB_API_KEY=your_api_key_here


This key is used to enrich movie details like:

🎬 Director
📝 Plot
⏳ Runtime
💰 Box Office
📅 Release Date

🟥 6️⃣ Running the Pipeline

Run these 3 scripts one by one:

▶️ 1. Load MovieLens Data
python etl.py

🌐 2. Fetch OMDb Metadata
python enrich_missing.py

📊 3. Run SQL Queries
python run_queries.py


These steps create the database and show analysis results in the terminal.

🗂️ 7️⃣ Project Structure (Visual)
movie-pipeline/
│
├── etl.py               📥 ETL process
├── enrich_missing.py    🌐 OMDb metadata fetch
├── run_queries.py       📊 Run SQL insights
├── schema.sql           🗄️ Database design
├── queries.sql          🔎 Analysis queries
├── requirements.txt     📦 Dependencies
├── .env                 🔑 API key
└── movies.db            🗃️ Final database

🧾 8️⃣ Outputs

After running the pipeline, you will get: 

✅  movies.db — The complete movie database

✅  Movies with extra details like director, plot, runtime, box office, and release date

✅  SQL query results shown in the terminal

✅  A fully working ETL process that adds data from the OMDb API

