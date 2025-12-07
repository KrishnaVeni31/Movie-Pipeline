
# Movie Pipeline — MovieLens + OMDb ETL

A simple data pipeline that loads MovieLens data, enriches it using the OMDb API, and stores everything in an SQLite database for analysis.


# Steps to Run the Project
### 1. Clone the repository
git clone https://github.com/KrishnaVeni31/Movie-Pipeline.git

cd movie-pipeline

### 2. Set up Python environment
python -m venv venv

### Activate the environment:
- Windows: venv\Scripts\activate
- Mac/Linux: source venv/bin/activate

### 3. Install dependencies
pip install -r requirements.txt

### 4. Add your OMDb API key
- Open .env file
- Add your key:
OMDB_API_KEY=your_api_key_here
To Verify = echo $env:OMDB_API_KEY

### 5. Run the pipeline
Run these scripts in order:
1. python etl.py → Load MovieLens data into SQLite
2. python enrich_missing.py → Add extra movie details from OMDb API
3. python run_queries.py → Run some example queries

## Project Structure
### Movie-Pipeline
- etl.py → Loads CSV data into the database
- enrich_missing.py → Adds extra movie details from OMDb
- run_queries.py → Runs SQL queries and shows results
- schema.sql → Defines database tables
- queries.sql → Contains SQL queries used in the project
- .env → Keeps your OMDb API key
- requirements.txt → List of required Python packages
- movies.db → The database created after running the ETL

## Summary
- Reads movie data from CSVs
- Adds director, plot, box office, runtime, etc. using OMDb
- Saves everything in movies.db for analysis
- Easy to query and explore movie data
