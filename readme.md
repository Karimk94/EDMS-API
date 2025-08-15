EDMS Middleware API

This application serves as the central processing hub for the EDMS ecosystem. It exposes a single API endpoint that, when called, fetches a batch of documents from the Oracle database, processes them through various AI services (Captioning, OCR, Face Recognition), and updates the database with the results.
This service is designed to contain all the complex business logic, database connections, and external API calls, isolating them from the client or trigger (the EDMS Crawler Service).
Features
Connects to an Oracle database to fetch and update document information.
Orchestrates calls to multiple external AI APIs in a specific sequence.
Intelligently skips already completed processing steps to avoid redundant work.
Performs all database updates in a single, atomic transaction per document.
Provides a single, simple endpoint (/process-batch) to run the entire workflow.
Setup and Installation
Prerequisites:
Python 3.x
Oracle Instant Client libraries installed and accessible via the system PATH.
Configuration:
Create a .env file based on the project's file structure.
Fill in all the required database credentials and the URLs for the downstream AI APIs.
Installation:
Open a command prompt in the project directory.
Create a virtual environment: python -m venv venv
Activate it: venv\Scripts\activate
Install dependencies: pip install -r requirements.txt
Running the API:
From the activated virtual environment, run the application using the provided batch file: run_api.bat.
The API will be available at http://localhost:5000.
