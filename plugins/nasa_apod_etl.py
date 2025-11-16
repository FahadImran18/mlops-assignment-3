"""
NASA APOD ETL Functions

This module contains all the functions for extracting, transforming,
and loading NASA APOD data, as well as versioning with DVC and Git.
"""

import json
import pandas as pd
import requests
import os
import subprocess
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


def extract_apod_data(api_key: str, output_path: str, **context) -> str:
    """
    Step 1: Extract data from NASA APOD API
    
    Args:
        api_key: NASA API key (DEMO_KEY for demo)
        output_path: Path to save raw JSON response
        
    Returns:
        Path to the saved raw JSON file
    """
    logger.info(f"Extracting data from NASA APOD API with key: {api_key}")
    
    url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save raw JSON
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Successfully extracted data and saved to {output_path}")
        logger.info(f"Extracted data: {json.dumps(data, indent=2)}")
        
        return output_path
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error extracting data from API: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during extraction: {e}")
        raise


def transform_apod_data(input_path: str, output_path: str, **context) -> str:
    """
    Step 2: Transform raw JSON into clean, structured format
    
    Args:
        input_path: Path to raw JSON file
        output_path: Path to save cleaned JSON file
        
    Returns:
        Path to the cleaned JSON file
    """
    logger.info(f"Transforming data from {input_path}")
    
    try:
        # Load raw JSON
        with open(input_path, 'r') as f:
            raw_data = json.load(f)
        
        # Select specific fields of interest
        fields_of_interest = ['date', 'title', 'url', 'explanation', 'media_type', 'hdurl']
        
        # Create cleaned data dictionary
        cleaned_data = {}
        for field in fields_of_interest:
            cleaned_data[field] = raw_data.get(field, None)
        
        # Add metadata
        cleaned_data['extracted_at'] = datetime.now().isoformat()
        cleaned_data['copyright'] = raw_data.get('copyright', None)
        
        # Create DataFrame for validation and structure
        df = pd.DataFrame([cleaned_data])
        
        logger.info(f"Transformed data shape: {df.shape}")
        logger.info(f"Transformed data columns: {df.columns.tolist()}")
        logger.info(f"Sample data:\n{df.head()}")
        
        # Save cleaned JSON
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(cleaned_data, f, indent=2)
        
        logger.info(f"Successfully transformed data and saved to {output_path}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise


def load_to_postgres(input_path: str, postgres_conn_id: str, **context) -> None:
    """
    Step 3a: Load cleaned data to PostgreSQL database
    
    Args:
        input_path: Path to cleaned JSON file
        postgres_conn_id: Airflow connection ID for PostgreSQL
    """
    logger.info(f"Loading data to PostgreSQL from {input_path}")
    
    try:
        # Load cleaned data
        with open(input_path, 'r') as f:
            cleaned_data = json.load(f)
        
        # Create DataFrame
        df = pd.DataFrame([cleaned_data])
        
        # Get PostgreSQL connection
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        
        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            date DATE UNIQUE,
            title TEXT,
            url TEXT,
            explanation TEXT,
            media_type VARCHAR(50),
            hdurl TEXT,
            copyright TEXT,
            extracted_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        postgres_hook.run(create_table_sql)
        logger.info("Table 'apod_data' created or already exists")
        
        # Insert data
        # Convert date string to date object if present
        if 'date' in df.columns and df['date'].notna().any():
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Prepare data for insertion with conflict handling on date
        insert_sql = """
        INSERT INTO apod_data (date, title, url, explanation, media_type, hdurl, copyright, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
        """
        
        for _, row in df.iterrows():
            postgres_hook.run(
                insert_sql,
                parameters=(
                    row.get('date'),
                    row.get('title'),
                    row.get('url'),
                    row.get('explanation'),
                    row.get('media_type'),
                    row.get('hdurl'),
                    row.get('copyright'),
                    row.get('extracted_at')
                )
            )
        
        logger.info("Successfully loaded data to PostgreSQL")
        
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {e}")
        raise


def load_to_csv(input_path: str, output_path: str, **context) -> str:
    """
    Step 3b: Load cleaned data to CSV file
    
    Args:
        input_path: Path to cleaned JSON file
        output_path: Path to save CSV file
        
    Returns:
        Path to the saved CSV file
    """
    logger.info(f"Loading data to CSV from {input_path}")
    
    try:
        # Load cleaned data
        with open(input_path, 'r') as f:
            cleaned_data = json.load(f)
        
        # Create DataFrame
        df = pd.DataFrame([cleaned_data])
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Check if file exists to append or create new
        if os.path.exists(output_path):
            # Append to existing CSV
            existing_df = pd.read_csv(output_path)
            # Avoid duplicates based on date
            if 'date' in df.columns and 'date' in existing_df.columns:
                df = pd.concat([existing_df, df]).drop_duplicates(subset=['date'], keep='last')
            else:
                df = pd.concat([existing_df, df], ignore_index=True)
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        
        logger.info(f"Successfully saved data to CSV: {output_path}")
        logger.info(f"CSV file size: {os.path.getsize(output_path)} bytes")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error loading data to CSV: {e}")
        raise


def version_with_dvc(csv_path: str, dvc_repo_path: str, **context) -> None:
    """
    Step 4: Version the CSV file using DVC
    
    Args:
        csv_path: Path to the CSV file to version
        dvc_repo_path: Path to the DVC repository
    """
    logger.info(f"Versioning CSV file with DVC: {csv_path}")
    
    try:
        # Ensure DVC repo directory exists
        os.makedirs(dvc_repo_path, exist_ok=True)
        
        # Change to DVC repo directory
        original_cwd = os.getcwd()
        os.chdir(dvc_repo_path)
        
        # Initialize DVC if not already initialized
        if not os.path.exists(os.path.join(dvc_repo_path, '.dvc')):
            logger.info("Initializing DVC repository...")
            result = subprocess.run(
                ['dvc', 'init', '--no-scm'],
                capture_output=True,
                text=True,
                check=False
            )
            if result.returncode != 0 and 'already initialized' not in result.stderr.lower():
                logger.warning(f"DVC init warning: {result.stderr}")
        
        # Copy CSV to DVC repo if it's not already there
        csv_filename = os.path.basename(csv_path)
        repo_csv_path = os.path.join(dvc_repo_path, csv_filename)
        
        if csv_path != repo_csv_path:
            import shutil
            shutil.copy2(csv_path, repo_csv_path)
            logger.info(f"Copied CSV to DVC repo: {repo_csv_path}")
        
        # Add file to DVC tracking
        logger.info(f"Adding {csv_filename} to DVC tracking...")
        result = subprocess.run(
            ['dvc', 'add', csv_filename],
            capture_output=True,
            text=True,
            check=True
        )
        
        logger.info(f"DVC add output: {result.stdout}")
        logger.info(f"Successfully versioned {csv_filename} with DVC")
        
        # Verify .dvc file was created
        dvc_file = f"{csv_filename}.dvc"
        if os.path.exists(dvc_file):
            logger.info(f"DVC metadata file created: {dvc_file}")
        else:
            raise FileNotFoundError(f"DVC metadata file not found: {dvc_file}")
        
        # Return to original directory
        os.chdir(original_cwd)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"DVC command failed: {e.stderr}")
        os.chdir(original_cwd)
        raise
    except Exception as e:
        logger.error(f"Error versioning with DVC: {e}")
        os.chdir(original_cwd)
        raise


def commit_to_git(dvc_repo_path: str, commit_message: str, **context) -> None:
    """
    Step 5: Commit DVC metadata file to Git
    
    Args:
        dvc_repo_path: Path to the DVC/Git repository
        commit_message: Git commit message
    """
    logger.info(f"Committing DVC metadata to Git in {dvc_repo_path}")
    
    try:
        # Change to repo directory
        original_cwd = os.getcwd()
        os.chdir(dvc_repo_path)
        
        # Fix Git ownership issue (dubious ownership)
        logger.info("Configuring Git safe directory...")
        subprocess.run(
            ['git', 'config', '--global', '--add', 'safe.directory', dvc_repo_path],
            capture_output=True,
            text=True,
            check=False
        )
        
        # Initialize Git if not already initialized
        if not os.path.exists(os.path.join(dvc_repo_path, '.git')):
            logger.info("Initializing Git repository...")
            subprocess.run(
                ['git', 'init'],
                capture_output=True,
                text=True,
                check=True
            )
        
        # Configure Git user (required for commits)
        logger.info("Configuring Git user...")
        subprocess.run(
            ['git', 'config', 'user.email', 'airflow@mlops.local'],
            capture_output=True,
            text=True,
            check=True
        )
        subprocess.run(
            ['git', 'config', 'user.name', 'Airflow MLOps'],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Add .dvc files and .gitignore
        logger.info("Staging DVC metadata files...")
        subprocess.run(
            ['git', 'add', '*.dvc', '.dvcignore', '.gitignore'],
            capture_output=True,
            text=True,
            check=False  # Don't fail if no files match
        )
        
        # Also add .dvc directory if it exists
        if os.path.exists('.dvc'):
            subprocess.run(
                ['git', 'add', '.dvc'],
                capture_output=True,
                text=True,
                check=False
            )
        
        # Check if there are changes to commit
        result = subprocess.run(
            ['git', 'status', '--porcelain'],
            capture_output=True,
            text=True,
            check=True
        )
        
        if result.stdout.strip():
            # Commit changes
            logger.info(f"Committing changes with message: {commit_message}")
            result = subprocess.run(
                ['git', 'commit', '-m', commit_message],
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"Git commit successful: {result.stdout}")
        else:
            logger.info("No changes to commit (files already committed)")
        
        # Return to original directory
        os.chdir(original_cwd)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Git command failed: {e.stderr}")
        os.chdir(original_cwd)
        raise
    except Exception as e:
        logger.error(f"Error committing to Git: {e}")
        os.chdir(original_cwd)
        raise

