import pandas as pd
import logging
import os
from typing import List, Dict
from datetime import datetime

class JobManager:
    def __init__(self, csv_path: str = "job_listings.csv"):
        self.csv_path = csv_path
        self.temp_path = f"{csv_path}.temp"
        
    def get_existing_job_ids(self) -> set:
        """Get set of existing job IDs from CSV file"""
        if not os.path.exists(self.csv_path):
            return set()
        
        try:
            df = pd.read_csv(self.csv_path)
            return set(df['job_id'].values)
        except Exception as e:
            logging.error(f"Error reading existing jobs: {e}")
            return set()

    def save_new_jobs(self, jobs: List[Dict]) -> None:
        """
        Safely save new jobs to CSV file, avoiding duplicates
        """
        existing_ids = self.get_existing_job_ids()
        
        # Filter out duplicates
        new_jobs = [
            job for job in jobs 
            if job['job_id'] not in existing_ids
        ]
        
        if not new_jobs:
            logging.info("No new jobs to save")
            return

        try:
            # Create new DataFrame with new jobs
            new_df = pd.DataFrame(new_jobs)
            
            if os.path.exists(self.csv_path):
                # Read existing CSV and concatenate with new jobs
                existing_df = pd.read_csv(self.csv_path)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df

            # Save to temporary file first
            combined_df.to_csv(self.temp_path, index=False)
            
            # If successful, rename temp file to actual file
            if os.path.exists(self.csv_path):
                os.remove(self.csv_path)
            os.rename(self.temp_path, self.csv_path)
            
            logging.info(f"Successfully saved {len(new_jobs)} new jobs to {self.csv_path}")
            
        except Exception as e:
            logging.error(f"Error saving jobs to CSV: {e}")
            if os.path.exists(self.temp_path):
                os.remove(self.temp_path) 