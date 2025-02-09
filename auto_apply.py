from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Set
import os

class JobApplicant:
    def __init__(self, resume_path: str):
        self.resume_path = resume_path
        self.logger = logging.getLogger(__name__)
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            filename='job_applications.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def apply_to_job(self, job: Dict) -> bool:
        """
        Attempts to apply to a job posting
        Returns: bool indicating success/failure
        """
        driver = webdriver.Chrome()
        success = False
        
        try:
            driver.get(job['url'])
            
            # Wait for upload button
            upload_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='file']"))
            )
            upload_button.send_keys(self.resume_path)
            
            # Wait for submit button
            submit_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Apply')]"))
            )
            submit_button.click()
            
            # Wait for confirmation
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Application submitted')]"))
            )
            
            success = True
            self.logger.info(f"Successfully applied to {job['title']} at {job['company']}")
            
        except Exception as e:
            self.logger.error(f"Failed to apply to {job['url']}: {str(e)}")
            
        finally:
            driver.quit()
            return success

def save_to_csv(jobs: List[Dict], filename: str = None):
    if filename is None:
        filename = f"job_listings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    df = pd.DataFrame(jobs)
    df.to_csv(filename, index=False)
    logging.info(f"Saved {len(jobs)} jobs to {filename}")

class JobManager:
    def __init__(self, csv_path: str = "job_listings.csv"):
        self.csv_path = csv_path
        self.temp_path = f"{csv_path}.temp"
        
    def get_existing_job_ids(self) -> Set[str]:
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
            # Clean up temp file if it exists
            if os.path.exists(self.temp_path):
                os.remove(self.temp_path)

