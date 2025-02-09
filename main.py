import schedule
import time
from typing import Dict
import logging
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone
import docx

from resume_match import ResumeMatchEngine
from job_fetch import JobFetchManager
from job_manager import JobManager

load_dotenv()

class JobSearchPipeline:
    def __init__(self, config: Dict):
        self.config = config
        self.resume_matcher = ResumeMatchEngine()
        self.job_fetcher = JobFetchManager()
        self.job_manager = JobManager(config['output_csv'])
        
        # Load resume text directly from Word document
        self.resume_text = self._extract_text_from_docx(config['resume_path'])

    def _extract_text_from_docx(self, docx_path: str) -> str:
        """Extract text from a Word document."""
        doc = docx.Document(docx_path)
        return '\n'.join([paragraph.text for paragraph in doc.paragraphs])

    def run_pipeline(self):
        try:
            # 1. Fetch jobs
            logging.info("Fetching jobs...")
            jobs = self.job_fetcher.fetch_all_jobs(
                what=self.config['search_query'],
                where=self.config['location'],
                max_results=self.config['max_jobs']
            )
            
            # 2. Match jobs with rate limiting
            logging.info("Matching jobs to resume...")
            for job in jobs:
                try:
                    match_result = self.resume_matcher.match_job_to_resume(
                        job['description'], 
                        self.resume_text
                    )
                    job.update(match_result)
                    time.sleep(1)  # Add 1 second delay between API calls
                except Exception as e:
                    logging.error(f"Error in resume matching: {e}")
                    if "429" in str(e):  # Rate limit hit
                        logging.info("Rate limit reached, waiting 60 seconds...")
                        time.sleep(60)
                        try:
                            match_result = self.resume_matcher.match_job_to_resume(
                                job['description'], 
                                self.resume_text
                            )
                            job.update(match_result)
                        except Exception as retry_e:
                            logging.error(f"Retry failed: {retry_e}")
                            continue

            # 3. Save new jobs to CSV
            self.job_manager.save_new_jobs(jobs)
            logging.info(f"Pipeline completed. Processed {len(jobs)} jobs.")

        except Exception as e:
            logging.error(f"Pipeline error: {e}")

def main():
    config = {
        'search_query': "Machine Learning Engineer",
        'location': "us",
        'resume_path': str(Path.home() / "OneDrive" / "Documents" / "Kamal_resume_MLE.docx"),
        'output_csv': "job_listings.csv",
        'max_jobs': 1000
    }

    pipeline = JobSearchPipeline(config)
    
    # Schedule daily run
    schedule.every().day.at("08:00").do(pipeline.run_pipeline)

    # Initial run
    pipeline.run_pipeline()

    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()
