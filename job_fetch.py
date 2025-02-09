import requests
import os
from datetime import datetime, timezone
from typing import List, Dict, Optional
from abc import ABC, abstractmethod
from dotenv import load_dotenv
import time  # Add this import
import pandas as pd

load_dotenv()

class JobFetcher(ABC):
    @abstractmethod
    def fetch_jobs(self, what: str, max_results: int = 10000, where: Optional[str] = None) -> List[Dict]:
        pass

class AdzunaJobFetcher(JobFetcher):
    def __init__(self):
        self.app_id = os.getenv("ADZUNA_APP_ID")
        self.app_key = os.getenv("ADZUNA_APP_KEY")
        self.base_url = "https://api.adzuna.com/v1/api/jobs"
        self.country = "us"  # or 'us', 'ca', etc.
        self.results_per_page = 50  # Adzuna's max per page
        self.request_delay = 1    # Delay between requests in seconds
        self.retry_delay = 60     # Delay when rate limited in seconds
        self.max_retries = 3      # Maximum number of retry attempts

    def _make_api_request(self, url: str, params: Dict) -> Dict:
        """
        Make API request with rate limiting handling
        """
        for attempt in range(self.max_retries):
            try:
                # Add delay between requests
                if attempt > 0:
                    time.sleep(self.request_delay)
                
                response = requests.get(url, params=params)
                
                # Handle rate limiting
                if response.status_code == 429:
                    wait_time = self.retry_delay
                    print(f"Rate limit reached. Waiting {wait_time} seconds... (Attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    print(f"Request error: {e}")
                    print(f"Retrying in {self.request_delay} seconds... (Attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(self.request_delay)
                else:
                    raise

        raise Exception("Max retries exceeded")

    def fetch_jobs(
        self, 
        what: str, 
        max_results: int, 
        where: Optional[str] = None  # Changed to None to fetch all jobs
    ) -> List[Dict]:
        """
        Fetch all available jobs from Adzuna API with rate limiting
        """
        all_jobs = []
        page = 1
        
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": self.results_per_page,
            "what": what,
            "content-type": "application/json",
        }
        
        if where:
            params["where"] = where

        try:
            while True:  # Continue until no more results
                print(f"Fetching page {page}...")
                
                try:
                    data = self._make_api_request(
                        f"{self.base_url}/{self.country}/search/{page}",
                        params
                    )

                    results = data.get("results", [])
                    if not results:  # No more results available
                        print("No more results available")
                        break

                    for job in results:
                        # Generate a unique identifier for each job
                        job_id = f"{job.get('company', {}).get('display_name')}_{job.get('title')}_{job.get('location', {}).get('display_name')}"
                        
                        all_jobs.append({
                            "job_id": job_id,
                            "title": job.get("title"),
                            "company": job.get("company", {}).get("display_name"),
                            "location": job.get("location", {}).get("display_name"),
                            "description": job.get("description"),
                            "salary_min": job.get("salary_min"),
                            "salary_max": job.get("salary_max"),
                            "url": job.get("redirect_url"),
                            "created": job.get("created"),
                            "source": "Adzuna",
                            "fetched_at": datetime.now(timezone.utc).isoformat()
                        })

                    print(f"Fetched {len(all_jobs)} jobs so far...")

                    # Break if we've reached max_results (if specified)
                    if max_results and len(all_jobs) >= max_results:
                        print(f"Reached maximum requested results: {max_results}")
                        break

                    # Add delay between successful page fetches
                    time.sleep(self.request_delay)
                    page += 1

                except Exception as e:
                    print(f"Error fetching page {page}: {e}")
                    break

            return all_jobs[:max_results] if max_results else all_jobs

        except Exception as e:
            print(f"Error in fetch_jobs: {e}")
            return all_jobs  # Return any jobs we've collected so far

class JobFetchManager:
    def __init__(self):
        self.fetchers = [
            AdzunaJobFetcher(),
            # Add more job fetchers here
        ]

    def fetch_all_jobs(self, what: str, max_results: int = 10000, where: Optional[str] = None) -> List[Dict]:
        all_jobs = []
        for fetcher in self.fetchers:
            try:
                jobs = fetcher.fetch_jobs(what, max_results, where)
                all_jobs.extend(jobs)
            except Exception as e:
                print(f"Error with {fetcher.__class__.__name__}: {e}")
        
        return all_jobs
