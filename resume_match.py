import google.generativeai as genai
from typing import Dict
import os
import json
from dotenv import load_dotenv
import time
import random

load_dotenv()

class ResumeMatchEngine:
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-2.0-flash')
        self.max_retries = 5
        self.base_delay = 2  # Base delay in seconds

    def _exponential_backoff(self, retry_count: int) -> float:
        """Calculate exponential backoff time with jitter."""
        delay = min(300, self.base_delay * (2 ** retry_count))  # Cap at 5 minutes
        jitter = random.uniform(0, 0.1 * delay)  # Add 0-10% jitter
        return delay + jitter

    def match_job_to_resume(self, job_desc: str, resume_text: str) -> Dict:
        """
        Match a job description against a resume using Google's Gemini Pro model.
        Includes rate limit handling with exponential backoff.
        
        Returns:
            Dict containing match score and detailed feedback
        """
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                prompt = f"""
                Analyze the following job description and resume for compatibility:
                
                JOB DESCRIPTION:
                {job_desc}

                RESUME:
                {resume_text}

                Provide the following in your response:
                1. A match score from 0 to 100
                2. Key matching skills
                3. Missing skills or qualifications
                
                Return your response strictly in the following JSON format:
                {{
                    "score": <number>,
                    "matching_skills": [<list of strings>],
                    "missing_skills": [<list of strings>]
                }}
                """

                response = self.model.generate_content(prompt)
                #print("Raw response:", response.text)
                
                # Clean and validate the response
                try:
                    text = response.text
                    start = text.find('{')
                    end = text.rfind('}') + 1
                    if start != -1 and end != 0:
                        json_str = text[start:end]
                        result = json.loads(json_str)
                    else:
                        raise ValueError("No JSON object found in response")
                        
                    required_fields = ['score', 'matching_skills', 'missing_skills']
                    if not all(field in result for field in required_fields):
                        raise ValueError("Missing required fields in response")
                        
                    return result
                    
                except json.JSONDecodeError as e:
                    print(f"JSON parsing error: {e}")
                    print(f"Attempted to parse: {text}")
                    return {
                        "score": 0,
                        "matching_skills": [],
                        "missing_skills": [],
                        "error": "Failed to parse LLM response"
                    }
                    
            except Exception as e:
                error_str = str(e)
                print(f"Error in resume matching (attempt {retry_count + 1}): {error_str}")
                
                # Check if it's a rate limit error
                if "429" in error_str:
                    if retry_count < self.max_retries - 1:  # If we still have retries left
                        delay = self._exponential_backoff(retry_count)
                        print(f"Rate limit hit. Waiting {delay:.2f} seconds before retry...")
                        time.sleep(delay)
                        retry_count += 1
                        continue
                    else:
                        print("Max retries reached for rate limit.")
                
                # For non-rate-limit errors or if max retries reached
                return {
                    "score": 0,
                    "matching_skills": [],
                    "missing_skills": [],
                    "error": error_str
                }
        
        # If we've exhausted all retries
        return {
            "score": 0,
            "matching_skills": [],
            "missing_skills": [],
            "error": "Max retries reached"
        }
