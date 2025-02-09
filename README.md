# Job Search Pipeline

Automated job search pipeline that:
- Fetches job listings from various job boards
- Matches jobs against your resume using AI
- Stores results in a CSV file with deduplication

## Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create a `.env` file with your API keys:
   ```
   GOOGLE_API_KEY=your_key_here
   ADZUNA_APP_ID=your_id_here
   ADZUNA_APP_KEY=your_key_here
   ```

## Usage

Update the `config` dictionary in `main.py` with your desired settings.
Run the pipeline: