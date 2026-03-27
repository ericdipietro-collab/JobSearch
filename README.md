# 💼 Job Search Healer: Tiered Funnel Scraper

A high-precision job search automation tool designed for professionals in high-stakes industries like **Financial Services** and **FinTech**. 

## 🚀 Key Features
* **Tiered Funnel Scoring**: Moves beyond simple keyword matching to score jobs based on Title alignment first, then deep Job Description (JD) analysis.
* **Anti-Double-Dipping Logic**: Uses a 0.5x multiplier on JD keywords if a title is already a "Fast-Track" match to prevent score inflation.
* **Hard Gating**: Automatically filters out roles based on location policy (Remote/Hybrid) and salary floors with a 5% negotiation buffer.
* **Transparency Logs**: Generates a "decision receipt" for every job, explaining exactly why a role was kept or filtered.

## 🛠️ Setup
1. Clone the repo: `git clone <your-repo-url>`
2. Install dependencies: `pip install -r requirements.txt`
3. Configure your targets in `config/job_search_companies.yaml`.
4. Set your salary and keyword preferences in `config/job_search_preferences.yaml`.

## 🖥️ Dashboard
Run the interactive Streamlit UI to audit results:
`streamlit run app.py`