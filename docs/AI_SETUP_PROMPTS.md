# AI Setup Prompts

These prompts help you configure the Job Search tool using AI assistants like **Claude**, **ChatGPT**, or **Gemini**. Copy a prompt, fill in the bracketed parts, and paste it into your AI of choice. The AI will generate valid YAML config files ready to drop into the `config/` folder.

---

## Prompt 1 — Generate your preferences file from your resume

Use this to create `config/job_search_preferences.yaml`. This is the most important config file — it sets your salary floor, location, and the keyword scoring engine that decides which jobs are a good fit.

**How to use:**
1. Copy the entire prompt below
2. Replace `[PASTE YOUR RESUME HERE]` with the text of your resume
3. Fill in `[YOUR ZIP CODE]`, `[YOUR NEAREST CITY]`, and `[YOUR TARGET SALARY]`
4. Paste into Claude, ChatGPT, or Gemini
5. Save the output as `config/job_search_preferences.yaml`

---

```
I'm setting up a job search automation tool that uses a YAML config file to score and filter job listings. Please generate a complete, valid YAML file for my job search preferences based on my resume and the instructions below.

MY RESUME:
[PASTE YOUR RESUME HERE]

MY PERSONAL DETAILS (fill these in):
- My zip code: [YOUR ZIP CODE]
- City/cities within commuting distance (30 miles): [YOUR NEAREST CITY, NEARBY CITY 2]
- My minimum acceptable salary: $[YOUR TARGET SALARY]
- Location preference: [remote only / remote or hybrid / open to anything]

YAML FILE TEMPLATE (generate output in this exact format):

version: 1
search:
  location_policy: remote_only    # remote_only | remote_or_hybrid | any
  geography:
    us_only: true
  location_preferences:
    remote_us:
      enabled: true
      bonus: 14
    local_hybrid:
      enabled: true
      primary_zip: 'FILL_IN'
      radius_miles: 30
      markers:
      - FILL_IN
      bonus: 4
      allow_if_salary_at_least_usd: FILL_IN
  compensation:
    enforce_min_salary: true
    target_salary_usd: FILL_IN
    min_salary_usd: FILL_IN
    preferred_remote_min_salary_usd: FILL_IN
    allow_missing_salary: true
    salary_floor_basis: midpoint
    negotiation_buffer_pct: 0.05
  recency:
    enforce_job_age: true
    max_job_age_days: 21

scoring:
  minimum_score_to_keep: 35
  apply_now:
    require_strong_title: true
    min_role_alignment: 6.0
    direct_title_markers:
    - FILL_IN_WITH_3_TO_8_KEY_TITLE_WORDS
  adjustments:
    missing_salary_penalty: 6
    salary_at_or_above_target_bonus: 6
    salary_meets_floor_bonus: 2
    salary_below_target_penalty: 12
  keyword_matching:
    count_unique_matches_only: true
    positive_keyword_cap: 40
    negative_keyword_cap: 45
  action_buckets:
    ranks:
      APPLY NOW: 0
      REVIEW TODAY: 1
      WATCH: 2
      MANUAL REVIEW: 3
      IGNORE: 4
    rules:
    - label: MANUAL REVIEW
      when:
        manual_review: true
    - label: APPLY NOW
      when:
        min_score: 88
        eligible: true
        strong_title: true
        known_salary_for_apply: true
    - label: REVIEW TODAY
      when:
        min_score: 88
        eligible: true
        strong_title: true
        known_salary_for_apply: false
    - label: REVIEW TODAY
      when:
        min_score: 74
        eligible: true
    - label: REVIEW TODAY
      when:
        tier_in: [1]
        min_score: 67
        eligible: true
    - label: WATCH
      when:
        min_score: 55
        eligible: true
    - label: WATCH
      when:
        min_score: 40
    - label: IGNORE
      when: {}

titles:
  require_one_positive_keyword: true
  positive_keywords:
  - FILL_IN_WITH_JOB_TITLES_I_AM_TARGETING
  negative_disqualifiers:
  - FILL_IN_WITH_TITLES_I_SHOULD_NEVER_SEE
  positive_weights:
    FILL_IN_TITLE: WEIGHT_5_TO_10
  fast_track_base_score: 50
  fast_track_min_weight: 8

keywords:
  body_positive:
    FILL_IN_DOMAIN_KEYWORD: WEIGHT_3_TO_10
  body_negative:
    FILL_IN_BAD_FIT_KEYWORD: PENALTY_15_TO_30

policy:
  title_rescue:
    adjacent_title_bonus: 8
    adjacent_title_strong_domain_bonus: 6
    adjacent_title_min_score_to_keep: 26
    strong_body_domain_markers:
    - FILL_IN
    adjacent_title_markers:
    - FILL_IN

INSTRUCTIONS FOR GENERATING THE OUTPUT:

1. titles.positive_keywords — Extract 8–15 job titles from my resume that I have held or am targeting. Include variations (e.g., "Senior Product Manager", "Product Lead", "Director of Product").

2. titles.negative_disqualifiers — List 15–25 job titles that would never be a fit for me based on my resume (e.g., if I'm in product management, add "software engineer", "data scientist", "recruiter", "sales", "marketing manager", etc.)

3. titles.positive_weights — Assign weights 4–10 to my top target titles. Weight 8–10 = my ideal role. Weight 4–6 = related/adjacent roles.

4. keywords.body_positive — Extract 15–25 domain keywords from my resume that would appear in job descriptions for roles I'd be good at. Score each 3–10 based on how strongly they signal fit. Higher = more specific to my expertise.

5. keywords.body_negative — List 15–25 keywords that indicate a poor fit (deep technical skills I don't have, functions I'm not targeting). Score each 15–30. Higher = stronger disqualifier.

6. scoring.apply_now.direct_title_markers — 5–8 short title keywords that, when present in a job title, indicate a strong match (e.g., "architect", "product manager", "platform").

7. Fill in all FILL_IN placeholders with appropriate values from my resume and the personal details I provided above.

Output ONLY the YAML file with no additional explanation. Ensure the output is valid YAML (2-space indentation, no tabs, strings with special characters in quotes).
```

---

## Prompt 2 — Build your company target list

Use this to generate `config/job_search_companies.yaml`. Companies are the source of your job listings — the more you have, the more results you'll see.

**How to use:**
1. Copy the prompt below
2. Fill in your role and industry
3. Optionally add companies you already know you want to target
4. Paste into Claude, ChatGPT, or Gemini
5. Add the output to `config/job_search_companies.yaml`

---

```
I'm building a list of companies to monitor for job openings in my job search. Please generate a YAML list of companies I should target, formatted for my job search tool.

MY PROFILE:
- Target role(s): [e.g., Product Manager, Data Engineer, Marketing Manager]
- Industry/domain: [e.g., FinTech, Healthcare Tech, E-commerce, SaaS, Consulting]
- Location preference: [Remote / Denver CO area / New York / etc.]
- Company size preference: [Startup / Mid-size / Enterprise / Any]
- Companies I definitely want included: [List any specific companies, or leave blank]
- Companies to exclude: [Any companies to skip, or leave blank]

OUTPUT FORMAT — generate 20–40 companies as a YAML list in this exact format:

- name: Company Name
  domain: companydomain.com
  adapter: greenhouse        # greenhouse | lever | ashby | workday | custom_manual
  adapter_key: companyslug   # the slug used in their ATS URL
  careers_url: https://boards.greenhouse.io/companyslug
  tier: 1                    # 1=top priority, 2=standard, 3=opportunistic
  industry: [fintech]        # one or more tags
  status: new

ADAPTER GUIDE (use this to pick the right adapter):
- greenhouse: careers page URL contains "boards.greenhouse.io" or "greenhouse.io"
- lever:       careers page URL contains "jobs.lever.co"
- ashby:       careers page URL contains "jobs.ashbyhq.com"
- workday:     careers page URL contains "myworkdayjobs.com"
- custom_manual: anything else — use the company's own /careers page as careers_url

TIER GUIDE:
- tier 1: companies where I'd be excited to work, strong culture fit, top priorities
- tier 2: good companies, solid opportunities, standard priority
- tier 3: opportunistic — worth monitoring but not actively pursuing

INSTRUCTIONS:
1. Research each company's actual careers page URL to determine the correct adapter and adapter_key
2. Set adapter_key to the slug from their ATS URL (e.g., for "boards.greenhouse.io/stripe" the key is "stripe")
3. Assign tiers based on how well the company matches my profile
4. If you're unsure about a company's ATS, use adapter: custom_manual and set careers_url to their /careers page
5. Focus on companies known to hire for [TARGET ROLE] roles
6. Include a mix of tiers — don't make everything tier 1

Output ONLY the YAML list, starting with the first "- name:" entry. No preamble or explanation.
```

---

## Prompt 3 — Refine and tune your keyword weights

Use this after you've run a few scraper runs and want to improve which jobs get surfaced. Paste in 3–5 job descriptions of roles you liked and 3–5 you didn't.

---

```
I'm tuning the keyword scoring engine for my job search tool. I'll give you examples of good and bad job matches, and I need you to suggest updated keyword weights for my YAML config.

MY CURRENT KEYWORDS (from my preferences YAML):
[PASTE YOUR CURRENT keywords.body_positive AND keywords.body_negative SECTIONS]

JOBS I LIKED (paste 2–5 job descriptions or titles + key requirements):
[PASTE JD TEXT OR BULLET POINTS]

JOBS I DID NOT LIKE (paste 2–5 job descriptions or titles + key requirements):
[PASTE JD TEXT OR BULLET POINTS]

Please output:
1. An updated keywords.body_positive section — add any keywords I missed, adjust weights up/down based on the examples, remove anything that doesn't help differentiate
2. An updated keywords.body_negative section — add any new disqualifying signals you see in the bad examples

Rules:
- Positive weights: 3–10. Higher = stronger signal. Use 8–10 only for very domain-specific terms.
- Negative penalties: 15–30. Higher = stronger disqualifier. Use 25–30 for must-never-match terms.
- Keep the output as valid YAML in the same format as my current config
- Explain each significant change you make (1 line per change)

Output the updated YAML sections first, then the explanations.
```

---

## Prompt 4 — Generate a starter list for a specific industry

If you're in a field not well-represented in the starter company list, use this to build a targeted list fast.

---

```
Generate a YAML list of 30 companies that actively hire [YOUR ROLE] professionals in [YOUR INDUSTRY / CITY OR REMOTE].

Format each entry exactly like this:

- name: Company Name
  domain: companydomain.com
  adapter: greenhouse
  adapter_key: companyslug
  careers_url: https://boards.greenhouse.io/companyslug
  tier: 2
  industry: [your-industry]
  status: new

Mix of tiers (roughly 5 tier-1, 20 tier-2, 5 tier-3). Use adapter: custom_manual and the company's /careers URL if you don't know their ATS. Output ONLY the YAML, no explanation.
```

---

## Tips for using these prompts

**Getting better results:**
- The more detail you give about your background in Prompt 1, the better the keywords will be
- Run the tool for a week, then look at the "Filtered Out" tab to see what's getting rejected — paste those job titles into Prompt 3 to improve filtering
- If you're seeing too many irrelevant jobs, increase the `negative_keyword_cap` in your preferences file and add stronger penalties

**Validating YAML output:**
- Paste the generated YAML into [yamllint.com](https://www.yamllint.com/) before saving it
- YAML is whitespace-sensitive — make sure the indentation is consistent (2 spaces, no tabs)

**After generating your company list:**
- Open the dashboard and go to **Target Companies**
- Click **Run ATS Healer** to automatically validate and fix any broken URLs in your list
- Some companies may need manual URL fixes — the **Bulk URL Fix** tab makes this fast

**Keeping your list fresh:**
- Re-run the healer every few weeks — companies change their ATS providers
- Add new companies from your networking and job fair contacts directly in the **Target Companies** UI
