# Jira-Jenkins Test Execution Automation for HIL Testing

An automation service for **Hardware-in-the-Loop (HIL) testing**. It watches Jira for new *Test Execution* tickets, reserves a free test node, triggers Jenkins to flash the right software and run the tests, and writes the results back to Jira. Engineers only review the build results.

## What it does

- **Ticket automation:** polls the Jira REST API every 2 minutes for new Test Execution tickets, extracts software versions and test parameters, and groups tickets by software prefix.
- **Concurrency:** processes groups in parallel threads with thread-safe node reservation (locks and atomic check-and-set), so the same HIL test node is never flashed twice at the same time.
- **Resource allocation:** a tiered node selection algorithm uses live node status from a node scheduling platform to minimize reflashing time:
  1. exact software match
  2. prefix-compatible node
  3. any available execution node
  4. fallback (no node available, the group waits for the next cycle)
- **CI/CD integration:** triggers Jenkins flashing and execution jobs through webhooks, polls the build status through the REST API, and updates Jira tickets with build results and links.
- **Error recovery:** automatic Jira status transitions, email failure notifications, no retry loops for failed software groups, and periodic cleanup of stale node reservations.

## How it works

```
Jira (Test Execution tickets)
        |
        v
  Group by software prefix  ->  Select and reserve a free node
        |
        v
  Jenkins: flash software (skipped on exact match)  ->  Jenkins: run tests
        |
        v
  Jira: comment with build link, move ticket to Done or Waiting for feedback
```

## Project structure

```
jira-jenkins-hil-automation/
|-- jira_jenkins_automation.py   # Main script
|-- sw_profiles.example.json     # Example mapping of SW prefix to HW/job settings
|-- .env.example                 # Example configuration (copy to .env)
|-- requirements.txt
|-- .gitignore
```

## Getting started

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env                       # fill in your Jira, Jenkins and scheduler settings
cp sw_profiles.example.json sw_profiles.json   # adapt to your software versions

python jira_jenkins_automation.py
```

## Configuration

All credentials, URLs and environment-specific values are read from environment variables (or a `.env` file). Nothing sensitive is stored in the code. See `.env.example` for the full list.

| Setting | Purpose |
|---|---|
| `JIRA_URL`, `JIRA_TOKEN` | Jira server and access token |
| `JIRA_FIELD_*` | Custom field IDs used to read ticket data |
| `JIRA_TRANSITION_*` | Workflow transition IDs |
| `JENKINS_URL`, `JENKINS_USERNAME`, `JENKINS_API_TOKEN` | Jenkins access |
| `JENKINS_FLASHING_TOKEN`, `JENKINS_EXECUTION_TOKEN` | Webhook trigger tokens |
| `BTS_SCHEDULER_URL`, `DUT_REQUEST_URL` | Node scheduling platform endpoints |
| `EMAIL_USER`, `EMAIL_PASS`, `NOTIFICATION_EMAIL` | Failure notifications (optional) |

`sw_profiles.json` maps a software prefix (for example `"20."`) to the hardware version and the job parameters used when flashing.

The Jenkins job parameter names in the payloads (for example `NODE_TO_RUN`, `KEYWORD_VERSION`, `DATABASE_PATH`) are generic. Adjust them in the script to match your own jobs.

## Security

Never commit `.env` or `sw_profiles.json`. Both are listed in `.gitignore`.

## Author

**Rahma Kacem**, Software Engineer
rahmakacem16@gmail.com
