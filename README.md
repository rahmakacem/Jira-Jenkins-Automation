# PrivateGPT for Jenkins

**An AI-powered chatbot for Jenkins: ask questions, get answers from your own pipeline data.**

It works like a log intelligence tool, so you don't have to dig through multiple dashboards when something breaks.

---

## Quick Start

### Option 1: Local (5 minutes)

```bash
cd jenkins-workflow-chatbot
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # Add your Groq API key
uvicorn app.main:app --reload --port 8000
```

Open http://localhost:8000 — works immediately in demo mode, no Jenkins needed.

### Option 2: With Live Jenkins

```bash
docker run -p 8080:8080 jenkins/jenkins:lts

# Get your API token: Jenkins > User > Configure > API Token
# Add to .env: JENKINS_URL, JENKINS_USER, JENKINS_TOKEN

chmod +x scripts/create_test_jobs.sh
JENKINS_USER=admin JENKINS_TOKEN=your_token ./scripts/create_test_jobs.sh

uvicorn app.main:app --reload --port 8000
```

### Option 3: Deploy to Cloud

**Render (free):** Push to GitHub, connect repo at render.com, set `GROQ_API_KEY`, deploy.

**Docker:**
```bash
docker build -t jenkins-chatbot .
docker run -p 8000:8000 -e GROQ_API_KEY=your_key jenkins-chatbot
```

---

## Demo Queries

| Query | What happens |
|-------|--------------|
| "Why did build #100 fail?" | Retrieves the relevant build data and explains the root cause |
| "Show me build failure trends" | Summarizes failures over time from your pipeline history |
| "Which jobs are most unstable?" | Ranks jobs by failure rate |
| "Build failure trends in a bar chart" | Renders the trends as a bar chart in the chat |

---

## How it works

```
Jenkins  →  ETL Agent  →  OpenSearch (Vector DB + embeddings)  →  Groq AI Chatbot
```

1. **Jenkins:** source of build and job data, collected through the Jenkins REST API.
2. **ETL Agent:** collects and processes build data automatically **every 30 minutes**.
3. **OpenSearch:** stores the data with semantic embeddings, acting as a vector database with **hybrid search** (BM25 keyword matching + KNN vector search).
4. **Groq AI Chatbot:** retrieves the most relevant context and generates the answer with Llama 3.1 70B, shown in a chat interface.

---

## Key Features

### Grounded answers (RAG)
This is a Retrieval-Augmented Generation system. It answers strictly from your actual CI/CD data and CloudWatch logs, so the model doesn't invent answers.

### Hybrid search
Keyword matching (BM25) is blended with semantic similarity (KNN vectors over sentence-transformer embeddings), so the right context is pulled for every question.

### Automated data collection
The ETL agent refreshes build data every 30 minutes, so answers reflect your recent builds without manual exports.

### Charts in the chat
Ask for trends and get them as a chart, for example a bar chart of build failures.

---

## Tech Stack

Built with open source in mind.

| Component | Role |
|---|---|
| **Groq API (Llama 3.1 70B)** | Fast LLM inference |
| **OpenSearch** | Vector DB with hybrid search (BM25 + KNN vectors) |
| **Sentence-Transformers** | Semantic embeddings |
| **Streamlit** | Conversational UI |
| **Jenkins REST API** | Source of build data |

---

## Project Structure

```
jenkins-workflow-chatbot/
|-- app/
|   |-- main.py                       # FastAPI app with RAG + demo mode
|   |-- agents/
|   |   |-- router.py                 # LLM intent classifier
|   |   |-- troubleshoot.py           # Build failure diagnosis
|   |   |-- workflow.py               # Step-by-step guidance
|   |   |-- recommend.py              # Plugin recommendations
|   |-- rag/
|   |   |-- engine.py                 # FAISS + sentence-transformers
|   |-- demo/
|   |   |-- mock_data.py              # Realistic mock build logs
|   |-- data/jenkins_docs/
|   |   |-- corpus.json               # 20 Jenkins doc chunks
|   |-- utils/
|       |-- jenkins_context.py        # Live Jenkins state reader
|-- frontend/
|   |-- index.html                    # Chat UI (markdown, syntax highlight, RAG citations)
|-- scripts/
|   |-- create_test_jobs.sh           # Creates demo jobs in Jenkins
|   |-- test_endpoints.sh             # API smoke tests
|-- tests/
|-- Dockerfile                        # Cloud deployment
|-- render.yaml                       # Render.com config
|-- railway.toml                      # Railway config
|-- Procfile                          # Heroku/Railway
|-- requirements.txt
|-- .env.example
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Frontend UI |
| GET | `/api/health` | Health check + RAG status |
| GET | `/api/jenkins/info` | Jenkins server state (live or demo) |
| GET | `/api/jenkins/failed` | All recent build failures |
| GET | `/api/jenkins/jobs/{name}` | Job details |
| GET | `/api/rag/search?q=query` | Test RAG retrieval directly |
| POST | `/api/chat` | Main chat endpoint (JSON) |
| POST | `/api/chat/stream` | SSE streaming chat |
| GET | `/docs` | Auto-generated OpenAPI docs |

---

## Author

**Rahma Kacem**, Software Engineer
rahmakacem16@gmail.com
