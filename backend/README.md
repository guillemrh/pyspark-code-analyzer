# Backend – PySpark Code Explainer (FastAPI)

This service exposes a clean API endpoint that accepts PySpark code, forwards it to a Gemini model, and returns a structured explanation.

---

## 📌 Features
- FastAPI server (`/explain/pyspark`)
- Gemini 1.5 Flash/Pro integration
- Pydantic validation
- Clean modular structure
- Fully dockerized

---

## 📁 File Structure

```text
backend/
├── app/
│   ├── main.py        # App entrypoint
│   ├── routes.py      # API routes
│   ├── llm.py         # Gemini client
│   └── schemas.py     # Pydantic models
├── requirements.txt
├── Dockerfile
└── README.md
```

---

## ▶️ Running (Docker)

From project root:

```bash
docker compose up --build
```

Backend runs internally at:  
`http://backend:8005`

(External port depends on your docker-compose mapping.)

---

## 🔐 Environment Variables

Create `.env` in `/backend`:

```bash
GEMINI_API_KEY=your_key_here
```

---

## 📡 API Endpoint

### POST `/explain/pyspark`

**Request**
```json
{
  "code": "df = spark.read.csv('data.csv')"
}
```

**Response**
```json
{
  "explanation": "This code reads a CSV file into a Spark DataFrame..."
}
```

---

## 🛠 Tech
- FastAPI  
- Pydantic  
- Google Generative AI SDK  
- Uvicorn  

---

## 📈 Next Backend Milestones
- Redis caching  
- Queue worker  
- DAG extraction  
- Error-type system  
