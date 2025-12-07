# Frontend – PySpark Code Explainer (Streamlit)

A minimal Streamlit UI for interacting with the FastAPI backend and generating PySpark explanations using Gemini.

---

## 🧩 Features
- Clean UI for PySpark code input
- Calls FastAPI backend
- Displays LLM-generated explanation
- Fully dockerized

---

## 📁 File Structure

```text
frontend/
├── app.py
├── Dockerfile
└── README.md
```

---

## ▶️ Running (Docker)

From root:

```bash
docker compose up --build
```

Then open:
```
http://localhost:8501
```

---

## ⚙️ Configuration

Backend URL (inside Docker network):

```python
BACKEND_URL = "http://backend:8005/explain/pyspark"
```

---

## 🛠 Tech
- Streamlit
- Python requests
- Docker

---

## 📈 Next Frontend Milestones
- Syntax highlighting  
- Line-by-line explanation mode  
- Model selector  
- Improved layout  
