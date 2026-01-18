import streamlit as st
import requests
import time
import os

BACKEND_BASE = os.getenv("BACKEND_URL", "http://backend:8000")

# Polling configuration
MAX_POLL_TIME = 120  # 2 minutes max
MAX_ATTEMPTS = 60
BASE_DELAY = 1.0
MAX_DELAY = 5.0

st.title("PySpark Intelligence Platform")

code = st.text_area("PySpark code")

if st.button("Explain"):
    # 1. Submit job
    try:
        r = requests.post(
            f"{BACKEND_BASE}/explain/pyspark",
            json={"code": code},
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to connect to backend: {e}")
        st.stop()

    if r.status_code != 200:
        try:
            data = r.json()
            st.error(f"Request failed: {data}")
        except Exception:
            st.error(f"Request failed. Status={r.status_code}, body={r.text}")
        st.stop()

    data = r.json()
    job_id = data["job_id"]

    st.info(f"Job queued: {job_id}")

    # 2. Poll status with timeout and exponential backoff
    with st.spinner("Waiting for result..."):
        start_time = time.time()
        attempt = 0
        delay = BASE_DELAY

        while attempt < MAX_ATTEMPTS:
            elapsed = time.time() - start_time
            if elapsed > MAX_POLL_TIME:
                st.error(
                    f"Request timed out after {MAX_POLL_TIME} seconds. Job ID: {job_id}"
                )
                st.info(
                    "The job may still be processing. Try refreshing with the same code later."
                )
                break

            try:
                resp = requests.get(
                    f"{BACKEND_BASE}/status/{job_id}",
                    timeout=10,
                )
                resp.raise_for_status()
                j = resp.json()
            except requests.exceptions.RequestException as e:
                st.warning(f"Connection error (attempt {attempt + 1}): {e}")
                attempt += 1
                time.sleep(delay)
                delay = min(delay * 1.5, MAX_DELAY)
                continue
            except Exception as e:
                st.error(f"Unexpected error: {e}")
                break

            status = j.get("status")

            if status == "finished":
                result = j.get("result")

                if not result:
                    st.error("No result returned.")
                    break

                llm = result.get("llm", {})
                analysis = result.get("analysis", {})

                # --- LLM ---
                st.subheader("Explanation")
                st.write(llm.get("explanation"))

                st.caption(
                    f"Tokens: {llm.get('tokens_used')} | "
                    f"Model latency: {llm.get('latency_ms')} ms | "
                    f"Job duration: {j.get('job_duration_ms')} ms"
                )

                # --- DAG ---
                st.subheader("Operation DAG")
                if "dag_dot" in analysis:
                    st.graphviz_chart(analysis["dag_dot"])

                # --- Lineage ---
                st.subheader("Data Lineage")
                if "lineage_dot" in analysis:
                    st.graphviz_chart(analysis["lineage_dot"])

                # --- Stage Summary ---
                st.subheader("Stage Summary")
                stage_summary = analysis.get("stage_summary")
                if stage_summary:
                    st.markdown(stage_summary.get("markdown", ""))
                else:
                    st.info("No stage summary available.")

                # --- Anti-patterns ---
                st.subheader("Anti-Patterns")
                antipatterns = analysis.get("antipatterns")
                if antipatterns:
                    st.markdown(antipatterns.get("markdown", ""))
                else:
                    st.info("No anti-patterns detected.")

                break

            elif status == "failed":
                result = j.get("result", {})
                error = result.get("error", {})
                st.error(
                    f"Job failed: {error.get('type', 'Unknown')}: {error.get('message', 'No details')}"
                )
                break

            elif status in ("pending", "running", "analysis_complete"):
                attempt += 1
                time.sleep(delay)
                # Reset delay on successful response
                delay = BASE_DELAY

            else:
                st.warning(f"Unknown status: {status}")
                break
