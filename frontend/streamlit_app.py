import streamlit as st
import requests
import time

BACKEND_BASE = "http://backend:8000"  # internal when running in docker-compose

st.title("PySpark Code Explainer (LLM)")

code = st.text_area("PySpark code")

if st.button("Explain"):
    # 1. Submit job
    r = requests.post(
        f"{BACKEND_BASE}/explain/pyspark",
        json={"code": code},
        timeout=10,
    )

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

    # 2. Poll status
    with st.spinner("Waiting for result..."):
        while True:
            resp = requests.get(
                f"{BACKEND_BASE}/status/{job_id}",
                timeout=10,
            )
            try:
                j = resp.json()
            except Exception:
                st.error(f"Backend error ({resp.status_code}): {resp.text}")
                break

            status = j.get("status")

            if status in ("finished", "failed"):
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

                st.write(
                    f"Tokens: {llm.get('tokens_used')}, "
                    f"Total duration: {j.get('job_duration_ms')} ms, "
                    f"Model (Gemini) latency: {llm.get('latency_ms')} ms "
                )

                break 

            time.sleep(1)