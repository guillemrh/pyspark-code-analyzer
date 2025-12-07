import streamlit as st
import requests

st.title("PySpark Code Explainer (LLM)")

code_input = st.text_area("Paste your PySpark code below:")

if st.button("Explain Code"):
    if not code_input.strip():
        st.warning("Please enter some PySpark code.")
    else:
        response = requests.post(
            "http://backend:8000/explain/pyspark",
            json={"code": code_input}
        )
        if response.status_code == 200:
            st.subheader("Explanation:")
            st.write(response.json()["explanation"])
        else:
            st.error(f"Error: {response.json().get('detail', 'Unknown error')}")