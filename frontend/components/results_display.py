"""Results display component with tabs for different analysis views."""

import streamlit as st

from components.graph_viewer import render_dag_graph, render_lineage_graph


def render_results(result: dict) -> None:
    """
    Render the analysis results in a tabbed interface.

    Args:
        result: The result dictionary from the API containing llm and analysis data
    """
    if not result:
        st.info("Submit PySpark code to see analysis results")
        return

    llm_result = result.get("llm", {})
    analysis = result.get("analysis", {})

    # Create tabs
    tab_explanation, tab_dag, tab_lineage, tab_stages, tab_antipatterns = st.tabs(
        ["Explanation", "Operation DAG", "Data Lineage", "Stages", "Anti-Patterns"]
    )

    # Tab 1: LLM Explanation
    with tab_explanation:
        render_explanation_tab(llm_result)

    # Tab 2: Operation DAG
    with tab_dag:
        render_dag_tab(analysis)

    # Tab 3: Data Lineage
    with tab_lineage:
        render_lineage_tab(analysis)

    # Tab 4: Stage Summary
    with tab_stages:
        render_stages_tab(analysis)

    # Tab 5: Anti-Patterns
    with tab_antipatterns:
        render_antipatterns_tab(analysis)


def render_explanation_tab(llm_result: dict) -> None:
    """Render the LLM explanation tab."""
    explanation = llm_result.get("explanation", "")

    if explanation:
        st.markdown(explanation)

        # Metrics
        st.divider()
        col1, col2, col3 = st.columns(3)
        with col1:
            tokens = llm_result.get("tokens_used", "N/A")
            st.metric("Tokens Used", tokens)
        with col2:
            latency = llm_result.get("latency_ms", "N/A")
            if latency != "N/A":
                latency = f"{latency:.0f} ms"
            st.metric("Model Latency", latency)
        with col3:
            model = llm_result.get("model", "N/A")
            st.metric("Model", model)
    else:
        st.info("No explanation available")


def render_dag_tab(analysis: dict) -> None:
    """Render the Operation DAG tab."""
    dag_dot = analysis.get("dag_dot", "")

    if dag_dot:
        render_dag_graph(dag_dot)

        # Show raw DOT in expander
        with st.expander("View DOT Source"):
            st.code(dag_dot, language="dot")
    else:
        st.info("No DAG data available")


def render_lineage_tab(analysis: dict) -> None:
    """Render the Data Lineage tab."""
    lineage_dot = analysis.get("lineage_dot", "")

    if lineage_dot:
        render_lineage_graph(lineage_dot)

        # Show raw DOT in expander
        with st.expander("View DOT Source"):
            st.code(lineage_dot, language="dot")
    else:
        st.info("No lineage data available")


def render_stages_tab(analysis: dict) -> None:
    """Render the Stage Summary tab."""
    stage_summary = analysis.get("stage_summary", {})

    if stage_summary:
        markdown = stage_summary.get("markdown", "")
        if markdown:
            st.markdown(markdown)
        else:
            st.info("No stage summary markdown available")

        # Show JSON data in expander
        json_data = stage_summary.get("json")
        if json_data:
            with st.expander("View Raw Data"):
                st.json(json_data)
    else:
        st.info("No stage summary available")


def render_antipatterns_tab(analysis: dict) -> None:
    """Render the Anti-Patterns tab."""
    antipatterns = analysis.get("antipatterns", {})

    if not antipatterns:
        st.success("No anti-patterns detected! Your code looks good.")
        return

    markdown = antipatterns.get("markdown", "")
    json_data = antipatterns.get("json", {})

    # Get total issues from JSON structure
    total_issues = json_data.get("total_issues", 0)
    by_rule = json_data.get("by_rule", {})

    if total_issues == 0 and not markdown:
        st.success("No anti-patterns detected! Your code looks good.")
        return

    # Summary
    st.markdown(f"**Found {total_issues} potential issue(s)**")

    # Build table data
    if by_rule:
        table_data = []
        for rule_id, issues in by_rule.items():
            for issue in issues:
                severity = issue.get("severity", "info").upper()
                message = issue.get("message", "No details")
                nodes = ", ".join(issue.get("nodes", []))
                table_data.append({
                    "Severity": severity,
                    "Rule": rule_id,
                    "Message": message,
                    "Affected Nodes": nodes,
                })

        st.table(table_data)
    elif markdown:
        # Fallback to markdown display
        st.markdown(markdown)


def render_error(error: dict, job_id: str | None = None) -> None:
    """Render an error message."""
    error_type = error.get("type", "Unknown Error")
    error_message = error.get("message", "An unknown error occurred")

    st.error(f"**{error_type}:** {error_message}")

    if job_id:
        st.caption(f"Job ID: {job_id}")

    # Show details if available
    if error.get("details"):
        with st.expander("Error Details"):
            st.code(error["details"])
