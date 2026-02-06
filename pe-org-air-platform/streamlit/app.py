import json
import os
from typing import Any, Dict

import requests
import streamlit as st

# ============================================================
# Config
# ============================================================

DEFAULT_API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
DEFAULT_API_PREFIX = os.getenv("API_PREFIX", "/api/v1")

ASSESSMENT_TYPES = ["screening", "due_diligence", "quarterly", "exit_prep"]
ASSESSMENT_STATUSES = ["draft", "in_progress", "submitted", "approved", "superseded"]
DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_portfolio",
    "culture_change",
]

DEFAULT_FILING_TYPES = ["10-K", "10-Q", "8-K"]

# ============================================================
# HTTP helpers
# ============================================================


def _join_url(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")


def _api_url(base: str, prefix: str, path: str, include_prefix: bool = True) -> str:
    if include_prefix:
        return _join_url(base, _join_url(prefix, path))
    return _join_url(base, path)


def _request(method: str, url: str, **kwargs: Any) -> requests.Response:
    timeout = kwargs.pop("timeout", 10)
    return requests.request(method, url, timeout=timeout, **kwargs)


def _request_json(method: str, url: str, **kwargs: Any) -> Any:
    resp = _request(method, url, **kwargs)
    if not resp.ok:
        raise requests.HTTPError(resp.text, response=resp)
    if resp.status_code == 204 or not resp.text.strip():
        return None
    return resp.json()


def _show_http_error(exc: requests.HTTPError) -> None:
    resp = exc.response
    if resp is None:
        st.error(f"Request failed: {exc}")
        return
    st.error(f"Request failed: {resp.status_code}")
    if resp.text:
        try:
            st.json(resp.json())
        except ValueError:
            st.code(resp.text)


def _show_response(resp: requests.Response) -> None:
    st.write(f"Status: {resp.status_code}")
    if resp.text:
        try:
            st.json(resp.json())
        except ValueError:
            st.code(resp.text)
    else:
        st.info("No content")


def _json_editor(label: str, value: dict[str, Any]) -> dict[str, Any]:
    text = st.text_area(label, value=json.dumps(value, indent=2))
    try:
        return json.loads(text) if text.strip() else {}
    except json.JSONDecodeError:
        st.error("Invalid JSON")
        return value


def _pick_date(val: Any) -> str:
    try:
        return val.isoformat()
    except Exception:
        return str(val)


# ============================================================
# UI
# ============================================================

st.set_page_config(page_title="PE OrgAIR Platform", layout="wide")
st.title("PE OrgAIR Platform")

with st.sidebar:
    st.header("API Settings")
    api_base = st.text_input("API Base URL", value=DEFAULT_API_BASE)
    api_prefix = st.text_input("API Prefix", value=DEFAULT_API_PREFIX)
    timeout = st.number_input("Timeout (seconds)", min_value=1, max_value=120, value=10)

    st.divider()
    st.caption("Notes")
    st.write("- Health routes are not prefixed")
    st.write("- Everything else uses API prefix (default /api/v1)")

st.divider()

# ============================================================
# Health
# ============================================================

health_col, info_col = st.columns([2, 3])
with health_col:
    st.subheader("Health")
    if st.button("GET /health"):
        try:
            url = _api_url(api_base, api_prefix, "/health", include_prefix=False)
            data = _request_json("GET", url, timeout=timeout)
            st.success("OK")
            st.json(data)
        except requests.HTTPError as exc:
            _show_http_error(exc)
        except Exception as exc:
            st.error(f"Health check failed: {exc}")

    if st.button("GET /health/detailed"):
        try:
            url = _api_url(api_base, api_prefix, "/health/detailed", include_prefix=False)
            data = _request_json("GET", url, timeout=timeout)
            st.success("OK")
            st.json(data)
        except requests.HTTPError as exc:
            _show_http_error(exc)
        except Exception as exc:
            st.error(f"Detailed health check failed: {exc}")

with info_col:
    st.subheader("Quick Notes")
    st.write("Use the tabs below for Case Study 1 (Companies/Assessments) and Case Study 2 (Evidence/Signals).")

st.divider()

# ============================================================
# Main tabs: CS1 + CS2
# ============================================================

main_tabs = st.tabs(["Companies", "Assessments", "Documents", "Signals", "Evidence"])

# ============================================================
# CS1: Companies
# ============================================================

with main_tabs[0]:
    st.subheader("Companies")
    # FIXED: tab labels now match content
    company_tabs = st.tabs(["List", "Get", "Create", "Update", "Delete"])

    with company_tabs[0]:
        page = st.number_input("Page", min_value=1, value=1, key="companies_list_page")
        page_size = st.number_input(
            "Page Size", min_value=1, max_value=100, value=20, key="companies_list_size"
        )
        if st.button("GET /companies"):
            try:
                url = _api_url(api_base, api_prefix, "/companies")
                data = _request_json(
                    "GET",
                    url,
                    params={"page": int(page), "page_size": int(page_size)},
                    timeout=timeout,
                )
                st.write(f"Total: {data.get('total', 'n/a')}")
                st.dataframe(data.get("items", []), use_container_width=True)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List companies failed: {exc}")

    with company_tabs[1]:
        company_id = st.text_input("Company ID", key="company_get_id")
        if st.button("GET /companies/{id}"):
            if not company_id.strip():
                st.error("Company ID is required.")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/companies/{company_id.strip()}")
                    data = _request_json("GET", url, timeout=timeout)
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get company failed: {exc}")

    with company_tabs[2]:
        with st.form("company_create"):
            name = st.text_input("Name")
            ticker = st.text_input("Ticker (optional, uppercase)")
            industry_id = st.text_input("Industry ID (optional)")
            position_factor = st.number_input(
                "Position Factor", min_value=-1.0, max_value=1.0, value=0.0, step=0.01
            )
            submitted = st.form_submit_button("POST /companies")

        if submitted:
            if not name.strip():
                st.error("Name is required.")
            else:
                payload = {"name": name.strip(), "position_factor": float(position_factor)}
                if ticker.strip():
                    payload["ticker"] = ticker.strip()
                if industry_id.strip():
                    payload["industry_id"] = industry_id.strip()
                try:
                    url = _api_url(api_base, api_prefix, "/companies")
                    data = _request_json("POST", url, json=payload, timeout=timeout)
                    st.success("Company created")
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Create company failed: {exc}")

    with company_tabs[3]:
        with st.form("company_update"):
            update_id = st.text_input("Company ID")
            update_name = st.text_input("Name (optional)")
            update_ticker = st.text_input("Ticker (optional, uppercase)")
            update_industry_id = st.text_input("Industry ID (optional)")
            include_position = st.checkbox("Include position_factor", value=False)
            update_position = st.number_input(
                "Position Factor", min_value=-1.0, max_value=1.0, value=0.0, step=0.01
            )
            submitted_update = st.form_submit_button("PUT /companies/{id}")

        if submitted_update:
            if not update_id.strip():
                st.error("Company ID is required.")
            else:
                payload: dict[str, Any] = {}
                if update_name.strip():
                    payload["name"] = update_name.strip()
                if update_ticker.strip():
                    payload["ticker"] = update_ticker.strip()
                if update_industry_id.strip():
                    payload["industry_id"] = update_industry_id.strip()
                if include_position:
                    payload["position_factor"] = float(update_position)
                if not payload:
                    st.error("Provide at least one field to update.")
                else:
                    try:
                        url = _api_url(api_base, api_prefix, f"/companies/{update_id.strip()}")
                        data = _request_json("PUT", url, json=payload, timeout=timeout)
                        st.success("Company updated")
                        st.json(data)
                    except requests.HTTPError as exc:
                        _show_http_error(exc)
                    except Exception as exc:
                        st.error(f"Update company failed: {exc}")

    with company_tabs[4]:
        delete_id = st.text_input("Company ID", key="company_delete_id")
        if st.button("DELETE /companies/{id}"):
            if not delete_id.strip():
                st.error("Company ID is required.")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/companies/{delete_id.strip()}")
                    resp = _request("DELETE", url, timeout=timeout)
                    if not resp.ok:
                        raise requests.HTTPError(resp.text, response=resp)
                    st.success("Company deleted")
                    _show_response(resp)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Delete company failed: {exc}")

# ============================================================
# CS1: Assessments
# ============================================================

with main_tabs[1]:
    st.subheader("Assessments")
    # FIXED: tab labels now match content
    assessment_tabs = st.tabs(["List", "Get", "Create", "Update Status", "Scores"])

    with assessment_tabs[0]:
        a_page = st.number_input("Page", min_value=1, value=1, key="assess_list_page")
        a_page_size = st.number_input(
            "Page Size", min_value=1, max_value=100, value=20, key="assess_list_size"
        )
        a_company_id = st.text_input("Company ID filter (optional)", key="assess_list_company")
        if st.button("GET /assessments"):
            try:
                url = _api_url(api_base, api_prefix, "/assessments")
                params = {"page": int(a_page), "page_size": int(a_page_size)}
                if a_company_id.strip():
                    params["company_id"] = a_company_id.strip()
                data = _request_json("GET", url, params=params, timeout=timeout)
                st.write(f"Total: {data.get('total', 'n/a')}")
                st.dataframe(data.get("items", []), use_container_width=True)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List assessments failed: {exc}")

    with assessment_tabs[1]:
        assessment_id = st.text_input("Assessment ID", key="assess_get_id")
        if st.button("GET /assessments/{id}"):
            if not assessment_id.strip():
                st.error("Assessment ID is required.")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/assessments/{assessment_id.strip()}")
                    data = _request_json("GET", url, timeout=timeout)
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get assessment failed: {exc}")

    with assessment_tabs[2]:
        with st.form("assess_create"):
            create_company_id = st.text_input("Company ID")
            assessment_type = st.selectbox("Assessment Type", ASSESSMENT_TYPES)
            assessment_date = st.date_input("Assessment Date")
            primary_assessor = st.text_input("Primary Assessor (optional)")
            secondary_assessor = st.text_input("Secondary Assessor (optional)")
            include_vr = st.checkbox("Include VR Score", value=False)
            vr_score = st.number_input("VR Score", min_value=0.0, max_value=100.0, value=50.0)
            include_conf = st.checkbox("Include Confidence Bounds", value=False)
            confidence_lower = st.number_input(
                "Confidence Lower", min_value=0.0, max_value=100.0, value=50.0
            )
            confidence_upper = st.number_input(
                "Confidence Upper", min_value=0.0, max_value=100.0, value=60.0
            )
            submitted_assessment = st.form_submit_button("POST /assessments")

        if submitted_assessment:
            if not create_company_id.strip():
                st.error("Company ID is required.")
            else:
                payload = {
                    "company_id": create_company_id.strip(),
                    "assessment_type": assessment_type,
                    "assessment_date": _pick_date(assessment_date),
                }
                if primary_assessor.strip():
                    payload["primary_assessor"] = primary_assessor.strip()
                if secondary_assessor.strip():
                    payload["secondary_assessor"] = secondary_assessor.strip()
                if include_vr:
                    payload["vr_score"] = float(vr_score)
                if include_conf:
                    payload["confidence_lower"] = float(confidence_lower)
                    payload["confidence_upper"] = float(confidence_upper)

                try:
                    url = _api_url(api_base, api_prefix, "/assessments")
                    data = _request_json("POST", url, json=payload, timeout=timeout)
                    st.success("Assessment created")
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Create assessment failed: {exc}")

    with assessment_tabs[3]:
        with st.form("assess_status"):
            status_assessment_id = st.text_input("Assessment ID")
            new_status = st.selectbox("New Status", ASSESSMENT_STATUSES)
            submitted_status = st.form_submit_button("PATCH /assessments/{id}/status")

        if submitted_status:
            if not status_assessment_id.strip():
                st.error("Assessment ID is required.")
            else:
                payload = {"status": new_status}
                try:
                    url = _api_url(
                        api_base, api_prefix, f"/assessments/{status_assessment_id.strip()}/status"
                    )
                    data = _request_json("PATCH", url, json=payload, timeout=timeout)
                    st.success("Status updated")
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Update status failed: {exc}")

    with assessment_tabs[4]:
        score_tabs = st.tabs(["List Scores", "Upsert Score", "Raw JSON"])

        with score_tabs[0]:
            scores_assessment_id = st.text_input("Assessment ID", key="scores_list_id")
            scores_page = st.number_input("Page", min_value=1, value=1, key="scores_list_page")
            scores_page_size = st.number_input(
                "Page Size", min_value=1, max_value=100, value=20, key="scores_list_size"
            )
            if st.button("GET /assessments/{id}/scores"):
                if not scores_assessment_id.strip():
                    st.error("Assessment ID is required.")
                else:
                    try:
                        url = _api_url(
                            api_base,
                            api_prefix,
                            f"/assessments/{scores_assessment_id.strip()}/scores",
                        )
                        data = _request_json(
                            "GET",
                            url,
                            params={"page": int(scores_page), "page_size": int(scores_page_size)},
                            timeout=timeout,
                        )
                        st.write(f"Total: {data.get('total', 'n/a')}")
                        st.dataframe(data.get("items", []), use_container_width=True)
                    except requests.HTTPError as exc:
                        _show_http_error(exc)
                    except Exception as exc:
                        st.error(f"Fetch scores failed: {exc}")

        with score_tabs[1]:
            with st.form("scores_upsert"):
                upsert_assessment_id = st.text_input("Assessment ID", key="scores_upsert_id")
                dimension = st.selectbox("Dimension", DIMENSIONS)
                score = st.number_input("Score", min_value=0.0, max_value=100.0, value=50.0)
                include_weight = st.checkbox("Include Weight", value=False)
                weight = st.number_input("Weight", min_value=0.0, max_value=1.0, value=0.2)
                confidence = st.slider("Confidence", min_value=0.0, max_value=1.0, value=0.8)
                evidence_count = st.number_input("Evidence Count", min_value=0, value=0)
                submitted_score = st.form_submit_button("POST /assessments/{id}/scores")

            if submitted_score:
                if not upsert_assessment_id.strip():
                    st.error("Assessment ID is required.")
                else:
                    payload = {
                        "assessment_id": upsert_assessment_id.strip(),
                        "dimension": dimension,
                        "score": float(score),
                        "confidence": float(confidence),
                        "evidence_count": int(evidence_count),
                    }
                    if include_weight:
                        payload["weight"] = float(weight)
                    try:
                        url = _api_url(
                            api_base, api_prefix, f"/assessments/{upsert_assessment_id.strip()}/scores"
                        )
                        data = _request_json("POST", url, json=payload, timeout=timeout)
                        st.success("Score upserted")
                        st.json(data)
                    except requests.HTTPError as exc:
                        _show_http_error(exc)
                    except Exception as exc:
                        st.error(f"Upsert score failed: {exc}")

        with score_tabs[2]:
            st.caption("Use this for custom payloads. Path uses prefix.")
            raw_assessment_id = st.text_input("Assessment ID", key="scores_raw_id")
            payload = _json_editor(
                "Payload",
                {
                    "assessment_id": "<uuid>",
                    "dimension": "data_infrastructure",
                    "score": 50,
                    "confidence": 0.8,
                    "evidence_count": 0,
                },
            )
            if st.button("POST Raw to /assessments/{id}/scores"):
                if not raw_assessment_id.strip():
                    st.error("Assessment ID is required.")
                else:
                    try:
                        url = _api_url(
                            api_base, api_prefix, f"/assessments/{raw_assessment_id.strip()}/scores"
                        )
                        data = _request_json("POST", url, json=payload, timeout=timeout)
                        st.success("Score upserted")
                        st.json(data)
                    except requests.HTTPError as exc:
                        _show_http_error(exc)
                    except Exception as exc:
                        st.error(f"Raw upsert failed: {exc}")

# ============================================================
# CS2: Documents
# ============================================================

with main_tabs[2]:
    st.subheader("Documents")
    doc_tabs = st.tabs(["Collect", "List", "Get", "Get Chunks"])

    with doc_tabs[0]:
        st.caption("Triggers document collection for a company (runs server-side).")
        with st.form("doc_collect_form"):
            company_id = st.text_input("Company ID (UUID)", key="doc_collect_company_id")
            filing_types = st.multiselect(
                "Filing types", DEFAULT_FILING_TYPES, default=DEFAULT_FILING_TYPES
            )
            years_back = st.number_input("Years back", min_value=1, max_value=10, value=3)
            submitted = st.form_submit_button("POST /documents/collect")

        if submitted:
            if not company_id.strip():
                st.error("Company ID is required.")
            else:
                payload = {
                    "company_id": company_id.strip(),
                    "filing_types": filing_types,
                    "years_back": int(years_back),
                }
                try:
                    url = _api_url(api_base, api_prefix, "/documents/collect")
                    data = _request_json("POST", url, json=payload, timeout=timeout)
                    st.success("Queued")
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Collect documents failed: {exc}")

    with doc_tabs[1]:
        st.caption("List documents (optionally filter by company and filing type).")
        company_id = st.text_input("Company ID filter (optional)", key="doc_list_company_id")
        filing_type = st.selectbox("Filing type filter (optional)", [""] + DEFAULT_FILING_TYPES, index=0)
        limit = st.number_input("Limit", min_value=1, max_value=1000, value=200, key="doc_list_limit")
        if st.button("GET /documents"):
            try:
                url = _api_url(api_base, api_prefix, "/documents")
                params: Dict[str, Any] = {"limit": int(limit)}
                if company_id.strip():
                    params["company_id"] = company_id.strip()
                if filing_type.strip():
                    params["filing_type"] = filing_type.strip()
                data = _request_json("GET", url, params=params, timeout=timeout)
                items = data.get("items") if isinstance(data, dict) else data
                st.dataframe(items or [], use_container_width=True)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List documents failed: {exc}")

    with doc_tabs[2]:
        document_id = st.text_input("Document ID", key="doc_get_id")
        if st.button("GET /documents/{id}"):
            if not document_id.strip():
                st.error("Document ID is required.")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/documents/{document_id.strip()}")
                    data = _request_json("GET", url, timeout=timeout)
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get document failed: {exc}")

    with doc_tabs[3]:
        document_id = st.text_input("Document ID", key="doc_chunks_id")
        limit = st.number_input(
            "Limit", min_value=1, max_value=1000, value=200, key="doc_chunks_limit"
        )
        if st.button("GET /documents/{id}/chunks"):
            if not document_id.strip():
                st.error("Document ID is required.")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/documents/{document_id.strip()}/chunks")
                    data = _request_json("GET", url, params={"limit": int(limit)}, timeout=timeout)
                    items = data.get("items") if isinstance(data, dict) else data
                    st.dataframe(items or [], use_container_width=True)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get document chunks failed: {exc}")

# ============================================================
# CS2: Signals
# ============================================================

with main_tabs[3]:
    st.subheader("Signals")
    # FIXED: labels match the logic in your handlers
    sig_tabs = st.tabs(["Collect", "List", "Company Summary", "Company By Category", "Get by ID"])

    with sig_tabs[0]:
        st.caption("Triggers signal collection for a company.")
        with st.form("signals_collect_form"):
            company_id = st.text_input("Company ID (UUID)", key="sig_collect_company_id")
            submitted = st.form_submit_button("POST /signals/collect")

        if submitted:
            if not company_id.strip():
                st.error("Company ID is required.")
            else:
                payload = {"company_id": company_id.strip()}
                try:
                    url = _api_url(api_base, api_prefix, "/signals/collect")
                    data = _request_json("POST", url, json=payload, timeout=timeout)
                    st.success("Queued")
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Collect signals failed: {exc}")

    with sig_tabs[1]:
        st.caption("List signals (filterable by company_id, ticker, type, source).")
        company_id = st.text_input("Company ID filter (optional)", key="sig_list_company_id")
        ticker = st.text_input("Ticker filter (optional)", key="sig_list_ticker")
        signal_type = st.text_input(
            "Signal type filter (optional) e.g. news/jobs/tech/patents", key="sig_list_type"
        )
        source = st.text_input("Source filter (optional)", key="sig_list_source")
        limit = st.number_input("Limit", min_value=1, max_value=1000, value=200, key="sig_list_limit")
        if st.button("GET /signals"):
            try:
                url = _api_url(api_base, api_prefix, "/signals")
                params: Dict[str, Any] = {"limit": int(limit)}
                if company_id.strip():
                    params["company_id"] = company_id.strip()
                if ticker.strip():
                    params["ticker"] = ticker.strip()
                if signal_type.strip():
                    params["signal_type"] = signal_type.strip()
                if source.strip():
                    params["source"] = source.strip()
                data = _request_json("GET", url, params=params, timeout=timeout)
                items = data.get("items") if isinstance(data, dict) else data
                st.dataframe(items or [], use_container_width=True)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List signals failed: {exc}")

    with sig_tabs[2]:
        st.caption("Get signal summary for a company.")
        company_id = st.text_input("Company ID (UUID)", key="sig_company_summary_id")
        if st.button("GET /companies/{id}/signals"):
            if not company_id.strip():
                st.error("Company ID is required.")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/companies/{company_id.strip()}/signals")
                    data = _request_json("GET", url, timeout=timeout)
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get company signal summary failed: {exc}")

    with sig_tabs[3]:
        st.caption("Get signals for a company by category.")
        company_id = st.text_input("Company ID (UUID)", key="sig_company_bycat_id")
        category = st.text_input(
            "Category (e.g. technology_hiring / innovation_activity / ...)",
            key="sig_company_bycat_cat",
        )
        limit = st.number_input(
            "Limit", min_value=1, max_value=1000, value=200, key="sig_company_bycat_limit"
        )
        if st.button("GET /companies/{id}/signals/{category}"):
            if not company_id.strip() or not category.strip():
                st.error("Company ID and category are required.")
            else:
                try:
                    url = _api_url(
                        api_base, api_prefix, f"/companies/{company_id.strip()}/signals/{category.strip()}"
                    )
                    data = _request_json("GET", url, params={"limit": int(limit)}, timeout=timeout)
                    items = data.get("items") if isinstance(data, dict) else data
                    st.dataframe(items or [], use_container_width=True)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get signals by category failed: {exc}")

    with sig_tabs[4]:
        st.caption("If your API has GET /signals/{signal_id}, test it here.")
        signal_id = st.text_input("Signal ID", key="sig_get_id")
        if st.button("GET /signals/{signal_id}"):
            if not signal_id.strip():
                st.error("Signal ID is required.")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/signals/{signal_id.strip()}")
                    data = _request_json("GET", url, timeout=timeout)
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get signal failed: {exc}")

# ============================================================
# CS2: Evidence
# ============================================================

with main_tabs[4]:
    st.subheader("Evidence")
    # FIXED: labels match content
    ev_tabs = st.tabs(["Company Evidence", "Backfill", "Stats"])

    with ev_tabs[0]:
        st.caption("Get all evidence for a company.")
        company_id = st.text_input("Company ID (UUID)", key="evidence_company_id")
        if st.button("GET /companies/{id}/evidence"):
            if not company_id.strip():
                st.error("Company ID is required.")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/companies/{company_id.strip()}/evidence")
                    data = _request_json("GET", url, timeout=timeout)
                    st.json(data)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get company evidence failed: {exc}")

    with ev_tabs[1]:
        st.caption("Backfill evidence for all 10 companies.")
        if st.button("POST /evidence/backfill"):
            try:
                url = _api_url(api_base, api_prefix, "/evidence/backfill")
                data = _request_json("POST", url, json={}, timeout=timeout)
                st.success("Queued")
                st.json(data)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Backfill failed: {exc}")

    with ev_tabs[2]:
        st.caption("Evidence collection statistics.")
        if st.button("GET /evidence/stats"):
            try:
                url = _api_url(api_base, api_prefix, "/evidence/stats")
                data = _request_json("GET", url, timeout=timeout)
                st.json(data)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Stats failed: {exc}")
