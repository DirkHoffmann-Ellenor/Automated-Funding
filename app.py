import os, pandas as pd
import streamlit as st
from datetime import datetime
from typing import List


from utils.constants import (
    SAVE_DIR, CSV_COLUMNS, ELIGIBILITY_ORDER
)

from utils.tools import (
    canon_funder_url, normalize_url, process_single_fund, canon_funder_url, load_results_csv, append_to_google_sheet, _get_sheet
        )

# ==========================================
# ========== SESSION / NAVIGATION ==========
# ==========================================

def login():
    st.title("🔐 Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    username = "DirkHoffmann"
    password = "H0ffmann123"

    if st.button("Login"):
        if username in st.secrets["users"] and st.secrets["users"][username] == password:
            st.session_state["logged_in"] = True
            st.session_state["username"] = username  # <-- SAVE USERNAME
            st.rerun()
        else:
            st.error("Invalid username or password")


if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    login()
    st.stop()
    
    
def init_session():
    # Load the user's API key once they are logged in
    if "api_key" not in st.session_state:
        if "username" in st.session_state:  
            user = st.session_state["username"]

            # If the user has a matching API key, store it
            if user in st.secrets["user_api_keys"]:
                st.session_state.api_key = st.secrets["user_api_keys"][user]
            else:
                st.session_state.api_key = ""  # fallback

        else:
            # No logged-in user yet
            st.session_state.api_key = ""

    if "logs" not in st.session_state:
        st.session_state.logs = []
    if "last_run_results" not in st.session_state:
        st.session_state.last_run_results = []
    if "page" not in st.session_state:
        st.session_state.page = "Scrape & Analyze"
    if "unlocked" not in st.session_state:
        st.session_state.unlocked = False

def set_sidebar_nav():
    with st.sidebar:
        
        st.title("ellenor Funding")
        st.caption("Scrape → Analyze → Review results")

        if st.button("🌐 Scrape & Analyze", key="nav_scrape", use_container_width=True):
            st.session_state.page = "Scrape & Analyze"

        if st.button("📊 Results", key="nav_results", use_container_width=True):
            st.session_state.page = "Results"
            
        if st.button("⚙️ Settings", key="nav_settings", use_container_width=True):
            st.session_state.page = "Settings"

        st.markdown("---")
        key_status = "Loaded" if st.session_state.api_key.strip() else "Not set"
        st.metric("API key", key_status)


# ===============================
# ========== UI PAGES ===========
# ===============================

def _eligibility_color(val: str) -> str:
    palette = {
        "Highly Eligible": "#1f9d55",  # green
        "Eligible": "#2d7dff",         # blue
        "Possibly Eligible": "#b7791f",# amber
        "Low Match": "#6b7280",        # gray
        "Not Eligible": "#dc2626"      # red
    }
    return palette.get(val, "#6b7280")

def _results_metrics(df: pd.DataFrame):
    total = len(df)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1: st.metric("Total processed", total)
    for idx, (col, label) in enumerate([
        ("Highly Eligible", "Highly Eligible"),
        ("Eligible", "Eligible"),
        ("Possibly Eligible", "Possibly Eligible"),
        ("Not Eligible", "Not Eligible")
    ]):
        count = (df["eligibility"] == col).sum() if "eligibility" in df.columns else 0
        [col2, col3, col4, col5][idx].metric(label, count)

def page_results():
    st.title("📊 Results")
    st.caption("Browse, filter, and export analyzed funds. Click URLs to open funding pages.")

    df = load_results_csv()
    if df.empty:
        st.info("No results yet. Use **Scrape & Analyze** to add funds.")
        return

    # Reorder: fund_url → eligibility → fund_name → rest
    ordered_cols = (
        ["fund_url", "eligibility", "fund_name",
         "application_status", "deadline", "funding_range",
         "geographic_scope", "applicant_types", "beneficiary_focus",
         "restrictions", "notes", "evidence",
         "pages_scraped", "visited_urls_count", "extraction_timestamp", "error"]
    )
    df = df[[c for c in ordered_cols if c in df.columns]]

    # Filters
    with st.expander("Filters", expanded=True):
        elig_filter = st.multiselect("Eligibility", ELIGIBILITY_ORDER, default=ELIGIBILITY_ORDER)
        keyword = st.text_input("Keyword (URL, name, notes, evidence)")
        f = df.copy()
        if elig_filter:
            f = f[f["eligibility"].isin(elig_filter)]
        if keyword.strip():
            kw = keyword.lower()
            mask = (
                f["fund_url"].fillna("").str.lower().str.contains(kw) |
                f["fund_name"].fillna("").str.lower().str.contains(kw) |
                f["notes"].fillna("").str.lower().str.contains(kw) |
                f["evidence"].fillna("").str.lower().str.contains(kw)
            )
            f = f[mask]

    _results_metrics(f)

    # Clickable links & nicer column labels
    colcfg = {
        "fund_url": st.column_config.LinkColumn("Fund URL"),
        "eligibility": st.column_config.TextColumn("Eligibility"),
        "fund_name": st.column_config.TextColumn("Fund Name"),
        "application_status": st.column_config.TextColumn("Status"),
        "deadline": st.column_config.TextColumn("Deadline"),
        "funding_range": st.column_config.TextColumn("Funding Range"),
        "geographic_scope": st.column_config.TextColumn("Scope"),
        "applicant_types": st.column_config.TextColumn("Applicant Types"),
        "beneficiary_focus": st.column_config.TextColumn("Beneficiaries"),
        "restrictions": st.column_config.TextColumn("Restrictions"),
        "notes": st.column_config.TextColumn("Notes"),
        "evidence": st.column_config.TextColumn("Evidence"),
        "pages_scraped": st.column_config.NumberColumn("Pages"),
        "visited_urls_count": st.column_config.NumberColumn("Links Visited"),
        "extraction_timestamp": st.column_config.TextColumn("Extracted At"),
        "error": st.column_config.TextColumn("Error")
    }

    st.dataframe(f, use_container_width=True, height=480, column_config=colcfg)

    st.download_button(
        "Download Filtered CSV",
        data=f.to_csv(index=False).encode("utf-8"),
        file_name="funds_results_filtered.csv",
        mime="text/csv"
    )

    with st.expander("Evidence details"):
        for _, row in f.iterrows():
            color = _eligibility_color(row.get("eligibility",""))
            st.markdown(
                f"**{row.get('fund_name','(unknown)')}** — "
                f"[{row.get('fund_url','')}]({row.get('fund_url','')}) · "
                f"<span style='color:{color};font-weight:600'>{row.get('eligibility','')}</span>",
                unsafe_allow_html=True
            )
            st.caption(f"Status: {row.get('application_status','')} · Deadline: {row.get('deadline','')}")
            st.write(row.get("evidence","") or "_No evidence captured_")
            st.divider()
            
            
def page_scrape():
    st.title("🌐 Scrape & Analyze")
    st.caption("Paste URLs or upload a CSV with `fund_url`. We'll skip items already processed or scraped.")

    # API key helper
    if not st.session_state.api_key.strip():
        st.warning("OpenAI API key not set. Scraping will run, but LLM extraction will be skipped (eligibility = low-confidence).")
        with st.expander("How to enable LLM extraction"):
            st.write("Go to **Settings → API Key** to enter a key, or unlock from your Local Vault.")

    st.subheader("Provide funding URLs")
    
    # Side-by-side inputs
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Enter URLs manually**")
        urls_text = st.text_area("One per line", height=120, placeholder="https://funder.org/grants\nhttps://another.org/funding-programme", label_visibility="collapsed")
    
    with col2:
        st.write("**Upload CSV with `fund_url` column**")
        up = st.file_uploader("Upload CSV", type=["csv"], label_visibility="collapsed")

    input_urls: List[str] = []
    if urls_text.strip():
        input_urls.extend([u.strip() for u in urls_text.strip().splitlines() if u.strip()])
    if up is not None:
        try:
            df_in = pd.read_csv(up)
            if "fund_url" not in df_in.columns:
                st.error("Uploaded CSV must include a `fund_url` column.")
            else:
                input_urls.extend([str(u).strip() for u in df_in["fund_url"].dropna().tolist()])
        except Exception as e:
            st.error(f"Could not read uploaded CSV: {e}")

    input_urls = [normalize_url(u) for u in input_urls]
    input_urls = list(dict.fromkeys(input_urls))  # dedupe

    if not input_urls:
        st.info("Add URLs or upload a CSV to proceed.")
        return

    # -----------------------------------------------
    # DUPLICATE CHECK
    # -----------------------------------------------
    st.subheader("Duplicate check")

    existing_df = load_results_csv()
    processed_urls = {canon_funder_url(u): u for u in existing_df["fund_url"].astype(str).tolist()}

    # Categorize URLs
    duplicates = []
    unique_new = []

    for u in input_urls:
        norm = canon_funder_url(u)
        if norm in processed_urls:
            duplicates.append((u, processed_urls[norm]))
        else:
            unique_new.append(u)

    # Side-by-side display for duplicates and new URLs
    col_dup, col_new = st.columns(2)
    
    urls_to_reprocess = []
    
    with col_dup:
        if duplicates:
            st.warning(f"⚠️ {len(duplicates)} Duplicate(s)")
            with st.expander("Select URLs to re-process", expanded=True):
                st.caption("Check URLs to re-scrape (existing data will be deleted)")
                for idx, (new_u, old_u) in enumerate(duplicates):
                    display_url = new_u if new_u == old_u else f"{new_u} (exists as: {old_u})"
                    if st.checkbox(display_url, key=f"dup_{idx}"):
                        urls_to_reprocess.append(new_u)
                
                if urls_to_reprocess:
                    st.info(f"📝 {len(urls_to_reprocess)} selected for re-processing")
        else:
            st.success("✅ No duplicates found")
    
    with col_new:
        if unique_new:
            st.success(f"✨ {len(unique_new)} New URL(s)")
            with st.expander("Show new URLs", expanded=True):
                for u in unique_new:
                    st.write(f"- {u}")
        else:
            st.info("No new URLs to process")

    # Build final processing list
    will_process = list(unique_new) + urls_to_reprocess

    if not will_process:
        st.info("Nothing to process. Add new URLs or select URLs to re-process.")
        return

    # Show summary
    st.success(f"🎯 Total: {len(will_process)} URL(s) ready to process")

    # Delete selected duplicates from Google Sheets
    if urls_to_reprocess:
        try:
            ws = _get_sheet()
            values = ws.get_all_values()
            fund_url_idx = values[0].index("fund_url")

            # Normalize URLs to delete
            norms_to_delete = {canon_funder_url(u) for u in urls_to_reprocess}

            # Delete matching rows (iterate bottom-up)
            delete_count = 0
            for r in range(len(values) - 1, 0, -1):
                row_url = canon_funder_url(values[r][fund_url_idx])
                if row_url in norms_to_delete:
                    ws.delete_rows(r + 1)
                    delete_count += 1

            if delete_count:
                st.success(f"🗑️ Deleted {delete_count} existing row(s) from Google Sheets")
        except Exception as e:
            st.error(f"Error deleting rows: {e}")

    # Continue button
    go = st.button("🚀 Start Processing", type="primary", use_container_width=True)
    if not go:
        return

    # Clear logs and prep trackers
    st.session_state.logs = []
    progress = st.progress(0)
    results: List[dict] = []
    errs = []

    os.makedirs(SAVE_DIR, exist_ok=True)

    # Container for processing status
    st.subheader("Processing Status")
    processing_container = st.container()

    # Track completion status for each URL
    url_status = {url: {"done": False, "result": None, "error": None} for url in will_process}

    for i, url in enumerate(will_process, start=1):
        # Create expander for this URL
        with processing_container:
            with st.expander(f"{'🔄' if not url_status[url]['done'] else '✅'} {url}", expanded=False):
                log_placeholder = st.empty()
                log_placeholder.info(f"Processing... ({i}/{len(will_process)})")
                
                try:
                    res = process_single_fund(url)
                    results.append(res)
                    url_status[url]["done"] = True
                    url_status[url]["result"] = res
                    
                    if res.get("error"):
                        errs.append((url, res["error"]))
                        url_status[url]["error"] = res["error"]
                        log_placeholder.error(f"✅ Completed with error: {res['error']}")
                    else:
                        log_placeholder.success("✅ Successfully processed")
                        
                        # Show result details
                        c1, c2, c3 = st.columns([2,1,1])
                        with c1:
                            st.markdown(f"**Fund Name:** {res.get('fund_name', 'N/A')}")
                            st.markdown(f"**URL:** [{res.get('fund_url','')}]({res.get('fund_url','')})")
                            st.markdown(f"**Funding range:** {res.get('funding_range','N/A')}")
                            st.markdown(f"**Scope:** {res.get('geographic_scope','N/A')}")
                        with c2:
                            st.metric("Pages scraped", int(res.get("pages_scraped") or 0))
                            st.metric("Links visited", int(res.get("visited_urls_count") or 0))
                        with c3:
                            color = _eligibility_color(res.get("eligibility",""))
                            st.markdown("**Eligibility**")
                            st.markdown(f"<span style='color:{color};font-weight:700'>{res.get('eligibility','N/A')}</span>", unsafe_allow_html=True)

                        with st.expander("📋 Evidence", expanded=False):
                            st.write(res.get("evidence", "") or "_No evidence recorded_")
                        with st.expander("📝 Details", expanded=False):
                            st.write(f"**Notes:** {res.get('notes','N/A')}")
                            st.write(f"**Restrictions:** {res.get('restrictions','N/A')}")
                            st.write(f"**Applicant types:** {res.get('applicant_types','N/A')}")
                            st.write(f"**Beneficiary focus:** {res.get('beneficiary_focus','N/A')}")

                    # Append to master CSV as we go
                    try:
                        append_to_google_sheet([res])
                    except Exception as e:
                        st.error(f"Could not save results: {e}")

                except Exception as e:
                    errs.append((url, str(e)))
                    url_status[url]["done"] = True
                    url_status[url]["error"] = str(e)
                    log_placeholder.error(f"❌ Error: {str(e)}")

        progress.progress(int(i/len(will_process)*100))

    st.success("✅ All URLs processed")

    if errs:
        st.subheader("⚠️ Issues encountered")
        grouped = {}
        for u, e in errs:
            key = "Network" if any(k in e for k in ["Name or service", "Failed to establish", "timeout"]) else \
                  "Access/HTTP" if any(k in e for k in ["403", "404", "429", "5"]) else \
                  "Other"
            grouped.setdefault(key, []).append((u, e))
        for g, items in grouped.items():
            with st.expander(f"{g} errors ({len(items)})", expanded=False):
                for u, e in items:
                    st.write(f"• **{u}** — {e}")

    # Batch table
    st.subheader("📊 This batch summary")
    df_new = pd.DataFrame(results)
    if not df_new.empty:
        for col in CSV_COLUMNS:
            if col not in df_new.columns:
                df_new[col] = ""
        # reorder like Results page
        ordered_cols = ["fund_url", "eligibility", "fund_name", "application_status", "deadline",
                        "funding_range", "geographic_scope", "applicant_types", "beneficiary_focus",
                        "restrictions", "notes", "evidence",
                        "pages_scraped", "visited_urls_count", "extraction_timestamp", "error"]
        df_new = df_new[[c for c in ordered_cols if c in df_new.columns]]

        colcfg = {
            "fund_url": st.column_config.LinkColumn("Fund URL", display_text="Open"),
            "eligibility": st.column_config.TextColumn("Eligibility"),
            "fund_name": st.column_config.TextColumn("Fund Name"),
            "application_status": st.column_config.TextColumn("Status"),
            "deadline": st.column_config.TextColumn("Deadline"),
            "funding_range": st.column_config.TextColumn("Funding Range"),
            "geographic_scope": st.column_config.TextColumn("Scope"),
            "applicant_types": st.column_config.TextColumn("Applicant Types"),
            "beneficiary_focus": st.column_config.TextColumn("Beneficiaries"),
            "restrictions": st.column_config.TextColumn("Restrictions"),
            "notes": st.column_config.TextColumn("Notes"),
            "evidence": st.column_config.TextColumn("Evidence"),
            "pages_scraped": st.column_config.NumberColumn("Pages"),
            "visited_urls_count": st.column_config.NumberColumn("Links Visited"),
            "extraction_timestamp": st.column_config.TextColumn("Extracted At"),
            "error": st.column_config.TextColumn("Error")
        }
        st.dataframe(df_new, use_container_width=True, height=420, column_config=colcfg)
        st.download_button(
            "Download This Batch (CSV)",
            data=df_new.to_csv(index=False).encode("utf-8"),
            file_name="funds_results_batch.csv",
            mime="text/csv"
        )
    else:
        st.info("No results produced.")
            
    st.markdown("---")
    st.subheader("🔁 Reprocess from scraped text (LLM only)")
    st.caption("Reads text files already in `Scraped/` and re-runs the LLM extractor without re-crawling.")

def page_settings():
    st.title("⚙️ Settings")
    st.caption("Manage API key, Local Vault, and utilities.")

    st.subheader("API Key")
    st.write("Set an OpenAI API key for LLM extraction.")
    key = st.text_input("OpenAI API key", value=st.session_state.api_key, type="password", help="Used only in this session unless saved to Local Vault.")
    b1, b2 = st.columns([1,1])
    with b1:
        if st.button("Save to Session", use_container_width=True):
            st.session_state.api_key = key.strip()
            st.success("API key saved to session.")
    with b2:
        if st.button("Clear from Session", type="secondary", use_container_width=True):
            st.session_state.api_key = ""
            st.warning("API key cleared from session.")

# ===============================
# ========== NAV / MAIN =========
# ===============================

def main():
    st.set_page_config(page_title="ellenor Auto Funding Discovery", page_icon="Logo.png", layout="wide")
    init_session()
    if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
        login()
        st.stop()
    set_sidebar_nav()

    page = st.session_state.page
    if page == "Results":
        page_results()
    elif page == "Settings":
        page_settings()
    else:
        page_scrape()  # default / main flow

if __name__ == "__main__":
    main()
