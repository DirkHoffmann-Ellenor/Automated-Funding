import os
import streamlit as st
import pandas as pd
from datetime import datetime
from Web_scraper_2 import process_new_funds, reprocess_scraped_funds, OUTPUT_CSV, INPUT_CSV

# ----------------------------
#  BASIC APP CONFIG
# ----------------------------
st.set_page_config(page_title="Ellenor Funding Finder", layout="wide")

# Password protection (uses Streamlit secrets)
PASSWORD = st.secrets.get("APP_PASSWORD", None)
if PASSWORD:
    pw_input = st.text_input("🔒 Enter access password", type="password")
    if pw_input != PASSWORD:
        st.warning("Incorrect password. Please enter the correct password to continue.")
        st.stop()

st.title("💸 Ellenor Funding Finder")
st.caption("Automatically find and evaluate funding opportunities for Ellenor Hospice.")

# ----------------------------
#  API Key Management
# ----------------------------
st.sidebar.header("🔑 API Key Management")
api_key = st.sidebar.text_input(
    "Enter your OpenAI API key",
    value=st.secrets.get("APIKEY", ""),
    type="password",
    help="This key is required for LLM-based extraction."
)

if api_key:
    os.environ["APIKEY"] = api_key
    st.sidebar.success("API key set successfully.")
else:
    st.sidebar.warning("No API key set — please enter one.")

# ----------------------------
#  File Upload or Link Input
# ----------------------------
st.header("📂 Upload or Add Funding Links")

tab1, tab2 = st.tabs(["📤 Upload CSV", "🔗 Enter Links Manually"])

with tab1:
    uploaded_csv = st.file_uploader("Upload CSV of funding URLs", type="csv")
    if uploaded_csv:
        df = pd.read_csv(uploaded_csv)
        df.to_csv(INPUT_CSV, index=False)
        st.success(f"✅ Uploaded {len(df)} URLs to process.")
        st.dataframe(df.head())

with tab2:
    urls_text = st.text_area(
        "Enter one or more funding URLs (one per line)",
        placeholder="https://example.com/fund1\nhttps://example.com/fund2"
    )
    if st.button("Add Links"):
        if urls_text.strip():
            urls = [u.strip() for u in urls_text.splitlines() if u.strip()]
            pd.DataFrame({"fund_url": urls}).to_csv(INPUT_CSV, index=False)
            st.success(f"✅ Added {len(urls)} URLs to {INPUT_CSV}")
        else:
            st.warning("Please enter at least one valid URL.")

# ----------------------------
#  Run Processing
# ----------------------------
st.header("⚙️ Process Funding Opportunities")

col1, col2 = st.columns(2)

with col1:
    batch_size = st.number_input("Batch Size", min_value=1, max_value=50, value=5, step=1)

with col2:
    mode = st.selectbox(
        "Processing Mode",
        ["Process New Funds (scrape + extract)", "Reprocess Existing Scraped Funds"]
    )

if st.button("🚀 Start Processing"):
    if not os.path.exists(INPUT_CSV):
        st.error("No input CSV found. Please upload or add URLs first.")
    elif not api_key:
        st.error("Please enter your OpenAI API key first.")
    else:
        with st.spinner("Processing funding data... this may take several minutes."):
            try:
                if "Reprocess" in mode:
                    reprocess_scraped_funds(INPUT_CSV, OUTPUT_CSV, batch_size)
                else:
                    process_new_funds(INPUT_CSV, OUTPUT_CSV, batch_size)
                st.success("✅ Processing complete!")
            except Exception as e:
                st.error(f"❌ Error: {e}")

# ----------------------------
#  Results Display & Cache
# ----------------------------
st.header("📊 Results & History")

if os.path.exists(OUTPUT_CSV):
    df = pd.read_csv(OUTPUT_CSV)
    st.dataframe(df.tail(20))
    st.download_button(
        label="⬇️ Download Results CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"funding_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
else:
    st.info("No results yet. Run a process to generate your first dataset.")

st.sidebar.markdown("---")
st.sidebar.info("Built for Ellenor Hospice by Dirk Hoffmann & GPT-5 🧠")
