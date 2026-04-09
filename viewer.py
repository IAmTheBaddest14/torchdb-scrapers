"""TorchDB Scraper Viewer — localhost dashboard for crawl runs, raw pages, and extracted products."""
import os
import json
from datetime import datetime, timezone

import streamlit as st
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

st.set_page_config(page_title="TorchDB Scraper", layout="wide", page_icon="🔦")

# ── Supabase connection ──────────────────────────────────────────────────────

@st.cache_resource
def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])

@st.cache_data(ttl=30)
def fetch_crawl_runs(brand: str | None = None):
    db = get_supabase()
    q = db.schema("scraper_staging").table("crawl_runs").select("*").order("started_at", desc=True)
    if brand:
        q = q.eq("brand", brand)
    return q.execute().data

@st.cache_data(ttl=30)
def fetch_raw_pages(crawl_run_id: int):
    db = get_supabase()
    return (
        db.schema("scraper_staging").table("raw_pages")
        .select("*").eq("crawl_run_id", crawl_run_id)
        .execute().data
    )

@st.cache_data(ttl=30)
def fetch_extracted_products(crawl_run_id: int | None = None, brand: str | None = None):
    db = get_supabase()
    if crawl_run_id:
        page_ids = [
            p["id"] for p in
            db.schema("scraper_staging").table("raw_pages")
            .select("id").eq("crawl_run_id", crawl_run_id).execute().data
        ]
        if not page_ids:
            return []
        q = db.schema("scraper_staging").table("extracted_products").select("*, raw_pages(url)").in_("raw_page_id", page_ids)
    else:
        q = db.schema("scraper_staging").table("extracted_products").select("*, raw_pages(url)")
        if brand:
            q = q.eq("brand", brand)
    return q.order("extracted_at", desc=True).execute().data

@st.cache_data(ttl=30)
def fetch_promotion_log(extracted_product_id: int):
    db = get_supabase()
    return (
        db.schema("scraper_staging").table("promotion_log")
        .select("*").eq("extracted_product_id", extracted_product_id)
        .order("promoted_at", desc=True).execute().data
    )

# ── Helpers ──────────────────────────────────────────────────────────────────

def confidence_badge(tier: str, score: float) -> str:
    colors = {"high": "🟢", "medium": "🟡", "low": "🔴"}
    return f"{colors.get(tier, '⚪')} {tier.upper()} ({score:.2f})"

def fmt_dt(dt_str: str | None) -> str:
    if not dt_str:
        return "—"
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return dt_str

def render_configuration_graph(graph: dict):
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**LEDs**")
        for led in graph.get("leds", []):
            ccts = ", ".join(led.get("cct_hints", [])) or "—"
            st.markdown(f"- `{led['name']}` — CCT: {ccts}")

        st.markdown("**Drivers**")
        drivers = graph.get("drivers", [])
        if drivers:
            for d in drivers:
                st.markdown(f"- `{d['name']}`")
        else:
            st.markdown("- *none extracted*")

        st.markdown("**Pairings**")
        pairings = graph.get("pairings", [])
        if pairings:
            for p in pairings:
                st.markdown(f"- {p['led']} → {p['driver']}")
        else:
            st.markdown("- *none extracted*")

        # Batteries
        batteries = graph.get("batteries", [])
        if batteries:
            st.markdown("**Batteries**")
            for b in batteries:
                parts = [f"`{b.get('type', '?')}`"]
                if b.get("capacity_mah"):
                    parts.append(f"{b['capacity_mah']} mAh")
                if b.get("included"):
                    parts.append("included")
                if not b.get("removable", True):
                    parts.append("non-removable")
                st.markdown(f"- {' · '.join(parts)}")

    with col2:
        specs = graph.get("specs", {})
        st.markdown("**Physical Specs**")
        rows = {
            "Max Lumens": specs.get("max_lumens"),
            "Length": f"{specs['length_mm']} mm" if specs.get("length_mm") else None,
            "Weight": f"{specs['weight_g']} g" if specs.get("weight_g") else None,
            "Material": specs.get("material"),
        }
        for label, val in rows.items():
            display = val if val is not None else "—"
            st.markdown(f"- **{label}:** {display}")

        price = graph.get("price")
        st.markdown(f"**Price:** {price or '—'}")

        # Tags
        tags = graph.get("tags", [])
        if tags:
            st.markdown("**Tags:** " + " ".join(f"`{t}`" for t in tags))

        # Compatible accessories
        accessories = graph.get("compatible_accessories", [])
        if accessories:
            st.markdown("**Compatible Accessories:** " + ", ".join(accessories))

        url = graph.get("source_url")
        if url:
            st.markdown(f"**Source:** [{url}]({url})")

        pdf_url = graph.get("manual_pdf_url")
        if pdf_url:
            st.markdown(f"**Manual:** [📄 PDF]({pdf_url})")

        ui_url = graph.get("ui_diagram_url")
        if ui_url:
            st.markdown("**UI Diagram:**")
            st.image(ui_url)

    # Mode data table
    mode_data = graph.get("mode_data", [])
    if mode_data:
        st.markdown("**Mode Data**")
        for light in mode_data:
            st.markdown(f"*{light.get('light', 'Unknown')}*")
            modes = light.get("modes", [])
            if modes:
                st.dataframe(modes, use_container_width=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.title("🔦 TorchDB Scraper")

all_runs = fetch_crawl_runs()
brands = sorted({r["brand"] for r in all_runs}) if all_runs else []
brand_filter = st.sidebar.selectbox("Brand", ["All"] + brands)
selected_brand = None if brand_filter == "All" else brand_filter

runs = fetch_crawl_runs(selected_brand)

if st.sidebar.button("🔄 Refresh data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown(f"**{len(runs)} crawl run(s)** found")


# ── Main area ─────────────────────────────────────────────────────────────────

tab_runs, tab_products, tab_raw = st.tabs(["📋 Crawl Runs", "🔬 Extracted Products", "📄 Raw Pages"])


# ── Tab 1: Crawl Runs ─────────────────────────────────────────────────────────

with tab_runs:
    st.header("Crawl Runs")
    if not runs:
        st.info("No crawl runs found.")
    else:
        for run in runs:
            status = "✅ Complete" if run.get("completed_at") else "🔄 In Progress"
            pages = run.get("pages_crawled", 0) or 0
            label = f"{status} — **{run['brand']}** — {fmt_dt(run['started_at'])} — {pages} page(s)"
            with st.expander(label):
                c1, c2, c3 = st.columns(3)
                c1.metric("Brand", run["brand"])
                c2.metric("Pages Crawled", pages)
                c3.metric("Scraper Version", run.get("scraper_version", "—"))
                st.markdown(f"**Started:** {fmt_dt(run['started_at'])}")
                st.markdown(f"**Completed:** {fmt_dt(run.get('completed_at'))}")
                st.markdown(f"**Run ID:** `{run['id']}`")


# ── Tab 2: Extracted Products ─────────────────────────────────────────────────

with tab_products:
    st.header("Extracted Products")

    run_options = {f"{r['brand']} — {fmt_dt(r['started_at'])} (ID {r['id']})": r["id"] for r in runs}
    run_label = st.selectbox("Filter by crawl run", ["All runs"] + list(run_options.keys()))
    selected_run_id = run_options[run_label] if run_label != "All runs" else None

    products = fetch_extracted_products(selected_run_id, selected_brand)

    if not products:
        st.info("No extracted products found.")
    else:
        st.markdown(f"**{len(products)} product(s)**")
        for p in products:
            graph = p.get("configuration_graph", {})
            tier = p.get("confidence_tier", "low")
            score = float(p.get("confidence_score", 0))
            badge = confidence_badge(tier, score)
            source_url = graph.get("source_url") or (p.get("raw_pages") or {}).get("url", "")
            header = f"{badge} — **{p['brand']}** / {p['model']} — `v{p.get('extraction_prompt_version', '?')}`"

            with st.expander(header):
                render_configuration_graph(graph)

                st.markdown("---")
                col_a, col_b = st.columns(2)
                col_a.markdown(f"**Extracted at:** {fmt_dt(p.get('extracted_at'))}")
                col_b.markdown(f"**Product ID:** `{p['id']}`")

                # Promotion log
                promo_log = fetch_promotion_log(p["id"])
                if promo_log:
                    st.markdown("**Promotion Log**")
                    for entry in promo_log:
                        action_icon = {"insert": "✅", "skip": "⏭️", "review-required": "🔍", "rejected": "❌"}.get(entry["action"], "❓")
                        st.markdown(
                            f"{action_icon} `{entry['action']}` — {fmt_dt(entry.get('promoted_at'))}"
                            + (f" — _{entry['diff_summary']}_" if entry.get("diff_summary") else "")
                        )

                # Raw JSON toggle
                with st.expander("Raw JSON"):
                    st.json(graph)


# ── Tab 3: Raw Pages ──────────────────────────────────────────────────────────

with tab_raw:
    st.header("Raw Pages")

    if not runs:
        st.info("No crawl runs found.")
    else:
        run_options_raw = {f"{r['brand']} — {fmt_dt(r['started_at'])} (ID {r['id']})": r["id"] for r in runs}
        run_label_raw = st.selectbox("Select crawl run", list(run_options_raw.keys()), key="raw_run_select")
        selected_run_id_raw = run_options_raw[run_label_raw]

        raw_pages = fetch_raw_pages(selected_run_id_raw)

        if not raw_pages:
            st.info("No raw pages for this run.")
        else:
            st.markdown(f"**{len(raw_pages)} page(s)**")
            for page in raw_pages:
                with st.expander(f"🌐 {page['url']}"):
                    c1, c2 = st.columns(2)
                    c1.markdown(f"**Crawled:** {fmt_dt(page.get('crawled_at'))}")
                    c2.markdown(f"**Page ID:** `{page['id']}`")
                    if page.get("manual_pdf_url"):
                        st.markdown(f"**Manual PDF:** [📄 View]({page['manual_pdf_url']})")

                    variant_data = page.get("raw_variant_data")
                    if variant_data:
                        st.markdown("**Variant Options**")
                        for opt in variant_data.get("options", []):
                            vals = ", ".join(str(v) for v in opt.get("values", []))
                            st.markdown(f"- **{opt['name']}:** {vals}")

                        variants = variant_data.get("variants", [])
                        if variants:
                            st.markdown(f"**{len(variants)} variant(s)**")
                            rows = []
                            for v in variants:
                                row = dict(v.get("options", {}))
                                row["price"] = v.get("price", "")
                                row["available"] = "✓" if v.get("available") else "✗"
                                rows.append(row)
                            st.dataframe(rows, use_container_width=True)

                    with st.expander("Markdown preview"):
                        md = page.get("markdown") or ""
                        st.text(md[:3000] + ("…" if len(md) > 3000 else ""))
