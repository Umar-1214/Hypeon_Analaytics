"""
Generate 90 days of realistic sample data for a brand: Meta/Google ads + Shopify orders.
Run from repo root: python scripts/generate_sample_data.py
Writes to data/raw/*.csv
"""
import csv
import random
from datetime import date, timedelta

random.seed(42)

START = date(2025, 1, 1)
DAYS = 90
END = START + timedelta(days=DAYS - 1)

# ---- Meta: 3 campaigns ----
META_CAMPAIGNS = [
    ("meta_br", "Brand Awareness"),
    ("meta_conv", "Conversion - Purchase"),
    ("meta_ret", "Retargeting - Cart"),
]
# Base daily spend per campaign (then add noise + weekly + trend)
META_BASE = {"meta_br": 80, "meta_conv": 200, "meta_ret": 120}


def meta_spend(d: date, cid: str, cname: str) -> tuple:
    base = META_BASE.get(cid, 100)
    day_idx = (d - START).days
    trend = 1.0 + 0.001 * day_idx  # slight upward trend
    weekly = 1.2 if d.weekday() < 5 else 0.85  # weekday vs weekend
    noise = random.gauss(1.0, 0.15)
    spend = max(10, round(base * trend * weekly * noise, 2))
    imp = int(spend * random.gauss(12, 2))
    clk = max(0, int(imp * random.gauss(0.02, 0.005)))
    return (d, cid, cname, spend, imp, clk)


# ---- Google: 2 campaigns ----
GOOGLE_CAMPAIGNS = [
    ("goog_search", "Search - Brand"),
    ("goog_pmax", "Performance Max"),
]
GOOGLE_BASE = {"goog_search": 90, "goog_pmax": 180}


def google_spend(d: date, cid: str, cname: str) -> tuple:
    base = GOOGLE_BASE.get(cid, 100)
    day_idx = (d - START).days
    trend = 1.0 + 0.0008 * day_idx
    weekly = 1.15 if d.weekday() < 5 else 0.9
    noise = random.gauss(1.0, 0.12)
    spend = max(10, round(base * trend * weekly * noise, 2))
    imp = int(spend * random.gauss(25, 4))
    clk = max(0, int(imp * random.gauss(0.015, 0.004)))
    return (d, cid, cname, spend, imp, clk)


# ---- Bing: 2 campaigns ----
BING_CAMPAIGNS = [
    ("bing_search", "Bing Search - Brand"),
    ("bing_shop", "Bing Shopping"),
]
BING_BASE = {"bing_search": 40, "bing_shop": 60}


def bing_spend(d: date, cid: str, cname: str) -> tuple:
    base = BING_BASE.get(cid, 50)
    day_idx = (d - START).days
    trend = 1.0 + 0.0005 * day_idx
    weekly = 1.1 if d.weekday() < 5 else 0.9
    noise = random.gauss(1.0, 0.14)
    spend = max(5, round(base * trend * weekly * noise, 2))
    imp = int(spend * random.gauss(20, 3))
    clk = max(0, int(imp * random.gauss(0.018, 0.004)))
    return (d, cid, cname, spend, imp, clk)


# ---- Pinterest: 2 campaigns ----
PINTEREST_CAMPAIGNS = [
    ("pin_aware", "Pinterest Awareness"),
    ("pin_conv", "Pinterest Conversions"),
]
PINTEREST_BASE = {"pin_aware": 35, "pin_conv": 55}


def pinterest_spend(d: date, cid: str, cname: str) -> tuple:
    base = PINTEREST_BASE.get(cid, 45)
    day_idx = (d - START).days
    trend = 1.0 + 0.0006 * day_idx
    weekly = 1.05 if d.weekday() < 5 else 0.95
    noise = random.gauss(1.0, 0.13)
    spend = max(5, round(base * trend * weekly * noise, 2))
    imp = int(spend * random.gauss(18, 3))
    clk = max(0, int(imp * random.gauss(0.02, 0.005)))
    return (d, cid, cname, spend, imp, clk)


# ---- Ad clicks pool for click-ID attribution ----
# Each entry: (click_id, date, campaign_id, campaign_name, channel)
def gen_ad_clicks():
    clicks = []
    click_idx = 1
    channels_campaigns = [
        ("meta", META_CAMPAIGNS),
        ("google", GOOGLE_CAMPAIGNS),
        ("bing", BING_CAMPAIGNS),
        ("pinterest", PINTEREST_CAMPAIGNS),
    ]
    for d in (START + timedelta(days=i) for i in range(DAYS)):
        for channel, campaigns in channels_campaigns:
            for cid, cname in campaigns:
                # 2–8 clicks per campaign per day
                n_clicks = random.randint(2, 8)
                for _ in range(n_clicks):
                    prefix = "fbclid" if channel == "meta" else "gclid" if channel == "google" else "bing_" if channel == "bing" else "pin_"
                    clicks.append((f"{prefix}_{click_idx}", d, cid, cname, channel))
                    click_idx += 1
    return clicks


# ---- Orders: revenue with lag vs spend; optional click_id for subset ----
def gen_orders(ad_clicks_list):
    """ad_clicks_list: list of (click_id, date, campaign_id, campaign_name, channel) for assignment."""
    daily_spend = {}
    for d in (START + timedelta(days=i) for i in range(DAYS)):
        total = sum(META_BASE.values()) + sum(GOOGLE_BASE.values())
        daily_spend[d] = total * random.gauss(1.0, 0.1)
    orders = []
    order_id = 1
    # Build click pool by date (so we can assign a click on or before order date)
    clicks_by_date = {}
    for cid, d, camp_id, cname, ch in ad_clicks_list:
        clicks_by_date.setdefault(d, []).append((cid, camp_id, cname, ch))
    sorted_dates = sorted(clicks_by_date.keys())
    for d in (START + timedelta(days=i) for i in range(DAYS)):
        base_orders = 8 + int(daily_spend[d] / 50)
        n_orders = max(2, min(30, int(random.gauss(base_orders, 4))))
        for _ in range(n_orders):
            if random.random() < 0.6:
                rev = round(random.gauss(45, 20), 2)
            else:
                rev = round(random.gauss(120, 50), 2)
            rev = max(10, rev)
            is_new = random.random() < 0.4
            name = f"#{1000 + order_id}"
            click_id, utm_src, utm_med, utm_camp = None, None, None, None
            if random.random() < 0.35 and sorted_dates:  # 35% of orders have click-ID
                # Pick a click on or before order date
                before = [sd for sd in sorted_dates if sd <= d]
                if before:
                    pick_date = random.choice(before)
                    pool = clicks_by_date.get(pick_date, [])
                    if pool:
                        cid, camp_id, cname, ch = random.choice(pool)
                        click_id = cid
                        utm_src = ch
                        utm_med = "cpc"
                        utm_camp = camp_id
            orders.append((f"ord_{order_id}", name, d, rev, is_new, click_id or "", utm_src or "", utm_med or "", utm_camp or ""))
            order_id += 1
    return orders


def main():
    from pathlib import Path
    repo_root = Path(__file__).resolve().parent.parent
    out_dir = repo_root / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Meta
    with open(out_dir / "meta_ads.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "campaign_id", "campaign_name", "spend", "impressions", "clicks"])
        for d in (START + timedelta(days=i) for i in range(DAYS)):
            for cid, cname in META_CAMPAIGNS:
                w.writerow(meta_spend(d, cid, cname))

    # Google
    with open(out_dir / "google_ads.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "campaign_id", "campaign_name", "spend", "impressions", "clicks"])
        for d in (START + timedelta(days=i) for i in range(DAYS)):
            for cid, cname in GOOGLE_CAMPAIGNS:
                w.writerow(google_spend(d, cid, cname))

    # Bing
    with open(out_dir / "bing_ads.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "campaign_id", "campaign_name", "spend", "impressions", "clicks"])
        for d in (START + timedelta(days=i) for i in range(DAYS)):
            for cid, cname in BING_CAMPAIGNS:
                w.writerow(bing_spend(d, cid, cname))

    # Pinterest
    with open(out_dir / "pinterest_ads.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "campaign_id", "campaign_name", "spend", "impressions", "clicks"])
        for d in (START + timedelta(days=i) for i in range(DAYS)):
            for cid, cname in PINTEREST_CAMPAIGNS:
                w.writerow(pinterest_spend(d, cid, cname))

    # Ad clicks (for click-ID attribution)
    ad_clicks_list = gen_ad_clicks()
    with open(out_dir / "ad_clicks.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["click_id", "date", "campaign_id", "campaign_name", "channel"])
        for row in ad_clicks_list:
            w.writerow(row)

    # Shopify orders (with optional click_id / utm for subset)
    orders = gen_orders(ad_clicks_list)
    with open(out_dir / "shopify_orders.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "name", "order_date", "revenue", "is_new_customer", "click_id", "utm_source", "utm_medium", "utm_campaign"])
        for row in orders:
            w.writerow(row)

    # WooCommerce orders (subset of days; some with click_id)
    wc_orders = []
    wc_id = 1
    sorted_dates = sorted(set(c[1] for c in ad_clicks_list))
    clicks_by_date = {}
    for cid, d, camp_id, cname, ch in ad_clicks_list:
        clicks_by_date.setdefault(d, []).append((cid, camp_id, cname, ch))
    for d in (START + timedelta(days=i) for i in range(DAYS)):
        if random.random() < 0.4:
            n_wc = random.randint(1, 8)
            for _ in range(n_wc):
                rev = max(15, round(random.gauss(55, 25), 2))
                is_new = random.random() < 0.5
                click_id, utm_src, utm_med, utm_camp = "", "", "", ""
                if random.random() < 0.3 and sorted_dates:
                    before = [sd for sd in sorted_dates if sd <= d]
                    if before:
                        pick_date = random.choice(before)
                        pool = clicks_by_date.get(pick_date, [])
                        if pool:
                            cid, camp_id, cname, ch = random.choice(pool)
                            click_id, utm_src, utm_med, utm_camp = cid, ch, "cpc", camp_id
                wc_orders.append((f"wc_{wc_id}", f"#WC{wc_id}", d, rev, is_new, click_id or "", utm_src or "", utm_med or "", utm_camp or ""))
                wc_id += 1
    with open(out_dir / "woocommerce_orders.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "name", "order_date", "revenue", "is_new_customer", "click_id", "utm_source", "utm_medium", "utm_campaign"])
        for row in wc_orders:
            w.writerow(row)

    print(f"Wrote meta_ads, google_ads, bing_ads, pinterest_ads, ad_clicks, shopify_orders, woocommerce_orders to {out_dir!s}")
    print(f"Date range: {START} to {END}, {len(orders)} Shopify orders, {len(wc_orders)} WooCommerce orders")


if __name__ == "__main__":
    main()
