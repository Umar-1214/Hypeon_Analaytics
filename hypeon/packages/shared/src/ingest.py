"""CSV loader: read from data/raw/ and upsert into raw tables; reconcile order net_revenue from transactions."""
import os
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlmodel import Session, select

from packages.shared.src.models import (
    IngestAudit,
    RawAdClicks,
    RawBingAds,
    RawGoogleAds,
    RawMetaAds,
    RawPinterestAds,
    RawShopifyOrders,
    RawShopifyTransactions,
    RawWooCommerceOrders,
)


def _raw_dir() -> Path:
    return Path(os.environ.get("DATA_RAW_DIR", "data/raw")).resolve()


def load_meta_ads(session: Session, csv_path: Optional[Path] = None) -> int:
    """Load or upsert raw_meta_ads from CSV. Returns count of rows processed."""
    path = csv_path or _raw_dir() / "meta_ads.csv"
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    count = 0
    for _, row in df.iterrows():
        existing = session.exec(
            select(RawMetaAds).where(
                RawMetaAds.date == row["date"],
                RawMetaAds.campaign_id == str(row["campaign_id"]),
            )
        ).first()
        if existing:
            existing.campaign_name = row.get("campaign_name")
            existing.spend = float(row.get("spend", 0))
            existing.impressions = int(row["impressions"]) if pd.notna(row.get("impressions")) else None
            existing.clicks = int(row["clicks"]) if pd.notna(row.get("clicks")) else None
        else:
            session.add(
                RawMetaAds(
                    date=row["date"],
                    campaign_id=str(row["campaign_id"]),
                    campaign_name=row.get("campaign_name"),
                    spend=float(row.get("spend", 0)),
                    impressions=int(row["impressions"]) if pd.notna(row.get("impressions")) else None,
                    clicks=int(row["clicks"]) if pd.notna(row.get("clicks")) else None,
                )
            )
        count += 1
    session.commit()
    return count


def load_google_ads(session: Session, csv_path: Optional[Path] = None) -> int:
    """Load or upsert raw_google_ads from CSV."""
    path = csv_path or _raw_dir() / "google_ads.csv"
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    count = 0
    for _, row in df.iterrows():
        existing = session.exec(
            select(RawGoogleAds).where(
                RawGoogleAds.date == row["date"],
                RawGoogleAds.campaign_id == str(row["campaign_id"]),
            )
        ).first()
        if existing:
            existing.campaign_name = row.get("campaign_name")
            existing.spend = float(row.get("spend", 0))
            existing.impressions = int(row["impressions"]) if pd.notna(row.get("impressions")) else None
            existing.clicks = int(row["clicks"]) if pd.notna(row.get("clicks")) else None
        else:
            session.add(
                RawGoogleAds(
                    date=row["date"],
                    campaign_id=str(row["campaign_id"]),
                    campaign_name=row.get("campaign_name"),
                    spend=float(row.get("spend", 0)),
                    impressions=int(row["impressions"]) if pd.notna(row.get("impressions")) else None,
                    clicks=int(row["clicks"]) if pd.notna(row.get("clicks")) else None,
                )
            )
        count += 1
    session.commit()
    return count


def load_bing_ads(session: Session, csv_path: Optional[Path] = None) -> int:
    """Load or upsert raw_bing_ads from CSV."""
    path = csv_path or _raw_dir() / "bing_ads.csv"
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    count = 0
    for _, row in df.iterrows():
        existing = session.exec(
            select(RawBingAds).where(
                RawBingAds.date == row["date"],
                RawBingAds.campaign_id == str(row["campaign_id"]),
            )
        ).first()
        if existing:
            existing.campaign_name = row.get("campaign_name")
            existing.spend = float(row.get("spend", 0))
            existing.impressions = int(row["impressions"]) if pd.notna(row.get("impressions")) else None
            existing.clicks = int(row["clicks"]) if pd.notna(row.get("clicks")) else None
        else:
            session.add(
                RawBingAds(
                    date=row["date"],
                    campaign_id=str(row["campaign_id"]),
                    campaign_name=row.get("campaign_name"),
                    spend=float(row.get("spend", 0)),
                    impressions=int(row["impressions"]) if pd.notna(row.get("impressions")) else None,
                    clicks=int(row["clicks"]) if pd.notna(row.get("clicks")) else None,
                )
            )
        count += 1
    session.commit()
    return count


def load_pinterest_ads(session: Session, csv_path: Optional[Path] = None) -> int:
    """Load or upsert raw_pinterest_ads from CSV."""
    path = csv_path or _raw_dir() / "pinterest_ads.csv"
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    count = 0
    for _, row in df.iterrows():
        existing = session.exec(
            select(RawPinterestAds).where(
                RawPinterestAds.date == row["date"],
                RawPinterestAds.campaign_id == str(row["campaign_id"]),
            )
        ).first()
        if existing:
            existing.campaign_name = row.get("campaign_name")
            existing.spend = float(row.get("spend", 0))
            existing.impressions = int(row["impressions"]) if pd.notna(row.get("impressions")) else None
            existing.clicks = int(row["clicks"]) if pd.notna(row.get("clicks")) else None
        else:
            session.add(
                RawPinterestAds(
                    date=row["date"],
                    campaign_id=str(row["campaign_id"]),
                    campaign_name=row.get("campaign_name"),
                    spend=float(row.get("spend", 0)),
                    impressions=int(row["impressions"]) if pd.notna(row.get("impressions")) else None,
                    clicks=int(row["clicks"]) if pd.notna(row.get("clicks")) else None,
                )
            )
        count += 1
    session.commit()
    return count


def _safe_float(x, default=0.0):
    try:
        return float(x) if x is not None and pd.notna(x) else default
    except (TypeError, ValueError):
        return default


def load_shopify_orders(session: Session, csv_path: Optional[Path] = None) -> int:
    """Load or upsert raw_shopify_orders from CSV. Sets net_revenue to total_price or revenue on ingest (reconciled later)."""
    path = csv_path or _raw_dir() / "shopify_orders.csv"
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    count = 0
    for _, row in df.iterrows():
        existing = session.exec(
            select(RawShopifyOrders).where(RawShopifyOrders.order_id == str(row["order_id"]))
        ).first()
        total_price = _safe_float(row.get("total_price"), row.get("revenue", 0))
        if existing:
            existing.order_date = row["order_date"]
            existing.revenue = _safe_float(row.get("revenue", 0))
            existing.is_new_customer = (
                bool(row["is_new_customer"]) if pd.notna(row.get("is_new_customer")) else None
            )
            existing.name = str(row["name"]) if pd.notna(row.get("name")) else existing.name
            existing.closed_at = pd.to_datetime(row["closed_at"]) if pd.notna(row.get("closed_at")) else existing.closed_at
            existing.cancelled_at = pd.to_datetime(row["cancelled_at"]) if pd.notna(row.get("cancelled_at")) else existing.cancelled_at
            existing.financial_status = str(row["financial_status"]) if pd.notna(row.get("financial_status")) else existing.financial_status
            existing.total_price = _safe_float(row.get("total_price")) or existing.total_price
            existing.subtotal_price = _safe_float(row.get("subtotal_price")) if pd.notna(row.get("subtotal_price")) else existing.subtotal_price
            existing.total_tax = _safe_float(row.get("total_tax")) if pd.notna(row.get("total_tax")) else existing.total_tax
            existing.currency = str(row["currency"]) if pd.notna(row.get("currency")) else existing.currency
            existing.source_name = str(row["source_name"]) if pd.notna(row.get("source_name")) else existing.source_name
            existing.customer_id = int(row["customer_id"]) if pd.notna(row.get("customer_id")) and str(row.get("customer_id")).isdigit() else existing.customer_id
            existing.is_test = bool(row.get("is_test", False)) if pd.notna(row.get("is_test")) else existing.is_test
            existing.net_revenue = _safe_float(row.get("net_revenue")) or total_price
            existing.click_id = str(row["click_id"]) if pd.notna(row.get("click_id")) and str(row.get("click_id")).strip() else existing.click_id
            existing.utm_source = str(row["utm_source"]) if pd.notna(row.get("utm_source")) else existing.utm_source
            existing.utm_medium = str(row["utm_medium"]) if pd.notna(row.get("utm_medium")) else existing.utm_medium
            existing.utm_campaign = str(row["utm_campaign"]) if pd.notna(row.get("utm_campaign")) else existing.utm_campaign
        else:
            session.add(
                RawShopifyOrders(
                    order_id=str(row["order_id"]),
                    name=str(row["name"]) if pd.notna(row.get("name")) else None,
                    order_date=row["order_date"],
                    revenue=_safe_float(row.get("revenue", 0)),
                    is_new_customer=(
                        bool(row["is_new_customer"]) if pd.notna(row.get("is_new_customer")) else None
                    ),
                    closed_at=pd.to_datetime(row["closed_at"]) if pd.notna(row.get("closed_at")) else None,
                    cancelled_at=pd.to_datetime(row["cancelled_at"]) if pd.notna(row.get("cancelled_at")) else None,
                    total_price=_safe_float(row.get("total_price")) if pd.notna(row.get("total_price")) else None,
                    subtotal_price=_safe_float(row.get("subtotal_price")) if pd.notna(row.get("subtotal_price")) else None,
                    total_tax=_safe_float(row.get("total_tax")) if pd.notna(row.get("total_tax")) else None,
                    currency=str(row["currency"]) if pd.notna(row.get("currency")) else None,
                    source_name=str(row["source_name"]) if pd.notna(row.get("source_name")) else None,
                    financial_status=str(row["financial_status"]) if pd.notna(row.get("financial_status")) else None,
                    customer_id=int(row["customer_id"]) if pd.notna(row.get("customer_id")) and str(row.get("customer_id")).replace("-", "").isdigit() else None,
                    is_test=bool(row.get("is_test", False)) if pd.notna(row.get("is_test")) else False,
                    net_revenue=_safe_float(row.get("net_revenue")) if pd.notna(row.get("net_revenue")) else total_price,
                    click_id=str(row["click_id"]) if pd.notna(row.get("click_id")) and str(row.get("click_id")).strip() else None,
                    utm_source=str(row["utm_source"]) if pd.notna(row.get("utm_source")) else None,
                    utm_medium=str(row["utm_medium"]) if pd.notna(row.get("utm_medium")) else None,
                    utm_campaign=str(row["utm_campaign"]) if pd.notna(row.get("utm_campaign")) else None,
                )
            )
        count += 1
    session.commit()
    return count


def load_shopify_transactions(session: Session, csv_path: Optional[Path] = None) -> int:
    """Load raw_shopify_transactions from CSV. CSV order_id is Shopify order_id (string); resolved to raw_shopify_orders.id."""
    path = csv_path or _raw_dir() / "shopify_transactions.csv"
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    count = 0
    for _, row in df.iterrows():
        order = session.exec(
            select(RawShopifyOrders).where(RawShopifyOrders.order_id == str(row["order_id"]))
        ).first()
        if not order or order.id is None:
            continue
        session.add(
            RawShopifyTransactions(
                order_id=order.id,
                kind=str(row["kind"]),
                status=str(row["status"]) if pd.notna(row.get("status")) else None,
                amount=_safe_float(row.get("amount")),
                currency=str(row["currency"]) if pd.notna(row.get("currency")) else None,
                created_at=pd.to_datetime(row["created_at"]) if pd.notna(row.get("created_at")) else None,
                gateway=str(row["gateway"]) if pd.notna(row.get("gateway")) else None,
                parent_id=int(row["parent_id"]) if pd.notna(row.get("parent_id")) and str(row.get("parent_id")).isdigit() else None,
                source_name=str(row["source_name"]) if pd.notna(row.get("source_name")) else None,
            )
        )
        count += 1
    session.commit()
    return count


def reconcile_orders(session: Session) -> int:
    """
    Compute net_revenue per order from transactions (sum(sale) - sum(refund)), write to raw_shopify_orders.net_revenue,
    and insert ingest_audit rows. Returns number of orders reconciled.
    """
    orders = list(session.exec(select(RawShopifyOrders)).all())
    reconciled = 0
    for order in orders:
        if order.id is None:
            continue
        tx_list = list(
            session.exec(
                select(RawShopifyTransactions).where(RawShopifyTransactions.order_id == order.id)
            ).all()
        )
        sales = sum(
            (t.amount or 0) for t in tx_list
            if t.kind in ("sale", "capture") and (t.status or "").lower() == "success"
        )
        refunds = sum(
            abs(t.amount or 0) for t in tx_list
            if t.kind == "refund" and (t.status or "").lower() == "success"
        )
        computed_net = sales - refunds
        if not tx_list and (order.financial_status or "").lower() in ("refunded", "voided"):
            computed_net = 0.0
        if order.cancelled_at is not None:
            computed_net = 0.0
        prev_net = order.net_revenue
        order.net_revenue = computed_net if tx_list else (order.net_revenue or order.total_price or order.revenue)
        diff = (order.net_revenue or 0) - (prev_net or 0)
        note = "refunds" if refunds else ("cancelled" if order.cancelled_at else "tx_reconcile")
        if not tx_list:
            note = "no_transactions"
        session.add(
            IngestAudit(
                order_id=order.order_id,
                computed_net_revenue=order.net_revenue,
                diff=diff if prev_net is not None else None,
                note=note,
            )
        )
        reconciled += 1
    session.commit()
    return reconciled


def load_woocommerce_orders(session: Session, csv_path: Optional[Path] = None) -> int:
    """Load or upsert raw_woocommerce_orders from CSV. Sets net_revenue to revenue if not provided."""
    path = csv_path or _raw_dir() / "woocommerce_orders.csv"
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    count = 0
    for _, row in df.iterrows():
        existing = session.exec(
            select(RawWooCommerceOrders).where(RawWooCommerceOrders.order_id == str(row["order_id"]))
        ).first()
        rev = _safe_float(row.get("revenue", 0))
        net = _safe_float(row.get("net_revenue")) if pd.notna(row.get("net_revenue")) else rev
        if existing:
            existing.order_date = row["order_date"]
            existing.revenue = rev
            existing.is_new_customer = (
                bool(row["is_new_customer"]) if pd.notna(row.get("is_new_customer")) else None
            )
            existing.name = str(row["name"]) if pd.notna(row.get("name")) else existing.name
            existing.net_revenue = net
            existing.click_id = str(row["click_id"]) if pd.notna(row.get("click_id")) and str(row.get("click_id")).strip() else existing.click_id
            existing.utm_source = str(row["utm_source"]) if pd.notna(row.get("utm_source")) else existing.utm_source
            existing.utm_medium = str(row["utm_medium"]) if pd.notna(row.get("utm_medium")) else existing.utm_medium
            existing.utm_campaign = str(row["utm_campaign"]) if pd.notna(row.get("utm_campaign")) else existing.utm_campaign
        else:
            session.add(
                RawWooCommerceOrders(
                    order_id=str(row["order_id"]),
                    name=str(row["name"]) if pd.notna(row.get("name")) else None,
                    order_date=row["order_date"],
                    revenue=rev,
                    is_new_customer=(
                        bool(row["is_new_customer"]) if pd.notna(row.get("is_new_customer")) else None
                    ),
                    net_revenue=net,
                    click_id=str(row["click_id"]) if pd.notna(row.get("click_id")) and str(row.get("click_id")).strip() else None,
                    utm_source=str(row["utm_source"]) if pd.notna(row.get("utm_source")) else None,
                    utm_medium=str(row["utm_medium"]) if pd.notna(row.get("utm_medium")) else None,
                    utm_campaign=str(row["utm_campaign"]) if pd.notna(row.get("utm_campaign")) else None,
                )
            )
        count += 1
    session.commit()
    return count


def load_ad_clicks(session: Session, csv_path: Optional[Path] = None) -> int:
    """Load raw_ad_clicks from CSV (click_id, date, campaign_id, campaign_name, channel). Replaces existing rows for same click_id."""
    path = csv_path or _raw_dir() / "ad_clicks.csv"
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    count = 0
    for _, row in df.iterrows():
        session.add(
            RawAdClicks(
                click_id=str(row["click_id"]),
                date=row["date"],
                campaign_id=str(row["campaign_id"]),
                campaign_name=str(row["campaign_name"]) if pd.notna(row.get("campaign_name")) else None,
                channel=str(row["channel"]).lower(),
            )
        )
        count += 1
    session.commit()
    return count


def run_ingest(session: Session, data_dir: Optional[Path] = None) -> dict:
    """Run full ingest from data/raw (or data_dir): ads, orders, transactions, then reconcile_orders. Returns counts."""
    if data_dir is not None:
        os.environ["DATA_RAW_DIR"] = str(data_dir)
    counts = {}
    counts["meta_ads"] = load_meta_ads(session)
    counts["google_ads"] = load_google_ads(session)
    counts["bing_ads"] = load_bing_ads(session)
    counts["pinterest_ads"] = load_pinterest_ads(session)
    counts["shopify_orders"] = load_shopify_orders(session)
    counts["shopify_transactions"] = load_shopify_transactions(session)
    counts["woocommerce_orders"] = load_woocommerce_orders(session)
    counts["ad_clicks"] = load_ad_clicks(session)
    counts["reconciled_orders"] = reconcile_orders(session)
    return counts
