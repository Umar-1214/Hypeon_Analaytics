"""Click-ID attribution: attribute 100% of order revenue to the channel/campaign that produced the click."""
from datetime import date
from typing import Set, Tuple

from sqlmodel import Session, select

from packages.shared.src.models import (
    AttributionEvent,
    RawAdClicks,
    RawShopifyOrders,
    RawWooCommerceOrders,
)


def run_click_id_attribution(
    session: Session,
    run_id: str,
    start_date: date,
    end_date: date,
) -> Tuple[int, Set[str]]:
    """
    For each order (Shopify + WooCommerce) in [start_date, end_date] that has a non-null click_id,
    look up the click in raw_ad_clicks and attribute 100% of (net_revenue or revenue) to that channel/campaign.
    Writes to attribution_events with the given run_id.
    Returns (number of attribution rows written, set of order_ids that were attributed).
    """
    attributed_order_ids: Set[str] = set()
    # Load all ad clicks by click_id for lookup
    clicks = {
        r.click_id: (r.channel, r.campaign_id, r.campaign_name)
        for r in session.exec(
            select(RawAdClicks).where(
                RawAdClicks.date >= start_date,
                RawAdClicks.date <= end_date,
            )
        ).all()
    }
    if not clicks:
        return 0, attributed_order_ids

    count = 0
    # Shopify orders with click_id
    for order in session.exec(
        select(RawShopifyOrders).where(
            RawShopifyOrders.order_date >= start_date,
            RawShopifyOrders.order_date <= end_date,
        )
    ).all():
        if not order.click_id or not str(order.click_id).strip():
            continue
        click_info = clicks.get(order.click_id.strip())
        if not click_info:
            continue
        channel, campaign_id, campaign_name = click_info
        revenue = float(order.net_revenue if order.net_revenue is not None else order.revenue)
        if revenue <= 0:
            continue
        session.add(
            AttributionEvent(
                order_id=order.order_id,
                channel=channel,
                campaign_id=campaign_id,
                cost_center=campaign_name,
                weight=1.0,
                allocated_revenue=revenue,
                event_date=order.order_date,
                run_id=run_id,
            )
        )
        attributed_order_ids.add(order.order_id)
        count += 1

    # WooCommerce orders with click_id
    for order in session.exec(
        select(RawWooCommerceOrders).where(
            RawWooCommerceOrders.order_date >= start_date,
            RawWooCommerceOrders.order_date <= end_date,
        )
    ).all():
        if not order.click_id or not str(order.click_id).strip():
            continue
        click_info = clicks.get(order.click_id.strip())
        if not click_info:
            continue
        channel, campaign_id, campaign_name = click_info
        revenue = float(order.net_revenue if order.net_revenue is not None else order.revenue)
        if revenue <= 0:
            continue
        session.add(
            AttributionEvent(
                order_id=order.order_id,
                channel=channel,
                campaign_id=campaign_id,
                cost_center=campaign_name,
                weight=1.0,
                allocated_revenue=revenue,
                event_date=order.order_date,
                run_id=run_id,
            )
        )
        attributed_order_ids.add(order.order_id)
        count += 1

    if count > 0:
        session.commit()
    return count, attributed_order_ids
