"""
Firestore reads: users/{uid}, organizations/{organization_id}.
Used to resolve organization_id and role from authenticated user.
Supports Option B schema: organizations with "projects" (bq_project + datasets per project).
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

# Optional dataset type for resolving which BQ dataset to use (marts, marts_ads, analytics, ga4, ads)
DATASET_TYPES = ("marts", "marts_ads", "analytics", "ga4", "ads")


def _get_firestore():
    """Return Firestore client if Firebase is initialized. Uses FIRESTORE_DATABASE_ID if set (e.g. hypeon-analytics)."""
    try:
        import os
        from .firebase import is_initialized
        if not is_initialized():
            return None
        from firebase_admin import firestore
        database_id = os.environ.get("FIRESTORE_DATABASE_ID")
        if database_id:
            return firestore.client(database_id=database_id)
        return firestore.client()
    except Exception as e:
        logger.debug("Firestore client unavailable: %s", e)
        return None


def get_user(uid: str) -> Optional[dict[str, Any]]:
    """
    Read users/{uid} from Firestore.
    Expected fields: email, displayName, organization_id, role (optional).
    """
    db = _get_firestore()
    if not db:
        return None
    try:
        doc = db.collection("users").document(uid).get()
        if doc.exists:
            return doc.to_dict()
        return None
    except Exception as e:
        logger.warning("Firestore get_user(%s) failed: %s", uid, e)
        return None


def get_organization(organization_id: str) -> Optional[dict[str, Any]]:
    """
    Read organizations/{organization_id} from Firestore.
    Expected fields: name, ad_channels (or datasets) for client/dataset config.
    Option B: "projects" array of { bq_project, datasets: [ { bq_dataset, bq_location, type? } ] }.
    """
    db = _get_firestore()
    if not db:
        return None
    try:
        doc = db.collection("organizations").document(organization_id).get()
        if doc.exists:
            return doc.to_dict()
        return None
    except Exception as e:
        logger.warning("Firestore get_organization(%s) failed: %s", organization_id, e)
        return None


def parse_org_projects(org_doc: Optional[dict[str, Any]]) -> List[dict[str, Any]]:
    """
    Return Option B "projects" array from org doc, or empty list.
    Each project: { "bq_project": str, "project_type"?: "organization"|"individual", "datasets": [...] }.
    """
    if not org_doc or not isinstance(org_doc.get("projects"), list):
        return []
    out = []
    for p in org_doc["projects"]:
        if not isinstance(p, dict) or not p.get("bq_project"):
            continue
        datasets = []
        for d in p.get("datasets") or []:
            if isinstance(d, dict) and d.get("bq_dataset"):
                datasets.append({
                    "bq_dataset": str(d["bq_dataset"]),
                    "bq_location": str(d.get("bq_location") or ""),
                    "type": d.get("type") if d.get("type") in DATASET_TYPES else None,
                })
        if datasets:
            entry: dict[str, Any] = {"bq_project": str(p["bq_project"]), "datasets": datasets}
            if p.get("project_type") in ("organization", "individual"):
                entry["project_type"] = p["project_type"]
            out.append(entry)
    return out


def get_org_projects_flat(org_doc: Optional[dict[str, Any]]) -> List[dict[str, Any]]:
    """
    Flatten Option B projects into a list of dataset configs with client_id (1-based index).
    Each item: { client_id, bq_project, bq_dataset, bq_location, description, type?, project_type? }.
    Used by /api/v1/me and by BQ resolution. If org has no "projects", returns empty list.
    """
    projects = parse_org_projects(org_doc)
    if not projects:
        return []
    flat = []
    for idx, proj in enumerate(projects):
        bq_project = proj.get("bq_project") or ""
        project_type = proj.get("project_type") if proj.get("project_type") in ("organization", "individual") else None
        for ds in proj.get("datasets") or []:
            client_id = len(flat) + 1
            bq_dataset = ds.get("bq_dataset") or ""
            bq_location = ds.get("bq_location") or ""
            ds_type = ds.get("type")
            description = bq_dataset or f"Dataset {client_id}"
            if bq_project:
                description = f"{bq_dataset} ({bq_project})"
            item: dict[str, Any] = {
                "client_id": client_id,
                "bq_project": bq_project,
                "bq_dataset": bq_dataset,
                "bq_location": bq_location,
                "description": description,
                "type": ds_type,
            }
            if project_type:
                item["project_type"] = project_type
            flat.append(item)
    return flat


def get_bq_config_for_client(organization_id: str, client_id: Optional[int]) -> Optional[dict[str, Any]]:
    """
    For an org using Option B (projects), return BQ config for the given client_id (1-based index).
    Returns { bq_project, bq_dataset, bq_location, type? } or None if not found or org has no projects.
    """
    org_doc = get_organization(organization_id)
    flat = get_org_projects_flat(org_doc)
    if not flat or client_id is None:
        return None
    for item in flat:
        if item.get("client_id") == int(client_id):
            return {
                "bq_project": item.get("bq_project"),
                "bq_dataset": item.get("bq_dataset"),
                "bq_location": item.get("bq_location"),
                "type": item.get("type"),
            }
    return None


def get_org_bq_context(organization_id: str) -> Optional[dict[str, Any]]:
    """
    Build a BigQuery context from the organization's Firestore config (Option B projects).
    Returns dict with: bq_project, bq_source_project, marts_dataset, marts_ads_dataset,
    ga4_dataset, ads_dataset, bq_location, bq_location_ads.
    Uses first dataset of each type (marts, marts_ads, ga4, ads). If no type, first dataset is treated as marts.
    Returns None if org has no projects (callers must not use shared env; show "datasets not configured" instead).
    """
    org_doc = get_organization(organization_id)
    flat = get_org_projects_flat(org_doc)
    if not flat:
        return None

    first_project = flat[0].get("bq_project") or ""
    first_location = flat[0].get("bq_location") or "europe-north2"
    ctx: dict[str, Any] = {
        "bq_project": first_project,
        "bq_source_project": first_project,
        "marts_dataset": "",
        "marts_ads_dataset": "",
        "ga4_dataset": "",
        "ads_dataset": "",
        "bq_location": first_location,
        "bq_location_ads": "EU",
    }
    for item in flat:
        proj = (item.get("bq_project") or "").strip()
        ds = (item.get("bq_dataset") or "").strip()
        loc = (item.get("bq_location") or "").strip()
        dtype = item.get("type")
        if not ds:
            continue
        if dtype == "marts" and not ctx["marts_dataset"]:
            ctx["marts_dataset"] = ds
            if loc:
                ctx["bq_location"] = loc
        elif dtype == "marts_ads" and not ctx["marts_ads_dataset"]:
            ctx["marts_ads_dataset"] = ds
            if loc:
                ctx["bq_location_ads"] = loc
        elif dtype == "ga4" and not ctx["ga4_dataset"]:
            ctx["ga4_dataset"] = ds
            if proj:
                ctx["bq_source_project"] = proj
        elif dtype == "ads" and not ctx["ads_dataset"]:
            ctx["ads_dataset"] = ds
            if proj:
                ctx["bq_source_project"] = ctx["bq_source_project"] or proj
        elif not dtype and not ctx["marts_dataset"]:
            ctx["marts_dataset"] = ds
            if loc:
                ctx["bq_location"] = loc
            if proj:
                ctx["bq_project"] = proj

    if not ctx["marts_dataset"] and not ctx["ga4_dataset"] and not ctx["ads_dataset"]:
        first_ds = flat[0].get("bq_dataset") or ""
        if first_ds:
            ctx["marts_dataset"] = first_ds
            ctx["bq_project"] = flat[0].get("bq_project") or ctx["bq_project"]
            ctx["bq_location"] = flat[0].get("bq_location") or ctx["bq_location"]
    return ctx if (ctx["marts_dataset"] or ctx["ga4_dataset"] or ctx["ads_dataset"]) else None
