"""
Review Audit API Service
Receives audit requests from Supabase, scrapes review data, sends report email.

Usage:
  uvicorn audit_service:app --host 0.0.0.0 --port 8000

Endpoints:
  POST /audit  - Trigger audit for a place (called by Supabase webhook)
  GET  /health - Health check
"""

import os
import asyncio
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks, Header
from pydantic import BaseModel
from typing import Optional

import resend
from supabase import create_client

from audit_scraper import run_single_place_audit

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
AUDIT_SECRET = os.environ.get("AUDIT_SECRET", "change-me-in-production")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "Review Manager <reviews@spinix.so>")

logging.basicConfig(
    filename="/home/hello/scraper/Scraper/audit_service.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Review Audit Service", version="1.0")
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
resend.api_key = RESEND_API_KEY


class AuditRequest(BaseModel):
    id: str
    email: str
    place_id: str
    place_name: str
    place_address: Optional[str] = ""
    place_rating: Optional[float] = None
    place_review_count: Optional[int] = None


class WebhookPayload(BaseModel):
    type: str = "INSERT"
    table: str = "review_audit_requests"
    record: dict = {}
    old_record: Optional[dict] = None


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.post("/audit")
async def trigger_audit(
    payload: WebhookPayload,
    background_tasks: BackgroundTasks,
    authorization: Optional[str] = Header(None),
):
    if authorization != f"Bearer {AUDIT_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    record = payload.record
    if not record.get("email") or not record.get("place_id"):
        raise HTTPException(status_code=400, detail="Missing email or place_id")

    if record.get("status") != "pending":
        return {"status": "skipped", "reason": "not pending"}

    audit_req = AuditRequest(
        id=record["id"],
        email=record["email"],
        place_id=record["place_id"],
        place_name=record.get("place_name", ""),
        place_address=record.get("place_address", ""),
        place_rating=record.get("place_rating"),
        place_review_count=record.get("place_review_count"),
    )

    supabase.table("review_audit_requests").update(
        {"status": "processing"}
    ).eq("id", audit_req.id).execute()

    background_tasks.add_task(process_audit, audit_req)

    logger.info(f"Audit queued: {audit_req.place_name} ({audit_req.place_id}) for {audit_req.email}")
    return {"status": "queued", "place": audit_req.place_name}


@app.post("/audit/manual")
async def trigger_audit_manual(
    req: AuditRequest,
    background_tasks: BackgroundTasks,
    authorization: Optional[str] = Header(None),
):
    if authorization != f"Bearer {AUDIT_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    background_tasks.add_task(process_audit, req)
    return {"status": "queued", "place": req.place_name}


async def process_audit(req: AuditRequest):
    try:
        logger.info(f"Starting audit: {req.place_name} for {req.email}")

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            run_single_place_audit,
            "",                  # maps_url (not used, we search by name)
            req.place_id,
            req.place_name,
            req.place_address,
        )

        if not result:
            logger.error(f"Scraper returned no result for {req.place_name}")
            supabase.table("review_audit_requests").update(
                {"status": "failed"}
            ).eq("id", req.id).execute()
            return

        logger.info(
            f"Audit complete: {req.place_name} - "
            f"{result['reviews_loaded']} loaded, "
            f"{result['unanswered']} unanswered, "
            f"{result['unanswered_pct']}%"
        )

        supabase.table("review_audit_requests").update({
            "status": "completed",
            "reviews_loaded": result["reviews_loaded"],
            "reviews_answered": result["answered"],
            "reviews_unanswered": result["unanswered"],
            "reviews_unanswered_pct": result["unanswered_pct"],
            "completed_at": datetime.utcnow().isoformat(),
        }).eq("id", req.id).execute()

        send_audit_email(req, result)

        logger.info(f"Audit email sent to {req.email} for {req.place_name}")

    except Exception as e:
        logger.error(f"Audit failed for {req.place_name}: {e}", exc_info=True)
        try:
            supabase.table("review_audit_requests").update(
                {"status": "failed"}
            ).eq("id", req.id).execute()
        except:
            pass


def send_audit_email(req: AuditRequest, result: dict):
    total = result["reviews_loaded"]
    unanswered = result["unanswered"]
    answered = result["answered"]
    unanswered_pct = result["unanswered_pct"]

    html = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
      <div style="text-align: center; margin-bottom: 30px;">
        <h1 style="color: #1a1a2e; font-size: 24px; margin-bottom: 5px;">Your Review Audit Report</h1>
        <p style="color: #666; font-size: 14px;">{req.place_name}</p>
        <p style="color: #999; font-size: 12px;">{req.place_address}</p>
      </div>

      <div style="background: #f8f9fa; border-radius: 12px; padding: 24px; margin-bottom: 20px;">
        <div style="display: flex; justify-content: space-around; text-align: center;">
          <div>
            <div style="font-size: 32px; font-weight: 700; color: #1a1a2e;">{total}</div>
            <div style="font-size: 12px; color: #666; margin-top: 4px;">Reviews analyzed</div>
          </div>
          <div>
            <div style="font-size: 32px; font-weight: 700; color: #10b981;">{answered}</div>
            <div style="font-size: 12px; color: #666; margin-top: 4px;">Answered</div>
          </div>
          <div>
            <div style="font-size: 32px; font-weight: 700; color: #ef4444;">{unanswered}</div>
            <div style="font-size: 12px; color: #666; margin-top: 4px;">Unanswered</div>
          </div>
        </div>
      </div>

      <div style="background: #fef2f2; border: 1px solid #fecaca; border-radius: 12px; padding: 20px; margin-bottom: 20px;">
        <p style="color: #dc2626; font-weight: 600; margin: 0 0 8px 0; font-size: 16px;">
          {unanswered_pct}% of your reviews have no reply
        </p>
        <p style="color: #666; margin: 0; font-size: 14px; line-height: 1.5;">
          Every unanswered review, especially negative ones, tells potential guests you don't care.
          Responding to reviews can improve your rating perception and bring back lost customers.
        </p>
      </div>

      <div style="background: #ecfdf5; border: 1px solid #a7f3d0; border-radius: 12px; padding: 20px; margin-bottom: 20px;">
        <p style="color: #059669; font-weight: 600; margin: 0 0 8px 0; font-size: 16px;">
          We'll reply to your first 10 reviews for free
        </p>
        <p style="color: #666; margin: 0 0 16px 0; font-size: 14px; line-height: 1.5;">
          Our AI drafts replies in your tone and language. You approve each one before it goes live.
          No commitment, no credit card needed.
        </p>
        <a href="https://spinix.so/review-manager#pricing-section"
           style="display: inline-block; background: #10b981; color: white; padding: 12px 24px;
                  border-radius: 8px; text-decoration: none; font-weight: 600; font-size: 14px;">
          Get started free
        </a>
      </div>

      <div style="text-align: center; padding-top: 20px; border-top: 1px solid #eee;">
        <p style="color: #999; font-size: 12px;">
          Sent by <a href="https://spinix.so" style="color: #10b981;">SpiniX</a> Review Manager
        </p>
      </div>
    </div>
    """

    resend.Emails.send({
        "from": FROM_EMAIL,
        "to": [req.email],
        "subject": f"Review Audit: {req.place_name} - {unanswered} unanswered reviews found",
        "html": html,
    })
