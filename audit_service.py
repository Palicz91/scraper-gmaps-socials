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
FROM_EMAIL = os.environ.get("FROM_EMAIL", "Review Manager <hello@spinix.so>")

logging.basicConfig(
    filename="/home/hello/scraper/Scraper/audit_service.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Review Audit Service", version="2.0")
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
            "",
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
            f"{result['unanswered']} unanswered ({result['unanswered_pct']}%), "
            f"{result['negative_unanswered']}/{result['negative_total']} negative unanswered"
        )

        # Store all results including negative review data and star distribution
        supabase.table("review_audit_requests").update({
            "status": "completed",
            "reviews_loaded": result["reviews_loaded"],
            "reviews_answered": result["answered"],
            "reviews_unanswered": result["unanswered"],
            "reviews_unanswered_pct": result["unanswered_pct"],
            "negative_total": result["negative_total"],
            "negative_unanswered": result["negative_unanswered"],
            "negative_unanswered_pct": result["negative_unanswered_pct"],
            "est_unanswered": result["est_unanswered"],
            "est_negative_unanswered": result["est_negative_unanswered"],
            "stars_5": result.get("est_stars_5", 0),
            "stars_4": result.get("est_stars_4", 0),
            "stars_3": result.get("est_stars_3", 0),
            "stars_2": result.get("est_stars_2", 0),
            "stars_1": result.get("est_stars_1", 0),
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
    negative_total = result["negative_total"]
    negative_unanswered = result["negative_unanswered"]
    negative_unanswered_pct = result["negative_unanswered_pct"]
    est_unanswered = result.get("est_unanswered", unanswered)
    est_negative = result.get("est_negative_unanswered", negative_unanswered)
    total_on_page = result.get("total_reviews_on_page", total)
    rating = result.get("rating", req.place_rating or "N/A")

    # Star distribution (estimated to total)
    est_s5 = result.get("est_stars_5", 0)
    est_s4 = result.get("est_stars_4", 0)
    est_s3 = result.get("est_stars_3", 0)
    est_s2 = result.get("est_stars_2", 0)
    est_s1 = result.get("est_stars_1", 0)
    star_total = est_s5 + est_s4 + est_s3 + est_s2 + est_s1
    star_max = max(est_s5, est_s4, est_s3, est_s2, est_s1, 1)

    # Severity color logic
    if unanswered_pct >= 50:
        severity_color = "#dc2626"
        severity_bg = "#fef2f2"
        severity_border = "#fecaca"
        severity_label = "Critical"
    elif unanswered_pct >= 25:
        severity_color = "#ea580c"
        severity_bg = "#fff7ed"
        severity_border = "#fed7aa"
        severity_label = "Needs attention"
    else:
        severity_color = "#ca8a04"
        severity_bg = "#fefce8"
        severity_border = "#fef08a"
        severity_label = "Room to improve"

    html = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background: #ffffff;">

      <!-- Header -->
      <div style="text-align: center; margin-bottom: 30px;">
        <div style="display: inline-block; background: #0f172a; border-radius: 12px; padding: 12px 24px; margin-bottom: 16px;">
          <span style="color: #10b981; font-weight: 700; font-size: 18px;">SpiniX</span>
          <span style="color: #94a3b8; font-size: 14px; margin-left: 8px;">Review Audit</span>
        </div>
        <h1 style="color: #1a1a2e; font-size: 22px; margin: 0 0 5px 0;">{req.place_name}</h1>
        <p style="color: #999; font-size: 13px; margin: 0;">{req.place_address}</p>
        {f'<p style="color: #666; font-size: 14px; margin: 8px 0 0 0;">⭐ {rating} ({total_on_page} reviews)</p>' if rating else ''}
      </div>

      <!-- Main stats -->
      <div style="background: #f8f9fa; border-radius: 12px; padding: 24px; margin-bottom: 20px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="text-align: center;">
          <tr>
            <td style="padding: 8px;">
              <div style="font-size: 32px; font-weight: 700; color: #1a1a2e;">{total}</div>
              <div style="font-size: 11px; color: #666; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px;">Analyzed</div>
            </td>
            <td style="padding: 8px;">
              <div style="font-size: 32px; font-weight: 700; color: #10b981;">{answered}</div>
              <div style="font-size: 11px; color: #666; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px;">Answered</div>
            </td>
            <td style="padding: 8px;">
              <div style="font-size: 32px; font-weight: 700; color: #ef4444;">{unanswered}</div>
              <div style="font-size: 11px; color: #666; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px;">Unanswered</div>
            </td>
          </tr>
        </table>
      </div>

      <!-- Star distribution -->
      {"" if star_total == 0 else f'''
      <div style="background: #f8f9fa; border-radius: 12px; padding: 20px; margin-bottom: 20px;">
        <p style="color: #1a1a2e; font-weight: 600; margin: 0 0 14px 0; font-size: 14px;">Rating distribution (estimated)</p>
        <table width="100%" cellpadding="0" cellspacing="0" style="font-size: 13px; color: #444;">
          <tr>
            <td style="padding: 4px 8px 4px 0; width: 30px; text-align: right;">5★</td>
            <td style="padding: 4px 0;"><div style="background: #e5e7eb; border-radius: 4px; height: 16px; width: 100%;"><div style="background: #10b981; border-radius: 4px; height: 16px; width: {round(est_s5/star_max*100)}%;"></div></div></td>
            <td style="padding: 4px 0 4px 8px; width: 40px; text-align: right; font-weight: 600;">{est_s5}</td>
          </tr>
          <tr>
            <td style="padding: 4px 8px 4px 0; width: 30px; text-align: right;">4★</td>
            <td style="padding: 4px 0;"><div style="background: #e5e7eb; border-radius: 4px; height: 16px; width: 100%;"><div style="background: #84cc16; border-radius: 4px; height: 16px; width: {round(est_s4/star_max*100)}%;"></div></div></td>
            <td style="padding: 4px 0 4px 8px; width: 40px; text-align: right; font-weight: 600;">{est_s4}</td>
          </tr>
          <tr>
            <td style="padding: 4px 8px 4px 0; width: 30px; text-align: right;">3★</td>
            <td style="padding: 4px 0;"><div style="background: #e5e7eb; border-radius: 4px; height: 16px; width: 100%;"><div style="background: #eab308; border-radius: 4px; height: 16px; width: {round(est_s3/star_max*100)}%;"></div></div></td>
            <td style="padding: 4px 0 4px 8px; width: 40px; text-align: right; font-weight: 600;">{est_s3}</td>
          </tr>
          <tr>
            <td style="padding: 4px 8px 4px 0; width: 30px; text-align: right;">2★</td>
            <td style="padding: 4px 0;"><div style="background: #e5e7eb; border-radius: 4px; height: 16px; width: 100%;"><div style="background: #f97316; border-radius: 4px; height: 16px; width: {round(est_s2/star_max*100)}%;"></div></div></td>
            <td style="padding: 4px 0 4px 8px; width: 40px; text-align: right; font-weight: 600;">{est_s2}</td>
          </tr>
          <tr>
            <td style="padding: 4px 8px 4px 0; width: 30px; text-align: right;">1★</td>
            <td style="padding: 4px 0;"><div style="background: #e5e7eb; border-radius: 4px; height: 16px; width: 100%;"><div style="background: #ef4444; border-radius: 4px; height: 16px; width: {round(est_s1/star_max*100)}%;"></div></div></td>
            <td style="padding: 4px 0 4px 8px; width: 40px; text-align: right; font-weight: 600;">{est_s1}</td>
          </tr>
        </table>
      </div>
      '''}

      <!-- Unanswered alert -->
      <div style="background: {severity_bg}; border: 1px solid {severity_border}; border-radius: 12px; padding: 20px; margin-bottom: 16px;">
        <p style="color: {severity_color}; font-weight: 600; margin: 0 0 4px 0; font-size: 15px;">
          {unanswered_pct}% of reviews have no reply
        </p>
        <p style="color: #666; margin: 0; font-size: 13px;">
          {severity_label}
          {f' — estimated {est_unanswered} unanswered across all {total_on_page} reviews' if est_unanswered > unanswered else ''}
        </p>
      </div>

      <!-- Negative review alert -->
      {"" if negative_total == 0 else f'''
      <div style="background: #fef2f2; border: 1px solid #fecaca; border-radius: 12px; padding: 20px; margin-bottom: 20px;">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td style="vertical-align: top; padding-right: 16px;">
              <span style="font-size: 28px;">⚠️</span>
            </td>
            <td>
              <p style="color: #dc2626; font-weight: 600; margin: 0 0 6px 0; font-size: 15px;">
                {negative_unanswered} of {negative_total} negative reviews have no reply
              </p>
              <p style="color: #666; margin: 0; font-size: 13px; line-height: 1.5;">
                Unanswered 1-2 star reviews are the #1 reason potential guests choose a competitor.
                {"" if negative_unanswered_pct < 50 else " More than half of your negative reviews are sitting unanswered right now."}
                {f" That could mean ~{est_negative} negative reviews without a response across your full review history." if est_negative > negative_unanswered else ""}
              </p>
            </td>
          </tr>
        </table>
      </div>
      '''}

      <!-- What this means -->
      <div style="background: #f0f9ff; border: 1px solid #bae6fd; border-radius: 12px; padding: 20px; margin-bottom: 20px;">
        <p style="color: #0369a1; font-weight: 600; margin: 0 0 8px 0; font-size: 15px;">
          What this means for your business
        </p>
        <p style="color: #555; margin: 0; font-size: 13px; line-height: 1.6;">
          Google prioritizes businesses that respond to reviews.
          Responding to just your negative reviews alone could improve your visibility in local search results.
          Guests who see owner replies are 1.7x more likely to visit.
        </p>
      </div>

      <!-- CTA -->
      <div style="background: #ecfdf5; border: 1px solid #a7f3d0; border-radius: 12px; padding: 24px; margin-bottom: 20px; text-align: center;">
        <p style="color: #059669; font-weight: 600; margin: 0 0 8px 0; font-size: 16px;">
          We'll draft replies to your first 10 reviews for free
        </p>
        <p style="color: #666; margin: 0 0 20px 0; font-size: 13px; line-height: 1.5;">
          AI drafts in your tone and language. You approve each one before it goes live.<br>
          No commitment. No credit card.
        </p>
        <a href="https://spinix.so/review-manager?utm_source=audit&utm_medium=email&utm_campaign=lead_magnet"
           style="display: inline-block; background: #10b981; color: white; padding: 14px 32px;
                  border-radius: 8px; text-decoration: none; font-weight: 600; font-size: 14px;">
          Start for free →
        </a>
      </div>

      <!-- Footer -->
      <div style="text-align: center; padding-top: 20px; border-top: 1px solid #eee;">
        <p style="color: #999; font-size: 11px; margin: 0;">
          Sent by <a href="https://spinix.so" style="color: #10b981; text-decoration: none;">SpiniX</a> Review Manager
        </p>
        <p style="color: #bbb; font-size: 10px; margin: 8px 0 0 0;">
          This is a one-time audit report you requested. No further emails will be sent.
        </p>
      </div>
    </div>
    """

    subject = f"Review Audit: {req.place_name}"
    if negative_unanswered > 0:
        subject += f" — {negative_unanswered} negative reviews without a reply"
    elif unanswered > 0:
        subject += f" — {unanswered} unanswered reviews found"

    resend.Emails.send({
        "from": FROM_EMAIL,
        "to": [req.email],
        "subject": subject,
        "html": html,
    })
