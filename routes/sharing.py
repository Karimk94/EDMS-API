from fastapi import APIRouter, HTTPException, Header, Request
import db_connector
from database import sharing as sharing_db
from database import documents as documents_db
from schemas.sharing import ShareLinkCreateRequest, ShareAccessRequest, ShareVerifyRequest
import logging
import os
import random

from utils.common import send_otp_email

router = APIRouter()
ALLOWED_DOMAIN = "@rta.ae"

@router.post('/api/share/generate')
async def generate_share_link(request: Request, req: ShareLinkCreateRequest):
    """
    Generates a shareable link for a document.
    """
    try:
        try:

            user = request.session.get('user')

            if not user:
                return HTTPException(status_code=401, detail="Unauthorized")

            user_info = await db_connector.get_user_details(user['username'])

            if not user_info:
                raise HTTPException(status_code=401, detail="Invalid session")

            username = user_info.get('username') or user_info.get('user_id')

            if not username:
                raise HTTPException(status_code=401, detail="Could not resolve user from session")

            user_id = await sharing_db.get_system_id_by_username(username)

            if not user_id:
                raise HTTPException(status_code=403, detail="User profile not found in EDMS database")

        except Exception as e:
            logging.error(f"Token resolution failed: {e}")
            raise HTTPException(status_code=401, detail="Session validation failed")

        # 3. Create Share Link
        token = await sharing_db.create_share_link(req.document_id, user_id, req.expiry_date)

        base_url = os.getenv('FRONTEND_URL', 'http://localhost:3000')
        link = f"{base_url}/shared/{token}"

        return {
            "token": token,
            "link": link,
            "expiry_date": req.expiry_date
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Share generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post('/api/share/request-access/{token}')
async def request_access_otp(token: str, req: ShareAccessRequest):
    """
    Step 1: Validates domain and sends OTP using Database for storage.
    """
    try:
        # Check domain
        if not req.viewer_email.lower().endswith(ALLOWED_DOMAIN):
            raise HTTPException(status_code=400, detail=f"Access restricted to {ALLOWED_DOMAIN} emails only.")

        # Verify Link Exists
        share_info = await sharing_db.get_share_details(token)
        if not share_info:
            raise HTTPException(status_code=404, detail="Link is invalid or expired")

        # Generate OTP
        otp = str(random.randint(100000, 999999))

        # Store OTP in Database
        saved = await sharing_db.save_otp(token, req.viewer_email, otp)
        if not saved:
            raise HTTPException(status_code=500, detail="Failed to generate OTP. Database unavailable.")

        # Send Email via SMTP
        send_otp_email(req.viewer_email, otp)

        return {"message": "OTP sent to your email.", "email": req.viewer_email}

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"OTP request error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post('/api/share/verify-access/{token}')
async def verify_access_otp(token: str, req: ShareVerifyRequest):
    """
    Step 2: Verifies OTP via Database.
    """
    try:
        # Verify and Consume OTP in one go
        is_valid = await sharing_db.verify_otp(token, req.viewer_email, req.otp)

        if not is_valid:
            # Note: This generic message covers expired, used, or wrong OTPs for security
            raise HTTPException(status_code=400, detail="Invalid or expired OTP.")

        share_info = await sharing_db.get_share_details(token)
        if not share_info:
            raise HTTPException(status_code=404, detail="Link is invalid or expired")

        await sharing_db.log_share_access(share_info['share_id'], req.viewer_email)

        document = await documents_db.get_document_by_id(share_info['document_id'])
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")

        stats = await sharing_db.get_access_stats(token)

        return {
            "document": document,
            "shared_by": share_info['created_by'],
            "access_stats": stats
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"OTP verification error: {e}")
        raise HTTPException(status_code=500, detail=str(e))