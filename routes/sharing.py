from fastapi import APIRouter, HTTPException, Header, Request, Query, Depends
import db_connector
from database import sharing as sharing_db
from database import documents as documents_db
from database import folders as folders_db
from schemas.sharing import (
    ShareLinkCreateRequest,
    ShareAccessRequest,
    ShareVerifyRequest,
    SharedFolderContentsRequest,
    SharedDocumentDownloadRequest
)
import logging
import os
import random
from datetime import datetime
from fastapi.responses import StreamingResponse
import io
import wsdl_client

from utils.common import send_otp_email, send_share_link_email
from utils.watermark import apply_watermark_to_image, apply_watermark_to_pdf, apply_watermark_to_video

router = APIRouter()
ALLOWED_DOMAIN = "@rta.ae"

@router.post('/api/share/generate')
async def generate_share_link(request: Request, req: ShareLinkCreateRequest):
    """
    Generates a shareable link for a document or folder.

    Supports two modes:
    1. Open mode (target_email=None): Any @rta.ae email can access
    2. Restricted mode (target_email set): Only the specified email can access

    Supports two share types:
    1. File share (share_type='file'): Shares a single document
    2. Folder share (share_type='folder'): Shares a folder and its contents
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

        # Validate target_email if provided
        target_email = None
        if req.target_email:
            target_email = req.target_email.strip().lower()
            if not target_email.endswith(ALLOWED_DOMAIN.lower()):
                raise HTTPException(
                    status_code=400,
                    detail=f"Target email must be from {ALLOWED_DOMAIN} domain"
                )

        # Determine share type
        share_type = getattr(req, 'share_type', 'file') or 'file'

        # Validate based on share type
        if share_type == 'folder':
            if not req.folder_id:
                raise HTTPException(status_code=400, detail="folder_id is required for folder shares")
            document_id = None
            folder_id = req.folder_id
            item_name = req.item_name or 'Folder'
        else:
            if not req.document_id:
                raise HTTPException(status_code=400, detail="document_id is required for file shares")
            document_id = req.document_id
            folder_id = None
            item_name = None

        # Create Share Link with optional target_email
        token = await sharing_db.create_share_link(
            document_id=document_id,
            folder_id=folder_id,
            created_by=user_id,
            expiry_date=req.expiry_date,
            target_email=target_email,
            share_type=share_type
        )

        base_url = os.getenv('FRONTEND_URL', 'http://localhost:3000')
        link = f"{base_url}/shared/{token}"

        # Send email to target recipient for restricted shares
        if target_email:
            try:
                # Get item name for the email
                if share_type == 'folder':
                    document_name = item_name or 'Shared Folder'
                else:
                    document = await documents_db.get_document_by_id(document_id)
                    document_name = document.get('docname',
                                                 document.get('title', 'Document')) if document else 'Document'

                # Get sharer's name/email for the email
                sharer_name = user_info.get('full_name') or user_info.get('email') or username

                send_share_link_email(
                    to_email=target_email,
                    share_link=link,
                    document_name=document_name,
                    sharer_name=sharer_name,
                    expiry_date=req.expiry_date
                )
            except Exception as email_error:
                logging.error(f"Failed to send share link email to {target_email}: {email_error}")

        return {
            "token": token,
            "link": link,
            "expiry_date": req.expiry_date,
            "target_email": target_email,
            "share_mode": "restricted" if target_email else "open",
            "share_type": share_type
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logging.error(f"Share generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get('/api/share/info/{token}')
async def get_share_info(token: str):
    """
    Returns basic share link info (without requiring authentication).
    Used by frontend to determine if email is pre-set and share type.
    For restricted shares, returns full target_email to enable auto-OTP.
    Also returns skip_otp flag - True for restricted shares to skip OTP verification.
    """
    try:
        share_info = await sharing_db.get_share_details(token)
        if not share_info:
            raise HTTPException(status_code=404, detail="Link is invalid or expired")

        target_email = share_info.get('target_email')
        share_type = share_info.get('share_type', 'file')
        is_restricted = target_email is not None

        # For restricted shares, return full email to enable auto-OTP
        # This is safe because the link is only sent to that specific email
        return {
            "is_restricted": is_restricted,
            "target_email": target_email,  # Full email for auto-OTP
            "target_email_hint": target_email[:3] + "***" + target_email[
                target_email.index('@'):] if target_email else None,
            "expiry_date": share_info.get('expiry_date'),
            "share_type": share_type,
            "skip_otp": is_restricted  # Skip OTP for restricted shares
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Share info error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post('/api/share/request-access/{token}')
async def request_access_otp(token: str, req: ShareAccessRequest):
    """
    Step 1: Validates domain/target email and sends OTP using Database for storage.

    If the share link has a target_email set, only that email can request access.
    Otherwise, any @rta.ae email can request access.
    """
    try:
        viewer_email = req.viewer_email.strip().lower()

        # Validate email access (handles both domain check and target_email restriction)
        is_allowed, error_message = await sharing_db.validate_target_email_access(token, viewer_email)
        if not is_allowed:
            raise HTTPException(status_code=403, detail=error_message)

        # Verify Link Exists (redundant but safe)
        share_info = await sharing_db.get_share_details(token)
        if not share_info:
            raise HTTPException(status_code=404, detail="Link is invalid or expired")

        # Generate OTP
        otp = str(random.randint(100000, 999999))

        # Store OTP in Database
        saved = await sharing_db.save_otp(token, viewer_email, otp)
        if not saved:
            raise HTTPException(status_code=500, detail="Failed to generate OTP. Database unavailable.")

        # Send Email via SMTP
        send_otp_email(viewer_email, otp)

        return {"message": "OTP sent to your email.", "email": viewer_email}

    except HTTPException as ex:
        raise ex
    except Exception as e:
        logging.error(f"OTP request error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post('/api/share/verify-access/{token}')
async def verify_access_otp(token: str, req: ShareVerifyRequest):
    """
    Step 2: Verifies OTP via Database (or skips for restricted shares).
    Returns document info for file shares, or folder info for folder shares.
    
    For restricted shares (target_email set), OTP verification can be skipped
    by setting skip_otp=True. Open shares always require OTP verification.
    """
    try:
        viewer_email = req.viewer_email.strip().lower()

        # Re-validate email access before verification
        is_allowed, error_message = await sharing_db.validate_target_email_access(token, viewer_email)
        if not is_allowed:
            raise HTTPException(status_code=403, detail=error_message)

        # Get share details to check if it's restricted
        share_info = await sharing_db.get_share_details(token)
        if not share_info:
            raise HTTPException(status_code=404, detail="Link is invalid or expired")

        target_email = share_info.get('target_email')
        is_restricted = target_email is not None

        # Determine if we should skip OTP verification
        # Only allow skipping OTP for restricted shares where viewer matches target
        if req.skip_otp:
            if not is_restricted:
                raise HTTPException(
                    status_code=400, 
                    detail="OTP verification cannot be skipped for open shares"
                )
            # For restricted shares, viewer_email must match target_email (already validated above)
            # Skip OTP verification - access is granted based on email match
        else:
            # Verify and Consume OTP in one go
            is_valid = await sharing_db.verify_otp(token, viewer_email, req.otp)

            if not is_valid:
                # Note: This generic message covers expired, used, or wrong OTPs for security
                raise HTTPException(status_code=400, detail="Invalid or expired OTP.")

        await sharing_db.log_share_access(share_info['share_id'], viewer_email)

        share_type = share_info.get('share_type', 'file')

        if share_type == 'folder':
            # Return folder info
            folder_id = share_info.get('folder_id')
            return {
                "share_type": "folder",
                "folder_id": folder_id,
                "shared_by": share_info['created_by']
            }
        else:
            # Return document info (existing behavior)
            document = await documents_db.get_document_by_id(share_info['document_id'])
            if not document:
                raise HTTPException(status_code=404, detail="Document not found")

            stats = await sharing_db.get_access_stats(token)

            return {
                "share_type": "file",
                "document": document,
                "shared_by": share_info['created_by'],
                "access_stats": stats
            }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"OTP verification error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get('/api/share/folder-contents/{token}')
async def get_shared_folder_contents(
        token: str,
        req: SharedFolderContentsRequest = Depends()
):
    """
    Returns the contents of a shared folder.
    Supports navigation within subfolders.
    """
    try:
        viewer_email = req.viewer_email
        parent_id = req.parent_id

        # 1. Validate email access
        is_allowed, error_message = await sharing_db.validate_target_email_access(token, viewer_email)
        if not is_allowed:
            raise HTTPException(status_code=403, detail=error_message)

        # 2. Verify the share link is valid
        share_info = await sharing_db.get_share_details(token)
        if not share_info:
            raise HTTPException(status_code=404, detail="Link is invalid or expired")

        # 3. Verify this is a folder share
        if share_info.get('share_type') != 'folder':
            raise HTTPException(status_code=400, detail="This is not a folder share")

        # 4. Verify the viewer has verified access via OTP
        has_access = await sharing_db.check_viewer_access(token, viewer_email)
        if not has_access:
            raise HTTPException(status_code=403, detail="Access not verified. Please complete OTP verification first.")

        # 5. Get the root shared folder ID (convert to string for consistent comparisons)
        root_folder_id = str(share_info['folder_id'])

        # 6. Determine which folder to list
        # If parent_id is provided, verify it's within the shared folder hierarchy
        current_folder_id = str(parent_id) if parent_id else root_folder_id

        # Security check: Ensure the requested folder is within the shared folder hierarchy
        if parent_id and str(parent_id) != root_folder_id:
            is_subfolder = await folders_db.verify_folder_in_hierarchy(root_folder_id, str(parent_id))
            if not is_subfolder:
                raise HTTPException(status_code=403, detail="Access denied. Folder is outside shared scope.")

        # 7. Get folder info and contents
        folder_info = await folders_db.get_folder_by_id(current_folder_id)
        contents = await folders_db.get_folder_contents(current_folder_id)

        # Build breadcrumb path
        breadcrumbs = await folders_db.build_breadcrumb_path(root_folder_id, current_folder_id)

        return {
            "folder_id": current_folder_id,
            "folder_name": folder_info.get('name', 'Shared Folder') if folder_info else 'Shared Folder',
            "root_folder_id": root_folder_id,
            "is_root": str(current_folder_id) == str(root_folder_id),
            "breadcrumbs": breadcrumbs,
            "contents": contents
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Folder contents error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get('/api/share/download/{token}')
async def download_shared_document(
        token: str,
        req: SharedDocumentDownloadRequest = Depends()
):
    """
    Downloads a shared document after OTP verification.
    Uses system DMS credentials to fetch the document.
    Applies watermark with viewer_email based on file type.

    For folder shares, doc_id parameter specifies which file to download.
    """
    try:
        viewer_email = req.viewer_email
        doc_id = req.doc_id

        # 1. Validate email access
        is_allowed, error_message = await sharing_db.validate_target_email_access(token, viewer_email)
        if not is_allowed:
            raise HTTPException(status_code=403, detail=error_message)

        # 2. Verify the share link is valid
        share_info = await sharing_db.get_share_details(token)
        if not share_info:
            raise HTTPException(status_code=404, detail="Link is invalid or expired")

        # 3. Verify the viewer has verified access via OTP
        has_access = await sharing_db.check_viewer_access(token, viewer_email)
        if not has_access:
            raise HTTPException(status_code=403, detail="Access not verified. Please complete OTP verification first.")

        # 4. Determine document ID based on share type
        share_type = share_info.get('share_type', 'file')

        if share_type == 'folder':
            # For folder shares, doc_id must be provided
            if not doc_id:
                raise HTTPException(status_code=400, detail="doc_id is required for folder share downloads")

            # Verify the document is within the shared folder hierarchy
            root_folder_id = str(share_info['folder_id'])
            is_in_folder = await folders_db.verify_document_in_folder(root_folder_id, str(doc_id))
            if not is_in_folder:
                raise HTTPException(status_code=403, detail="Document is outside shared folder scope")

            document_id = doc_id
        else:
            # For file shares, use the document_id from share_info
            document_id = share_info['document_id']

        # 5. Login with system credentials
        dst = wsdl_client.dms_system_login()
        if not dst:
            raise HTTPException(status_code=500, detail="Failed to authenticate with DMS")

        # 6. Get document info and content
        filename, media_type, file_ext = await db_connector.get_media_info_from_dms(dst, document_id)
        if not filename:
            raise HTTPException(status_code=404, detail="Document not found")

        # Ensure filename has extension
        if file_ext and not filename.lower().endswith(file_ext.lower()):
            filename = f"{filename}{file_ext}"

        file_bytes = db_connector.get_media_content_from_dms(dst, document_id)
        if not file_bytes:
            raise HTTPException(status_code=500, detail="Failed to retrieve document content")

        # 7. Create watermark text using viewer_email
        watermark_text = f"{viewer_email} | {document_id} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        # 8. Determine mime type and apply watermark based on media type
        mimetype = 'application/octet-stream'
        processed_bytes = file_bytes

        if media_type == 'image':
            processed_bytes, mimetype = apply_watermark_to_image(file_bytes, watermark_text)
        elif media_type == 'pdf':
            processed_bytes, mimetype = apply_watermark_to_pdf(file_bytes, watermark_text)
        elif media_type == 'video':
            processed_bytes, mimetype = apply_watermark_to_video(file_bytes, watermark_text, filename)
        elif media_type == 'text':
            mimetype = "text/plain"
        elif media_type == 'excel':
            mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif media_type == 'powerpoint':
            mimetype = "application/vnd.openxmlformats-officedocument.presentationml.presentation"

        # 9. Log the download
        await sharing_db.log_share_access(share_info['share_id'], viewer_email)

        # 10. Return the watermarked file
        return StreamingResponse(
            io.BytesIO(processed_bytes),
            media_type=mimetype,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Share download error: {e}")
        raise HTTPException(status_code=500, detail=str(e))