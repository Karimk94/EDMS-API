from fastapi import Request, HTTPException, status
import re
import wsdl_client
import gc
import smtplib
import os
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import base64

def verify_editor(request: Request):
    user = request.session.get("user")
    if not user or user.get("security_level") != "Editor":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Editor privileges required."
        )

def clean_repeated_words(text):
    if not text:
        return ""
    words = text.split()
    if not words:
        return ""
    result_words = [words[0]]
    for i in range(1, len(words)):
        current_word_norm = re.sub(r'[^\w]', '', words[i])
        last_result_word_norm = re.sub(r'[^\w]', '', result_words[-1])
        if current_word_norm and current_word_norm == last_result_word_norm:
            result_words[-1] = words[i]
        else:
            result_words.append(words[i])
    return " ".join(result_words)

def get_current_user(request: Request):
    """
    Unified function to retrieve the current authenticated user from the session.
    Raises 401 Unauthorized if the user is not logged in.
    """
    user = request.session.get('user')
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: User not logged in"
        )
    return user

def get_session_token(request: Request):
    """
    Unified function to retrieve the DMS token from the current user's session.
    """
    user = get_current_user(request)
    token = user.get('token')
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: Valid session token not found"
        )
    return token

def find_active_soap_client():
    """
    Scans memory to find an active Zeep SOAP Client object.
    Useful when the client instance is hidden in closures or unknown variable names.
    """
    for name, obj in vars(wsdl_client).items():
        if hasattr(obj, 'service') and hasattr(obj.service, 'Search'):
            return obj

    try:
        for obj in gc.get_objects():
            if hasattr(obj, 'service') and hasattr(obj.service, 'Search'):
                if hasattr(obj, 'wsdl') or hasattr(obj, 'transport'):
                    return obj
    except Exception:
        pass

    return None

def get_otp_email_template(otp: str, recipient_email: str, validity_minutes: int = 5) -> str:
    """
    Generates an HTML email template for OTP verification.

    Args:
        otp: The one-time password code
        recipient_email: The email address of the recipient
        validity_minutes: How long the OTP is valid for

    Returns:
        HTML string for the email body
    """

    # Get configurable values from environment or use defaults
    company_name = os.getenv("COMPANY_NAME")
    support_email = os.getenv("SUPPORT_EMAIL")
    primary_color = os.getenv("EMAIL_PRIMARY_COLOR")

    # Build logo as base64 data URI from local file
    logo_base64_src = ""
    logo_filename = os.getenv("COMPANY_LOGO_FILENAME")

    # Get current file location
    current_file = os.path.abspath(__file__)

    # Get directory of current file (utils/)
    current_dir = os.path.dirname(current_file)

    # Get parent directory (project root)
    base_dir = os.path.dirname(current_dir)

    # Build expected logo path
    logo_path = os.path.join(base_dir, 'static', 'images', logo_filename)

    # Check if logo file exists
    logo_exists = os.path.exists(logo_path)

    if logo_exists:
        try:
            # Get file size
            file_size = os.path.getsize(logo_path)

            # Determine mime type based on file extension
            ext = os.path.splitext(logo_filename)[1].lower()
            mime_types = {
                '.png': 'image/png',
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.gif': 'image/gif',
                '.svg': 'image/svg+xml',
                '.webp': 'image/webp'
            }
            mime_type = mime_types.get(ext, 'image/png')

            # Read and encode the image
            with open(logo_path, 'rb') as img_file:
                logo_bytes = img_file.read()

                logo_base64 = base64.b64encode(logo_bytes).decode('utf-8')

                logo_base64_src = f"data:{mime_type};base64,{logo_base64}"

        except Exception as e:
            logging.error(f"[ERROR] Could not load logo from {logo_path}: {e}", exc_info=True)
    else:
        logging.warning(f"[WARNING] Logo file not found at: {logo_path}")

        # Try alternative paths
        alt_paths = [
            os.path.join(os.getcwd(), 'static', 'images', logo_filename),
            os.path.join(os.getcwd(), logo_filename),
            os.path.join(base_dir, logo_filename),
            os.path.join(current_dir, 'static', 'images', logo_filename),
        ]

        for i, alt_path in enumerate(alt_paths):
            exists = os.path.exists(alt_path)
            if exists and not logo_base64_src:
                try:
                    ext = os.path.splitext(logo_filename)[1].lower()
                    mime_type = {'.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
                                 '.gif': 'image/gif'}.get(ext, 'image/png')
                    with open(alt_path, 'rb') as img_file:
                        logo_base64 = base64.b64encode(img_file.read()).decode('utf-8')
                        logo_base64_src = f"data:{mime_type};base64,{logo_base64}"
                except Exception as e:
                    logging.error(f"  [ERROR] Failed to load from {alt_path}: {e}")
    # === END LOGO PATH RESOLUTION ===

    current_datetime = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    # Logo section
    logo_section = ""
    if logo_base64_src:
        logo_section = f'''
            <div style="text-align: center; margin-bottom: 2px;">
                <img src="{logo_base64_src}" alt="{company_name} Logo" style="max-width: 140px; max-height: 50px; height: auto;">
            </div>
        '''
    else:
        # Fallback to text-based logo
        logo_section = f'''
            <div style="text-align: center; margin-bottom: 2px;">
                <h1 style="color: #ffffff; margin: 0; font-size: 20px; font-weight: 700;">{company_name}</h1>
            </div>
        '''

    html_template = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Verification Code</title>
</head>
<body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f7fa; line-height: 1.4;">
    <table role="presentation" style="width: 100%; border-collapse: collapse;">
        <tr>
            <td align="center" style="padding: 10px;">
                <table role="presentation" style="width: 100%; max-width: 480px; border-collapse: collapse; background-color: #ffffff; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); overflow: hidden;">

                    <!-- Compact Header Section: Reduced top padding to move logo up -->
                    <tr>
                        <td style="padding: 6px 15px 10px 15px; background: linear-gradient(135deg, {primary_color} 0%, #004499 100%); text-align: center;">
                            {logo_section}
                            <h2 style="color: #444444; margin: 0; font-size: 18px; font-weight: 600; letter-spacing: 0.5px; opacity: 0.95;">
                                Document Access Verification
                            </h2>
                        </td>
                    </tr>

                    <!-- Main Content -->
                    <tr>
                        <td style="padding: 20px 25px;">
                            <p style="color: #444444; font-size: 14px; margin: 0 0 15px 0; text-align: center;">
                                Please use the verification code below to complete your request.
                            </p>

                            <!-- OTP Code Box: Fixed selection issues with inline-block and tight line-height -->
                            <div style="background-color: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px; padding: 12px; text-align: center; margin: 0 0 15px 0;">
                                <span style="display: inline-block; color: {primary_color}; font-size: 28px; font-weight: 700; letter-spacing: 4px; font-family: 'Courier New', monospace; line-height: 1; margin: 0;">{otp}</span>
                            </div>

                            <!-- Expiry Warning -->
                            <div style="background-color: #fff3cd; border-radius: 4px; padding: 8px 12px; margin-bottom: 15px; text-align: center;">
                                <p style="color: #856404; font-size: 12px; margin: 0;">
                                    <strong>⏰ Expires in {validity_minutes} mins.</strong>
                                </p>
                            </div>

                            <!-- Security Notice -->
                            <div style="background-color: #f8d7da; border-left: 3px solid #dc3545; border-radius: 4px; padding: 8px 12px; margin-bottom: 15px;">
                                <p style="color: #721c24; font-size: 11px; margin: 0; line-height: 1.4;">
                                    <strong>🔒 Security Notice:</strong> If you did not request this, please ignore this email. Do not share this code.
                                </p>
                            </div>

                            <!-- Request Details -->
                            <div style="border-top: 1px solid #eeeeee; padding-top: 12px; margin-top: 5px;">
                                <p style="color: #666666; font-size: 11px; margin: 0;">
                                    <strong>Request for:</strong> {recipient_email} <span style="float: right;">{current_datetime}</span>
                                </p>
                            </div>
                        </td>
                    </tr>

                    <!-- Minimal Footer -->
                    <tr>
                        <td style="padding: 10px; background-color: #f8f9fa; border-top: 1px solid #e9ecef; text-align: center;">
                            <p style="color: #999999; font-size: 11px; margin: 0;">
                                Need help? <a href="mailto:{support_email}" style="color: {primary_color}; text-decoration: none;">{support_email}</a>
                            </p>
                        </td>
                    </tr>

                </table>
            </td>
        </tr>
    </table>
</body>
</html>
'''
    return html_template

def get_plain_text_email(otp: str, recipient_email: str, validity_minutes: int = 5) -> str:
    """
    Generates a plain text fallback for email clients that don't support HTML.
    """
    company_name = os.getenv("COMPANY_NAME")
    support_email = os.getenv("SUPPORT_EMAIL")
    current_datetime = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    plain_text = f"""
{company_name} - Document Access Verification
{'=' * 50}

Hello,

You have requested access to a shared document. Please use the verification code below to complete your access request:

YOUR VERIFICATION CODE: {otp}

⏰ IMPORTANT: This code will expire in {validity_minutes} minutes.

REQUEST DETAILS:
- Email Address: {recipient_email}
- Request Time: {current_datetime}
- Valid For: {validity_minutes} minutes

🔒 SECURITY NOTICE:
If you did not request this verification code, please ignore this email.
Do not share this code with anyone. Our team will never ask you for this code.

If you have any questions, please contact our support team at {support_email}

Thank you for using {company_name}

---
This is an automated message. Please do not reply directly to this email.
"""
    return plain_text

def send_otp_email(to_email: str, otp: str, validity_minutes: int = 5):
    """
    Sends an OTP verification email using SMTP with HTML template.
    Requires environment variables: SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD

    Optional environment variables for customization:
    - COMPANY_NAME: Name displayed in email (default: "Smart EDMS")
    - COMPANY_LOGO_FILENAME: Logo filename in static/images/ folder (default: "logo.png")
    - SUPPORT_EMAIL: Support contact email
    - COMPANY_WEBSITE: Company website URL
    - EMAIL_PRIMARY_COLOR: Primary color for email styling (default: "#0066cc")

    Logo is embedded as base64 directly in the email, so it works without external URLs.
    Place your logo file in: static/images/logo.png (or set COMPANY_LOGO_FILENAME)
    """
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    from_email = os.getenv("SMTP_SENDER_EMAIL")
    sender_name = os.getenv("SMTP_SENDER_NAME")

    if not smtp_user or not smtp_password:
        logging.warning(f"SMTP credentials not configured. Mocking OTP for {to_email}: {otp}")
        return

    subject = "Document Access Verification Code"

    # Create multipart message for both HTML and plain text
    msg = MIMEMultipart('alternative')
    msg['From'] = f"{sender_name} <{from_email}>"
    msg['To'] = to_email
    msg['Subject'] = subject
    msg['X-Priority'] = '1'  # High priority
    msg['X-Mailer'] = 'Smart EDMS Notification System'

    # Generate email content
    plain_text_content = get_plain_text_email(otp, to_email, validity_minutes)
    html_content = get_otp_email_template(otp, to_email, validity_minutes)

    # Attach both versions (plain text first, then HTML)
    # Email clients will display the last format they support
    part1 = MIMEText(plain_text_content, 'plain', 'utf-8')
    part2 = MIMEText(html_content, 'html', 'utf-8')

    msg.attach(part1)
    msg.attach(part2)

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(from_email, to_email, msg.as_string())
        server.quit()
    except smtplib.SMTPAuthenticationError as e:
        logging.error(f"SMTP Authentication failed: {e}")
        raise HTTPException(status_code=500, detail="Email service authentication failed. Please contact support.")
    except smtplib.SMTPRecipientsRefused as e:
        logging.error(f"Recipient refused: {e}")
        raise HTTPException(status_code=400, detail="Invalid email address. Please check and try again.")
    except smtplib.SMTPException as e:
        logging.error(f"SMTP error sending email to {to_email}: {e}")
        raise HTTPException(status_code=500, detail="Failed to send verification email. Please try again later.")
    except Exception as e:
        logging.error(f"Unexpected error sending email to {to_email}: {e}")
        raise HTTPException(status_code=500, detail="Failed to send verification email. Please contact support.")

def get_share_link_email_template(
        share_link: str,
        document_name: str,
        sharer_name: str,
        recipient_email: str,
        expiry_date: datetime = None
) -> str:
    """
    Generates an HTML email template for share link notification.
    """
    company_name = os.getenv("COMPANY_NAME")
    support_email = os.getenv("SUPPORT_EMAIL")
    primary_color = os.getenv("EMAIL_PRIMARY_COLOR")

    # Build logo as base64
    logo_base64_src = ""
    logo_filename = os.getenv("COMPANY_LOGO_FILENAME")
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)
    base_dir = os.path.dirname(current_dir)
    logo_path = os.path.join(base_dir, 'static', 'images', logo_filename)

    if os.path.exists(logo_path):
        try:
            ext = os.path.splitext(logo_filename)[1].lower()
            mime_types = {
                '.png': 'image/png',
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.gif': 'image/gif',
                '.svg': 'image/svg+xml',
                '.webp': 'image/webp'
            }
            mime_type = mime_types.get(ext, 'image/png')
            with open(logo_path, 'rb') as img_file:
                logo_bytes = img_file.read()
                logo_base64 = base64.b64encode(logo_bytes).decode('utf-8')
                logo_base64_src = f"data:{mime_type};base64,{logo_base64}"
        except Exception as e:
            logging.error(f"Could not load logo: {e}")

    # Logo section
    logo_section = ""
    if logo_base64_src:
        logo_section = f'''
            <div style="text-align: center; margin-bottom: 2px;">
                <img src="{logo_base64_src}" alt="{company_name} Logo" style="max-width: 140px; max-height: 50px; height: auto;">
            </div>
        '''
    else:
        logo_section = f'''
            <div style="text-align: center; margin-bottom: 2px;">
                <h1 style="color: #ffffff; margin: 0; font-size: 20px; font-weight: 700;">{company_name}</h1>
            </div>
        '''

    current_datetime = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    # Format expiry date if provided
    expiry_section = ""
    if expiry_date:
        if isinstance(expiry_date, str):
            try:
                expiry_date = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
            except:
                pass
        if isinstance(expiry_date, datetime):
            expiry_formatted = expiry_date.strftime("%B %d, %Y")
            expiry_section = f'''
                <div style="background-color: #fff3cd; border-radius: 4px; padding: 8px 12px; margin-bottom: 15px; text-align: center;">
                    <p style="color: #856404; font-size: 12px; margin: 0;">
                        <strong>⏰ This link expires on {expiry_formatted}</strong>
                    </p>
                </div>
            '''

    html_template = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Document Shared With You</title>
</head>
<body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f7fa; line-height: 1.4;">
    <table role="presentation" style="width: 100%; border-collapse: collapse;">
        <tr>
            <td align="center">
                <table role="presentation" style="width: 100%; max-width: 520px; border-collapse: collapse; background-color: #ffffff; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); overflow: hidden;">

                    <!-- Header -->
                    <tr>
                        <td style="padding: 0px 15px 10px 0px; background: linear-gradient(135deg, {primary_color} 0%, #004499 100%); text-align: center;">
                            {logo_section}
                            <h2 style="color: #444444; margin: 0; font-size: 18px; font-weight: 600; letter-spacing: 0.5px; opacity: 0.95;">
                                Document Shared With You
                            </h2>
                        </td>
                    </tr>

                    <!-- Main Content -->
                    <tr>
                        <td style="padding: 25px;">
                            <p style="color: #444444; font-size: 14px; margin: 0 0 20px 0;">
                                Hello,
                            </p>

                            <p style="color: #444444; font-size: 14px; margin: 0 0 20px 0;">
                                <strong>{sharer_name}</strong> has shared a document with you:
                            </p>

                            <!-- Document Info Box -->
                            <div style="background-color: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px; padding: 15px; margin: 0 0 20px 0;">
                                <div style="display: flex; align-items: center;">
                                    <span style="font-size: 24px; margin-right: 12px;">📄</span>
                                    <div>
                                        <p style="color: #333333; font-size: 16px; font-weight: 600; margin: 0;">{document_name}</p>
                                        <p style="color: #666666; font-size: 12px; margin: 4px 0 0 0;">Shared on {current_datetime}</p>
                                    </div>
                                </div>
                            </div>

                            {expiry_section}

                            <!-- Access Button -->
                            <div style="text-align: center; margin: 25px 0;">
                                <a href="{share_link}" style="display: inline-block; background-color: {primary_color}; color: #ffffff; text-decoration: none; padding: 14px 35px; border-radius: 6px; font-size: 14px; font-weight: 600; box-shadow: 0 2px 4px rgba(0, 102, 204, 0.3);">
                                    Access Document
                                </a>
                            </div>

                            <p style="color: #666666; font-size: 12px; margin: 20px 0 0 0; text-align: center;">
                                Or copy this link: <br/>
                                <a href="{share_link}" style="color: {primary_color}; word-break: break-all;">{share_link}</a>
                            </p>

                            <!-- Security Notice -->
                            <div style="background-color: #e8f4fd; border-left: 3px solid {primary_color}; border-radius: 4px; padding: 10px 12px; margin-top: 25px;">
                                <p style="color: #0056b3; font-size: 11px; margin: 0; line-height: 1.5;">
                                    <strong>🔒 Secure Access:</strong> You will need to verify your email address ({recipient_email}) with a one-time code to access this document.
                                </p>
                            </div>
                        </td>
                    </tr>

                    <!-- Footer -->
                    <tr>
                        <td style="padding: 15px; background-color: #f8f9fa; border-top: 1px solid #e9ecef; text-align: center;">
                            <p style="color: #999999; font-size: 11px; margin: 0;">
                                Questions? Contact <a href="mailto:{support_email}" style="color: {primary_color}; text-decoration: none;">{support_email}</a>
                            </p>
                            <p style="color: #cccccc; font-size: 10px; margin: 8px 0 0 0;">
                                This is an automated message from {company_name}
                            </p>
                        </td>
                    </tr>

                </table>
            </td>
        </tr>
    </table>
</body>
</html>
'''
    return html_template

def get_share_link_plain_text(
        share_link: str,
        document_name: str,
        sharer_name: str,
        recipient_email: str,
        expiry_date: datetime = None
) -> str:
    """
    Generates a plain text fallback for share link email.
    """
    company_name = os.getenv("COMPANY_NAME", "Smart EDMS")
    support_email = os.getenv("SUPPORT_EMAIL", "support@rta.ae")
    current_datetime = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    expiry_text = ""
    if expiry_date:
        if isinstance(expiry_date, str):
            try:
                expiry_date = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
            except:
                pass
        if isinstance(expiry_date, datetime):
            expiry_text = f"\n⏰ This link expires on: {expiry_date.strftime('%B %d, %Y')}\n"

    plain_text = f"""
{company_name} - Document Shared With You
{'=' * 50}

Hello,

{sharer_name} has shared a document with you.

DOCUMENT: {document_name}
SHARED ON: {current_datetime}
{expiry_text}
ACCESS LINK:
{share_link}

🔒 SECURE ACCESS:
You will need to verify your email address ({recipient_email}) with a one-time code to access this document.

If you have any questions, please contact our support team at {support_email}

---
This is an automated message from {company_name}
"""
    return plain_text

def send_share_link_email(
        to_email: str,
        share_link: str,
        document_name: str,
        sharer_name: str,
        expiry_date: datetime = None
):
    """
    Sends a share link notification email to the target recipient.
    """
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    from_email = os.getenv("SMTP_SENDER_EMAIL")
    sender_name = os.getenv("SMTP_SENDER_NAME")

    if not smtp_user or not smtp_password:
        logging.warning(f"SMTP credentials not configured. Share link email to {to_email} not sent.")
        return

    subject = f"Document Shared: {document_name}"

    # Create multipart message
    msg = MIMEMultipart('alternative')
    msg['From'] = f"{sender_name} <{from_email}>"
    msg['To'] = to_email
    msg['Subject'] = subject
    msg['X-Priority'] = '1'
    msg['X-Mailer'] = 'Smart EDMS Notification System'

    # Generate email content
    plain_text_content = get_share_link_plain_text(
        share_link, document_name, sharer_name, to_email, expiry_date
    )
    html_content = get_share_link_email_template(
        share_link, document_name, sharer_name, to_email, expiry_date
    )

    # Attach both versions
    part1 = MIMEText(plain_text_content, 'plain', 'utf-8')
    part2 = MIMEText(html_content, 'html', 'utf-8')

    msg.attach(part1)
    msg.attach(part2)

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(from_email, to_email, msg.as_string())
        server.quit()
        # logging.info(f"Share link email sent successfully to {to_email}")
    except smtplib.SMTPAuthenticationError as e:
        logging.error(f"SMTP Authentication failed: {e}")
        raise Exception("Email service authentication failed")
    except smtplib.SMTPRecipientsRefused as e:
        logging.error(f"Recipient refused: {e}")
        raise Exception("Invalid email address")
    except smtplib.SMTPException as e:
        logging.error(f"SMTP error sending share link email to {to_email}: {e}")
        raise Exception("Failed to send share link email")
    except Exception as e:
        logging.error(f"Unexpected error sending share link email to {to_email}: {e}")
        raise Exception("Failed to send share link email")