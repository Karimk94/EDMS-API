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
    import base64

    # Get configurable values from environment or use defaults
    company_name = os.getenv("COMPANY_NAME")
    support_email = os.getenv("SUPPORT_EMAIL")
    company_website = os.getenv("COMPANY_WEBSITE")
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

    current_year = datetime.now().year
    current_datetime = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    # Logo section - embed base64 image if available
    logo_section = ""
    if logo_base64_src:
        logo_section = f'''
            <div style="text-align: center; margin-bottom: 10px;">
                <img src="{logo_base64_src}" alt="{company_name} Logo" style="max-width: 180px; max-height: 80px; height: auto;">
            </div>
        '''
    else:
        # Fallback to text-based logo if no image file found
        logo_section = f'''
            <div style="text-align: center; margin-bottom: 10px;">
                <h1 style="color: {primary_color}; margin: 0; font-size: 28px; font-weight: 700;">{company_name}</h1>
            </div>
        '''

    html_template = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Document Access Verification</title>
</head>
<body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f7fa; line-height: 1.6;">
    <table role="presentation" style="width: 100%; border-collapse: collapse;">
        <tr>
            <td align="center" style="padding: 0px 10px;">
                <table role="presentation" style="width: 100%; max-width: 600px; border-collapse: collapse; background-color: #ffffff; border-radius: 12px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">

                    <!-- Header Section -->
                    <tr>
                        <td style="padding: 10px 40px 0px 10px; background: linear-gradient(135deg, {primary_color} 0%, #004499 100%); border-radius: 12px 12px 0 0;">
                            {logo_section.replace(f'color: {primary_color}', 'color: #ffffff')}
                            <h1 style="color: #333333; margin: 0; font-size: 24px; font-weight: 600; text-align: center;">
                                Document Access Verification
                            </h1>
                        </td>
                    </tr>

                    <!-- Main Content -->
                    <tr>
                        <td style="padding: 40px;">
                            <p style="color: #333333; font-size: 16px; margin: 0 0 20px 0;">
                                Hello,
                            </p>
                            <p style="color: #555555; font-size: 15px; margin: 0 0 25px 0;">
                                You have requested access to a shared document. Please use the verification code below to complete your access request:
                            </p>

                            <!-- OTP Code Box -->
                            <div style="background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); border: 2px dashed {primary_color}; border-radius: 12px; padding: 30px; text-align: center; margin: 30px 0;">
                                <p style="color: #666666; font-size: 14px; margin: 0 0 10px 0; text-transform: uppercase; letter-spacing: 2px;">
                                    Your Verification Code
                                </p>
                                <p style="color: {primary_color}; font-size: 42px; font-weight: 700; margin: 0; letter-spacing: 8px; font-family: 'Courier New', monospace;">
                                    {otp}
                                </p>
                            </div>

                            <!-- Expiry Warning -->
                            <div style="background-color: #fff3cd; border-left: 4px solid #ffc107; border-radius: 0 8px 8px 0; padding: 15px 20px; margin: 25px 0;">
                                <p style="color: #856404; font-size: 14px; margin: 0;">
                                    <strong>⏰ Important:</strong> This code will expire in <strong>{validity_minutes} minutes</strong>. Please enter it promptly to access your document.
                                </p>
                            </div>

                            <!-- Request Details -->
                            <div style="background-color: #f8f9fa; border-radius: 8px; padding: 20px; margin: 25px 0;">
                                <h3 style="color: #333333; font-size: 16px; margin: 0 0 15px 0; border-bottom: 1px solid #dee2e6; padding-bottom: 10px;">
                                    📋 Request Details
                                </h3>
                                <table style="width: 100%; font-size: 14px;">
                                    <tr>
                                        <td style="color: #666666; padding: 5px 0; width: 40%;">Email Address:</td>
                                        <td style="color: #333333; padding: 5px 0; font-weight: 500;">{recipient_email}</td>
                                    </tr>
                                    <tr>
                                        <td style="color: #666666; padding: 5px 0;">Request Time:</td>
                                        <td style="color: #333333; padding: 5px 0; font-weight: 500;">{current_datetime}</td>
                                    </tr>
                                    <tr>
                                        <td style="color: #666666; padding: 5px 0;">Valid For:</td>
                                        <td style="color: #333333; padding: 5px 0; font-weight: 500;">{validity_minutes} minutes</td>
                                    </tr>
                                </table>
                            </div>

                            <!-- Security Notice -->
                            <div style="background-color: #f8d7da; border-left: 4px solid #dc3545; border-radius: 0 8px 8px 0; padding: 15px 20px; margin: 25px 0;">
                                <p style="color: #721c24; font-size: 13px; margin: 0;">
                                    <strong>🔒 Security Notice:</strong> If you did not request this verification code, please ignore this email. Do not share this code with anyone. Our team will never ask you for this code.
                                </p>
                            </div>

                            <p style="color: #555555; font-size: 14px; margin: 25px 0 0 0;">
                                If you have any questions or need assistance, please contact our support team at 
                                <a href="mailto:{support_email}" style="color: {primary_color}; text-decoration: none;">{support_email}</a>
                            </p>
                        </td>
                    </tr>

                    <!-- Footer -->
                    <tr>
                        <td style="padding: 30px 40px; background-color: #f8f9fa; border-radius: 0 0 12px 12px; border-top: 1px solid #e9ecef;">
                            <table role="presentation" style="width: 100%;">
                                <tr>
                                    <td style="text-align: center;">
                                        <p style="color: #666666; font-size: 14px; margin: 0 0 10px 0;">
                                            Thank you for using {company_name}
                                        </p>
                                        <p style="color: #999999; font-size: 12px; margin: 0 0 15px 0;">
                                            <a href="{company_website}" style="color: {primary_color}; text-decoration: none;">{company_website}</a>
                                        </p>
                                        <hr style="border: none; border-top: 1px solid #dee2e6; margin: 20px 0;">
                                        <p style="color: #999999; font-size: 11px; margin: 0;">
                                            © {current_year} {company_name}. All rights reserved.<br>
                                            This is an automated message. Please do not reply directly to this email.
                                        </p>
                                    </td>
                                </tr>
                            </table>
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

    company_name = os.getenv("COMPANY_NAME")
    subject = f"{company_name} - Document Access Verification Code"

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
        logging.info(f"OTP email sent successfully to {to_email}")
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