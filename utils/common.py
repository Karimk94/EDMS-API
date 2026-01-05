from fastapi import Request, HTTPException, status
import re
import wsdl_client
import gc
import smtplib
import os
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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

def send_otp_email(to_email: str, otp: str):
    """
    Sends an OTP verification email using SMTP.
    Requires environment variables: SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD
    """
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    from_email = os.getenv("SMTP_SENDER_EMAIL")

    if not smtp_user or not smtp_password:
        logging.warning(f"SMTP credentials not configured. Mocking OTP for {to_email}: {otp}")
        return

    subject = "Smart EDMS - Document Access Verification"
    body = f"Your verification code is: {otp}\n\nThis code expires in 10 minutes."

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
        logging.info(f"OTP email sent to {to_email}")
    except Exception as e:
        logging.error(f"Failed to send email to {to_email}: {e}")
        raise HTTPException(status_code=500, detail="Failed to send verification email. Please contact support.")
