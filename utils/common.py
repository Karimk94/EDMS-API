from fastapi import Request, HTTPException, status
import re
import wsdl_client
import gc

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