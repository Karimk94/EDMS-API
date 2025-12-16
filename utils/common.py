from fastapi import Request, HTTPException, status
import re

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