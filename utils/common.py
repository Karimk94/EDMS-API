from functools import wraps
from flask import session, abort
import re

def editor_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session or session['user'].get('security_level') != 'Editor':
            abort(403)
        return f(*args, **kwargs)
    return decorated_function

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