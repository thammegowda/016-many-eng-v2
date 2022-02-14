
#pip install sacremoses==0.0.47

import logging as log
from sacremoses import MosesTokenizer, MosesPunctNormalizer

log.basicConfig(level=log.INFO)

normr = MosesPunctNormalizer(
        lang='en',
        norm_quote_commas=True,
        norm_numbers=True,
        pre_replace_unicode_punct=True,
        post_remove_control_chars=True,
    )

tok = MosesTokenizer(lang='en')

def tokenize_eng(text):
    try:
        text = normr.normalize(text)
        text = tok.tokenize(text, escape=False, return_str=True, aggressive_dash_splits=True,
            protected_patterns=tok.WEB_PROTECTED_PATTERNS)
        return text
    except:
        if text:
            log.exception(f"error: {text}")
            return '<TOKERR> ' + text
        else:
            return ''

tokenize_src = tokenize_eng 
