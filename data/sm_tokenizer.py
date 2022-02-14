# pip install sacremoses==0.0.47

import logging as log
from sacremoses import MosesTokenizer, MosesPunctNormalizer
TOK_ERR_PREF = '<TOKERR>'

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
    if not text:
        return ''
    try:
        text = normr.normalize(text)
        text = tok.tokenize(text, escape=False, return_str=True, aggressive_dash_splits=True,
                            protected_patterns=tok.WEB_PROTECTED_PATTERNS)
        text = text.replace('\r', ' ').replace('\t', ' ')
        return text
    except:
        log.exception(f"error: {text}")
        return f'{TOK_ERR_PREF} ' + text.replace('\r', ' ').replace('\t', ' ')


tokenize_src = tokenize_eng
