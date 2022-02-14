#!/usr/bin/env python3

from myspark import spark
import unicodedata
import html
import hashlib


inp_file = 'train-all.tsv'
#inp_file = 'train-all.10M.tsv' # smaller file to test the code

held_out_file = 'devs-tests-all.tsv' # these sentences are to be excluded
out_file = inp_file.replace('.tsv', '') + '-dedup.tsv'

MAX_CHARS = 1024
LEN_RATIO = 5

SCHEMA = 'did STRING, src STRING, eng STRING'


def read_tsv(path):
    with open(path, 'r', encoding='utf-8', errors='ignore') as rdr:
        for line in rdr:
            yield line.rstrip('\n').split('\t')

def hash_text(text):
    return int(hashlib.md5(text.encode('utf-8')).hexdigest(), 16)

    
def clean(text):
    if text is None:
        return ''
    text = ' '.join(text.split()).strip()
    text = html.unescape(text)
    text = unicodedata.normalize('NFKC', text)
    return text


def hash_tsv_segs(path):
    mem = set() 
    for id_, src, eng in read_tsv(path):
        mem.add(hash_text(clean(src)))
        mem.add(hash_text(clean(eng)))
    return mem
    
exclude_hashes = hash_tsv_segs(held_out_file)


def is_url(text):
    # not 100% correct, but fast for https?
    return ' ' not in text and text.startswith('http')


def is_good(src, eng):
    if not src or not eng: # empty
        return False

    if src == eng: # copy
        return False

    if max(len(src), len(eng)) >= MAX_CHARS: # too long
        return False
    
    ratio = len(src) / len(eng) 
    if ratio < 1/LEN_RATIO or ratio > LEN_RATIO:
        return False

    # english sentence has many non-ascii chars
    if sum(1 for x in eng if ord(x) > 255) >= 0.25 * len(eng):
        return False
    
    if is_url(src) or is_url(eng):
        return False

    # dev or test sentence 
    if hash_text(src) in exclude_hashes or hash_text(eng) in exclude_hashes:
        return False
    
    # couldnt reject with any known rules ==> good
    return True


def row_to_tsv(row):
    return f'{row.did}\t{row.src}\t{row.eng}'

print(f"Clean and dedupe {inp_file} ->  {out_file}")
spark.read.csv(inp_file, sep='\t', schema=SCHEMA) \
    .rdd.map(lambda row: (row.did, clean(row.src), clean(row.eng))) \
       .filter(lambda row: is_good(src=row[1], eng=row[2])) \
    .toDF(schema=SCHEMA)\
    .dropDuplicates(['src', 'eng'])\
    .rdd.map(row_to_tsv)\
    .saveAsTextFile(out_file)

print("Done")
