#!/usr/bin/env python3

from datetime import datetime
from pathlib import Path
from myspark import spark


delim = '\t'
SCHEMA = 'did STRING, src STRING, eng STRING'
MAX_TOKENS=512

def get_src_lang(did):
    if not did:
        return 'ERROR'
    parts = did.split('-')
    if len(parts) != 5:
        return 'ERROR'
    l1, l2 = parts[3], parts[4]

    if 'eng' in l1:
        src = l2
    else:
        src = l1
    src = src.split('_')[0] # ISO 639-3 only
    return src

def get_row_stats(row):
    src_lang = get_src_lang(row.did)
    src_toks = len((row.src or '').split())
    eng_toks = len((row.eng or '').split())
    return src_lang, (1, src_toks, eng_toks)

def main(inp_file, out_file):
    start = datetime.now()
    stats = spark.read.csv(inp_file, sep='\t', schema=SCHEMA)\
                      .rdd\
                      .map(get_row_stats)\
                      .reduceByKey(lambda a, b: tuple(ai + bi for ai, bi in zip(a, b)))\
                      .collectAsMap()
    
    stats = list(sorted(stats.items(), key=lambda x: x[1][0], reverse=True))
    total = [0, 0, 0]
    for _, v in stats:
        total[0] += v[0]
        total[1] += v[1]
        total[2] += v[2]


    HEADER = f'Language{delim}Sentences{delim}SourceToks{delim}EnglishToks'
    with open(out_file, 'w') as out:
        out.write(HEADER + '\n')
        tstat = delim.join(str(x) for x in total)
        out.write(f'TOTAL{delim}{tstat}\n')
        for lang, lstat in stats:
            lstat = delim.join(str(x) for x in lstat)
            out.write(f'{lang}{delim}{lstat}\n')
            
    end = datetime.now()
    print(f"{end} :: Done {inp_file}")
    print(f"time taken: {end - start}")

    
if __name__ == '__main__':    
    #inp_file = 'train-all-dedup-smtok.tsv'
    #inp_file = 'train-all.10M.tsv'
    #inp_file = 'train-all.tsv'
    #inp_file = 'train-all-dedup.tsv'
    #inp_file = 'train-all-dedup-smtok-nourl.tsv'
    inp_file = 'train-all.cleandedupe.tok.tsv'
    
    out_file = inp_file.replace('.tsv', '') + '.stats.tsv'

    main(inp_file, out_file)
