#!/usr/bin/env python3

from datetime import datetime
from pathlib import Path
from myspark import spark
from get_stats import main as make_stats


SCHEMA = 'did STRING, src STRING, eng STRING'

MAX_TOKENS=512

def is_good(text):
    if not text:
        return False
    if text.startswith('<TOKERR> '):
        return False
    if 'http' in text:
        return False
    if ' href' in text:
        return False    
    return True


def main(inp_file, out_file):
    if (Path(out_file) / '_SUCCESS').exists():
        print(f"WARNING {out_file} is valid; skipping")
        raise Exception(f'{out_file} exists and valid. skipping')
    start = datetime.now()
    print(f"{start} :: Tokenize {inp_file} ->  {out_file}")

    (
        spark.read.csv(inp_file, sep='\t', schema=SCHEMA).rdd
        .map(lambda row: (row.did or '', row.src or '', row.eng or ''))
        .filter(lambda row: (row[0] and row[1] and row[2]) and is_good(row[1]) and is_good(row[2]))
        .map("\t".join).saveAsTextFile(out_file)
    )

    end = datetime.now()
    print(f"{end} :: Done {inp_file}")
    print(f"time taken: {end - start}")
    
    

if __name__ == '__main__':
    inp_file = 'train-all-dedup-smtok.tsv'
    out_file = inp_file.replace('.tsv', '') + '-nourl.tsv'
    main(inp_file, out_file)
