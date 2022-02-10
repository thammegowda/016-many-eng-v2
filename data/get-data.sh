#!/usr/bin/env bash

set -euo pipefail

DIR=$(dirname ${BASH_SOURCE[0]})
DIR=$(realpath "${DIR}")


cd $DIR
# mtdata v0.3.5
[[ -f _DOWNd ]] || python -m mtdata get-recipe -ri many-eng-v2 -o . -j 8 && touch _DOWNd
# some downloads will fail, ignore them

function log { echo "$(date --rfc-3339=seconds):: $@" >&2; }

function parallel_files {
    dir=$1
    
    ls $dir/*.* \
        | xargs -I {} basename {} \
        | awk -F '-' 'NF==5' \
        | sed 's/\.[^\.]*$//' \
        | uniq \
        | while read did; do
            #did=$(basename $path | )
            lang1=$(echo $did | cut -d- -f4)
            lang2=$(echo $did | cut -d- -f5)
            
            if [[ $lang1 == eng* ]]; then # eng might be eng, eng_US, eng_IN, ...
                eng=$lang1
                src=$lang2
            else
                src=$lang1
                eng=$lang2
            fi
            echo $did $did.$src $did.$eng
        done
}


function merge_all {
    out=$1
    parts_dir=$2
    if [[ -f $out._OK ]]; then
        log "$out is valid; skipping"
        return
    fi
    i=0
    ( parallel_files $parts_dir \
          | while read did src eng; do
          src=$parts_dir/$src
          eng=$parts_dir/$eng
          l1=$(wc -l < $src)
          l2=$(wc -l < $eng)
          if [[ $l1 -ne $l2 ]]; then
              log "ERROR: $did source has $l1 lines English has $l2 lines"
              echo "$did" >> merge-errors.txt
              continue
          fi         
          paste $src $eng | sed "s/^/$did\t/"
          i=$((i+1))
          log "$i Done: $did"
      done ) | tqdm --unit_scale=1 --unit=line > $out
    touch $out._OK
}

function merge_all_train {
    
    out=train-all.tsv
    parts_dir=train-parts
    if [[ -f $out._OK ]]; then
        log "$out is valid; skipping"
        return
    fi
    i=0
    ( parallel_files $parts_dir \
          | while read did src eng; do
          src=$parts_dir/$src
          eng=$parts_dir/$eng
          l1=$(wc -l < $src)
          l2=$(wc -l < $eng)
          if [[ $l1 -ne $l2 ]]; then
              log "ERROR: $did source has $l1 lines English has $l2 lines"
              echo "$did" >> merge-errors.txt
              continue
          fi         
          paste $src $eng | sed "s/^/$did\t/"
          i=$((i+1))
          log "$i Done: $did"
      done ) | tqdm --unit_scale=1 --unit=line > $out
    touch $out._OK
}


merge_all train-all.tsv train-parts
merge_all devs-tests-all.tsv tests






