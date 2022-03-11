#!/usr/bin/env bash

set -euo pipefail

[[ -f devs-tests-all.1file.tok.tsv ]] || cat devs-tests-all.tok.tsv/part-000* > devs-tests-all.1file.tok.tsv

# separate devs and tests ;;; note: exclude visitestonia, an accidentally included dataset
[[ -f  tests-all.tok.tsv ]] || cat  devs-tests-all.1file.tok.tsv | grep -v 'visitestonia' | grep $'^[^\t]*test.*' > tests-all.tok.tsv
[[ -f  devs-all.tok.tsv ]] || cat  devs-tests-all.1file.tok.tsv | grep -v 'visitestonia' | grep -v $'^[^\t]*test.*' > devs-all.tok.tsv


[[ -f devs-all.shuf.tok.tsv ]] || shuf devs-all.tok.tsv > devs-all.shuf.tok.tsv
[[ -f devs.shuf10k.tok.tsv ]] || grep -v NULL devs-all.shuf.tok.tsv | head -10000 > devs.shuf10k.tok.tsv

[[ -f devs.shuf10k.eng ]] || {
    cat devs.shuf10k.tok.tsv | awk -F '\t' -v out=devs.shuf10k '{ print $1 > out".did"; print $2 > out".src.tok"; print $3 > out".eng.tok"; print $4 > out".src"; print $5 > out".eng" }'
}
