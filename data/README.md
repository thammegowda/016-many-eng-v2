# Tips, Tricks and Gotchas

. Mtdata fail to download a few datasets. I just ignored them. 

. If you ever need to compress files, use pigz, it is faster

. Spark Setup : IMPORTANT
  edit myspark.py
   and set correctly `cpus` `memory` and `tmp_dir`


. Slurm submissions

At USC CARC, we have largmem nodes with 1TB RAM. So I configured sulrm jobs and ran
1TB is not necessary (technically). For 60 CPU cores, it used 550GB RAM.
    
    ./slurm-job.sh -J prep ./step0.prep.py -hi devs-tests-all.tsv -ti train-all.tsv train.JW300.raw.v2.tsv  -to train-all.cleandedupe.tok.tsv

    # run this after the above job finishes; creates *.stats.tsv
    ./slurm-job.sh -J stats ./stepx.stats.py
    

. Combine all files. There were 2,314,289,643 lines found (from *.stats.tsv)

     cat train-all.cleandedupe.tok.tsv/part-* | tqdm --total=2314289643 --unit-scale=1 \
      | awk -F '\t' -v out=train-all.cleandedupe '{print $1 > out".did"; print $2> out".src.tok"; print $3 > out".eng.tok"}'

. Combine dev and tests (from spark partitions) into single file
    
    cat devs-tests-all.tok.tsv/part-000* > devs-tests-all.1file.tok.tsv
    
    # separate devs and tests ;;; note: exclude visitestonia, an accidentally included dataset
    cat  devs-tests-all.1file.tok.tsv | grep -v 'visitestonia' | grep $'^[^\t]*test.*' > tests-all.tok.tsv
    cat  devs-tests-all.1file.tok.tsv | grep -v 'visitestonia' | grep -v $'^[^\t]*test.*' > devs-all.tok.tsv

