#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    UHVDB/uhbdb_updater
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : https://github.com/UHVDB/uhbdb_updater
----------------------------------------------------------------------------------------
*/

process SPLITBYGENUS {
    label 'process_high'
    tag "split_by_genus"
    container "https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/a6/a6b4319d6bba30fc01cff76aa7f26a0f43592697b8819bc7dcd0b50e61ac184e/data"

    input:
    path(tsv)
    val(large_threshold)

    output:
    path("large_genera/*")  , emit: large
    path("small_genera/*")  , emit: small
    path(".command.log")    , emit: log
    path(".command.sh")     , emit: script

    script:
    """
    ### Split by input genus
    split_by_genus.py \\
        --input ${tsv} \\
        --large_threshold ${large_threshold}
    """
}

// MODULE: UHBDB_LARGEUPDATER
process UHBDB_LARGEUPDATER {
    label 'process_super_high'

    input:
    tuple val(meta), path(local_txt), val(urls_txt), path(objects_txt), path(uhdbd_sketch), path(uhbdb_agc), path(uhbdb_tsv)

    output:
    tuple val(meta), path("split_fnas/*")   , emit: fnas
    tuple val(meta), path(".command.log")   , emit: log
    tuple val(meta), path(".command.sh")    , emit: script

    script:
    """
    ### Download fastas (aria2c)
    # download with aria2c
    aria2c \\
        --input=${urls_txt} \\
        --dir=url_fastas \\
        --max-concurrent-downloads=${task.cpus}

    ### Rename local fastas to match IDs in TSV
    # create directory for renamed local fastas
    mkdir -p local_fastas

    # create symbolic links for local fastas with new names based on TSV
    while IFS=' ' read -r target link; do
    if [[ -n "\$target" && -n "\$link" ]]; then
        # Create the symbolic link
        ln -s "\$target" "\$link"
    fi
    done < "${local_txt}"

    ### Create sketchlib sketch (sketchlib)
    # create input file of fastas
    find ./ -path '*_fastas/*' > fasta_files.txt

    # create tsv with file base name for 1st column then path for second column
    awk -F/ '{print \$NF"\\t"\$0}' fasta_files.txt > fasta_files.tsv
    sed -i 's/\\.f.*\\t/\\t/g' fasta_files.tsv

    # create sketchlib sketch
    sketchlib sketch \\
        -f fasta_files.tsv \\
        -o ${meta.genus}_k21_s1000 \\
        -k 21 \\
        -s 1000 \\
        -t ${task.cpus} \\
        -v

    ### SELF: all-v-all dist (sketchlib)
    sketchlib dist \\
        ${meta.genus}_k21_s1000.skm \\
        -o ${meta.genus}_self_dist.tsv \\
        -k 21 \\
        --ani \\
        --threads ${task.cpus} \\
        --verbose \\
        --knn 100

    ### SELF: Clusty extract unique sequences (clusty)
    clusty \\
        ${meta.genus}_self_dist.tsv \\
        ${meta.genus}_self_unique_cdhit.tsv \\
        --objects-file ${meta.genus}_objects.tsv \\
        --similarity \\
        --min ani 1.0 \\
        --out-representatives

    ### SELF: Remove redundant sketches from sketch (sketchlib)
    # identify redundant sketches
    csvtk filter2 \\
        ${meta.genus}_unique_cdhit.tsv \\
        --tabs \\
        --filter '( \$1 != \$2 )' \\
        | cut -f 1 > ${meta.genus}_rm_list.txt

    # remove redundant sketches from sketchlib
    sketchlib delete \\
        ${meta.genus}_k21_s1000.skm \\
        ${meta.genus}_rm_list.txt \\
        ${meta.genus}_k21_s1000_unique.skm \\
        -v

    ### IF UHBDB EXISTS 
    if [[ ! -f ${uhdbd_sketch} ]]; then
        clusty \\
            ${meta.genus}_self_dist.tsv \\
            ${meta.genus}_self_unique_cdhit.tsv \\
            --objects-file ${meta.genus}_objects.tsv \\ # add n50 to representativeness
            --similarity \\
            --min ani 0.9999 \\
            --out-representatives

        clusty \\
            ${meta.genus}_self_dist.tsv \\
            ${meta.genus}_self_unique_cdhit.tsv \\
            --objects-file ${meta.genus}_objects.tsv \\ # add n50 to representativeness
            --similarity \\
            --min ani 0.999 \\
            --out-representatives

        # agc

        # seqkit
    else
        ### NEW2OLD: new unique vs old unique dist (sketchlib)
        sketchlib dist \\
            ${meta.genus}_k21_s1000_unique.skm ${uhdbd_sketch} \\
            -o ${meta.genus}_new2old_dist.tsv \\
            -k 21 \\
            --ani \\
            --threads ${task.cpus} \\
            --verbose \\
            --knn 100

        ### NEW2OLD: Identify new unique sequences (clusty)
        clusty \\
            ${meta.genus}_new2old_dist.tsv \\
            ${meta.genus}_new2old_unique_cdhit.tsv \\
            --objects-file ${meta.genus}_objects.tsv \\ # add n50 to representativeness
            --similarity \\
            --min ani 1.0 \\
            --out-representatives

        clusty \\
            ${meta.genus}_new2old_dist.tsv \\
            ${meta.genus}_new2old_strain_cdhit.tsv \\
            --objects-file ${meta.genus}_objects.tsv \\ # add n50 to representativeness
            --similarity \\
            --min ani 0.9999 \\
            --out-representatives

        clusty \\
            ${meta.genus}_new2old_dist.tsv \\
            ${meta.genus}_new2old_genomovar_cdhit.tsv \\
            --objects-file ${meta.genus}_objects.tsv \\ # add n50 to representativeness
            --similarity \\
            --min ani 0.999 \\
            --out-representatives

        ### NEW2OLD: Remove redundant sketches from new sketch (sketchlib)
        # identify redundant sketches

        ### NEW2OLD: merge new unique sketches with old unique sketches (sketchlib)

        ### Create fasta file of new unique sequences (seqkit)

        ### Append new unique fasta to archive (agc)
    fi
    """
}


// Run entry workflow
workflow {

    main:
    
    // MODULE: Split by genus
    SPLITBYGENUS(
        params.input,
        params.large_threshold
    )


    // // Delete intermediate and Nextflow-specific files
    // def remove_tmp = params.remove_tmp
    // workflow.onComplete {
    //     if (output_file.exists()) {
    //         def work_dir = new File("./work/")
    //         def nextflow_dir = new File("./.nextflow/")
    //         def launch_dir = new File(".")

    //         work_dir.deleteDir()
    //         nextflow_dir.deleteDir()
    //         launch_dir.eachFileRecurse { file ->
    //             if (file.name ==~ /\.nextflow\.log.*/) {
    //                 file.delete()
    //             }
    //         }

    //         if (remove_tmp) {
    //             def tmp_dir = new File("./tmp/")
    //             tmp_dir.deleteDir()
    //         }
    //     }
    // }
}
