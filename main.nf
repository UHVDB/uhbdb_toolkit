#!/usr/bin/env nextflow

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    UHVDB/uhbdb_updater
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : https://github.com/UHVDB/uhbdb_updater
----------------------------------------------------------------------------------------
*/

// MODULE: Split by genus (label as large )

// FUNCTION: Split into large (> 10,000 sequences) and small (< 10,000 sequences) genera

// MODULE: UHBDB_UPDATER
process UHVDB_UPDATER {
    label 'process_high'

    input:
    tuple val(meta), path(fna)

    output:
    tuple val(meta), path("split_fnas/*")   , emit: fnas
    tuple val(meta), path(".command.log")   , emit: log
    tuple val(meta), path(".command.sh")    , emit: script

    script:
    """
    seqkit split2 \\
        ${fna} \\
        --threads ${task.cpus} \\
        --by-size ${params.chunk_size} \\
        --out-dir split_fnas \\
        --extension ".gz"
    """
}


// Run entry workflow
workflow {

    main:
    // Check if output file already exists
    def output_file = file("${params.output}")
    def input_fna = channel.fromPath(params.input_fna).map { fna ->
            [ [ id: "${fna.getBaseName()}" ], fna ]
        }

    if (!output_file.exists()) {

        // Split input FNA into chunks
        SEQKIT_SPLIT2(
            channel.fromPath(params.input_fna).map { fna ->
                [ [ id: "${fna.getBaseName()}" ], fna ]
            }
        )

        ch_split_fnas = SEQKIT_SPLIT2.out.fnas
            .map { _meta, fnas -> fnas }
            .flatten()
            .map { fna ->
                [ [ id: fna.getBaseName() ], fna ]
            }

        // Create kmer-db database from input FNA
        KMERDB_BUILD(input_fna)

        // Align split FNAs against kmer-db database
        ALIGN(
            ch_split_fnas,
            KMERDB_BUILD.out.kmerdb.join(input_fna).collect()
        )

        // Combine ANI scores
        COMBINEANIS(
            ALIGN.out.parquet.map { _meta, parquet -> [ [ id:'combined'], parquet ] }.groupTuple(sort:'deep')
        )

        if ( (params.cluster == 'mcl') || (params.cluster == 'MCL') ) {
            // Cluster combined ANI scores with MCL
            MCL(
                COMBINEANIS.out.tsv
            )
        } else if ( (params.cluster == 'clusty') || (params.cluster == 'Clusty') ) {
            CLUSTY(
                COMBINEANIS.out.tsv,
                ALIGN.out.ids.first()
            )
        }

    } else {
        println "[UHVDB/nucleotidecluster]: Output file [${params.output}] already exists!"
    }

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
