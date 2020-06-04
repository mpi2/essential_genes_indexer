DSMAP = {
    "gene": [
        {
            "mouse": {
                "id": "id",
                "public_id": "mgi_id",
                "data_sources": [
                    {
                        "impc_adult_viability": {
                            "col_id": "mouse_gene_id",
                            "prefix": "impc_adu_via",
                            "exclude": ""
                        },
                    },
                    {
                        "impc_embryo_viability": {
                            "col_id": "mouse_gene_id",
                            "prefix": "impc_emb_via",
                            "exclude": ""
                        }
                    },
                    {
                        "mouse_gene_synonym_relation": {
                            "col_id": "mouse_gene_id",
                            "prefix": "mouse_gen_syn_rel",
                            "exclude": ""
                        }
                    }
                ]
            }
        }
    ]
}
