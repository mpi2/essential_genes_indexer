SELECT age.*, clin.*, gnp.*, hgnc.*, hg.*, hgs.*, idg.*
FROM human_gene hg
         LEFT OUTER JOIN achilles_gene_effect        AS age  ON age. human_gene_id  = hg.  id
         LEFT OUTER JOIN clingen                     AS clin ON clin.human_gene_id  = hg.  id
         LEFT OUTER JOIN gnomad_plof                 AS gnp  ON gnp. human_gene_id  = hg.  id
         LEFT OUTER JOIN hgnc_gene                   AS hgnc ON hgnc.human_gene_id  = hg.  id
         LEFT OUTER JOIN human_gene_synonym_relation AS hgsr ON hgsr.human_gene_id  = hg.  id
         LEFT OUTER JOIN human_gene_synonym          AS hgs  ON hgs. id             = hgsr.human_gene_synonym_id
         LEFT OUTER JOIN idg                                 ON idg. id             = hg.  id;