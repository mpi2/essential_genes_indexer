SELECT o.*, hg.*, hmf.*, mg.*, mmf.*
FROM ortholog o
         LEFT OUTER JOIN mouse_gene                  AS mg  ON mg. id            = o. mouse_gene_id
         LEFT OUTER JOIN mouse_mapping_filter        AS mmf ON mmf.mouse_gene_id = mg.id
         LEFT OUTER JOIN human_gene                  AS hg  ON hg. id            = o. human_gene_id
         LEFT OUTER JOIN human_mapping_filter        AS hmf ON hmf.human_gene_id = hg.id;