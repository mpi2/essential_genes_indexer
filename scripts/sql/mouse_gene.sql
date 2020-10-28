SELECT mg.*, f.*, mgs.*, cav.*, iev.*
FROM mouse_gene                                      mg
         LEFT OUTER JOIN combined_adult_viability    cav  ON cav. mouse_gene_id  = mg.  id
         LEFT OUTER JOIN impc_embryo_viability       iev  ON iev. mouse_gene_id  = mg.  id
         LEFT OUTER JOIN fusil                       f    ON f.   mouse_gene_id  = mg.  id
         LEFT OUTER JOIN mouse_gene_synonym_relation mgsr ON mgsr.mouse_gene_id  = mg.  id
         LEFT OUTER JOIN mouse_gene_synonym          mgs  ON mgs. id             = mgsr.mouse_gene_synonym_id;