SELECT mg.*, f.*, mgs.*, iav.*, iev.*
FROM mouse_gene                                      mg
         LEFT OUTER JOIN impc_adult_viability        iav  ON iav. mouse_gene_id  = mg.  id
         LEFT OUTER JOIN impc_embryo_viability       iev  ON iev. mouse_gene_id  = mg.  id
         LEFT OUTER JOIN fusil                       f    ON f.   mouse_gene_id  = mg.  id
         LEFT OUTER JOIN mouse_gene_synonym_relation mgsr ON mgsr.mouse_gene_id  = mg.  id
         LEFT OUTER JOIN mouse_gene_synonym          mgs  ON mgs. id             = mgsr.mouse_gene_synonym_id;