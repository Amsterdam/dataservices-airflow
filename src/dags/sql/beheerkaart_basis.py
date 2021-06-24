from typing import Final

# The source defines a different name for the ID column
# before insert data the source column name must be present
# after insert it can be translated by the provenance operator
RENAME_COLS: Final = """
ALTER TABLE pte.bkt_bgt RENAME COLUMN id TO bk_bkt_bgt;
ALTER TABLE pte.bkt_eigendomsrecht RENAME COLUMN id TO bk_bkt_eigendomsrecht;
ALTER TABLE pte.bkt_beheerkaart_basis RENAME COLUMN id TO bk_bkt_beheerkaart_basis;
"""
