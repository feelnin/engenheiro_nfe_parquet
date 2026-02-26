"""
Script auxiliar para gerar o ZIP de fixtures.
Execute uma vez: python tests/fixtures/make_zip.py
"""
import zipfile
from pathlib import Path

fixtures = Path(__file__).parent

with zipfile.ZipFile(fixtures / "zip_com_xmls.zip", "w", zipfile.ZIP_DEFLATED) as zf:
    zf.write(fixtures / "nfe_nfeproc.xml", "nfe_nfeproc.xml")
    zf.write(fixtures / "cte_cteproc.xml", "cte_cteproc.xml")

print("zip_com_xmls.zip criado com sucesso.")