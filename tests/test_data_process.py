import unittest
import pandas as pd
import xml.etree.ElementTree as ET
from extraction.data_process import (
    format_date,
    get_efisc_codes,
    process_dates,
    process_depot,
    extract_data
)

class TestExtractionFunctions(unittest.TestCase):

    def test_format_date(self):
        """Test de la fonction format_date."""
        self.assertEqual(format_date("2024-06-15T12:34:56"), "2024-06-15")
        self.assertEqual(format_date(None), None)
        self.assertEqual(format_date(""), None)

    def test_get_efisc_codes(self):
        """Test que get_efisc_codes retourne un dictionnaire valide."""
        efisc_codes = get_efisc_codes()
        self.assertIsInstance(efisc_codes, dict)
        self.assertIn("siren_societe", efisc_codes)
        self.assertEqual(efisc_codes["siren_societe"], "909475")

    def test_process_dates(self):
        """Test du traitement des dates et calcul du millésime."""
        df = pd.DataFrame({
            "dateDebPer": ["2023-01-01T00:00:00", "2024-01-01T00:00:00"],
            "dateFinPer": ["2023-12-31T00:00:00", "2024-12-31T00:00:00"]
        })
        
        df_processed = process_dates(df)
        
        self.assertEqual(df_processed.loc[0, "datedebut"], "2023-01-01")
        self.assertEqual(df_processed.loc[0, "datefin"], "2023-12-31")
        self.assertEqual(df_processed.loc[0, "millesime_calcule"], 23)
        self.assertEqual(df_processed.loc[1, "millesime_calcule"], 24)

    def test_process_depot(self):
        """Test du traitement d'un dépôt XML."""
        xml_data = """<root>
            <depot>
                <enteteDepot siren="123456789" idDepot="DEP001" dateDebPer="2023-01-01" dateFinPer="2023-12-31" />
                <formulaire noForm="2058CG">
                    <efisc iRepEF="1" cdEfisc="909475" vlEfisc="ABC123"/>
                </formulaire>
            </depot>
        </root>"""
        
        root = ET.fromstring(xml_data)
        depot = root.find(".//depot")
        result = process_depot(depot)
        
        self.assertIsInstance(result, list)
        self.assertEqual(result[0]["mere_siren"], "123456789")
        self.assertEqual(result[0]["id_depot"], "DEP001")
        self.assertEqual(result[0]["siren_societe"], "ABC123")

    def test_extract_data(self):
        """Test de l'extraction des données depuis un fichier XML."""
        xml_data = """<root>
            <depot>
                <enteteDepot siren="123456789" idDepot="DEP001" dateDebPer="2023-01-01" dateFinPer="2023-12-31" />
                <formulaire noForm="2058CG">
                    <efisc iRepEF="1" cdEfisc="909475" vlEfisc="ABC123"/>
                </formulaire>
            </depot>
        </root>"""

        with open("test.xml", "w") as f:
            f.write(xml_data)

        df, error = extract_data("test.xml")
        
        self.assertIsNone(error)
        self.assertFalse(df.empty)
        self.assertEqual(df.loc[0, "mere_siren"], "123456789")
        self.assertEqual(df.loc[0, "siren_societe"], "ABC123")

if __name__ == "__main__":
    unittest.main()