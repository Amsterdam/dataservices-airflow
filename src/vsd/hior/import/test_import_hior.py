from django.test import TestCase
from . import import_hior


class TestHiorImport(TestCase):
    def test_get_value(self):
        response = import_hior.get_value({"x": 1}, "x")
        assert response == "1"

        response = import_hior.get_value({"x": "a'b'c"}, "x")
        assert response == "'a\"b\"c'"

        response = import_hior.get_value({"x": 'a"b"c'}, "x")
        assert response == "'a\"b\"c'"

    def test_get_insert(self):
        response = import_hior.get_insert("TABLE", [{"f1": 1, "f2": 2}], ["f1", "f2"])
        assert (
            response
            == f"""
INSERT INTO TABLE
    (f1, f2)
VALUES
    (1, 2);"""
        )

        response = import_hior.get_insert(
            "TABLE", [{"f1": 1, "f2": 2}, {"f1": 3, "f2": 4}], ["f1", "f2"]
        )
        assert (
            response
            == f"""
INSERT INTO TABLE
    (f1, f2)
VALUES
    (1, 2),
    (3, 4);"""
        )

    def test_import_row(self):
        series = {
            "Kerntekst": "kerntekst",
            "Toelichting": "toelichting",
            "Thema": "thema",
            "Subthema 1": "subthema 1   ",
            "Subthema 2": "subthema 2",
            "Stadsdeel": "stadsdeel   ",  # Should be stripped
            "Type.": "type.",
            "Niveau ": "niveau ",
            "(bestuurlijke)  bron ": "(bestuurlijke)  bron ",
            "Afbeelding 1": "afbeelding 1",
            "Afbeelding 2": "afbeelding 2",
            "Afbeelding 3": "afbeelding 3",
            "Afbeelding 4": "afbeelding 4",
            "Afbeelding 5": "afbeelding 5",
            "Download 1": "/download 1",
            "Download 2": "\download 2",  # convert to /
        }
        response = import_hior.import_row(1, series)
        assert response == (
            {"id": 3, "text": "kerntekst", "description": "toelichting"},
            [
                {"item_id": 3, "name": "Theme", "value": "Thema"},
                {"item_id": 3, "name": "Theme", "value": "Subthema 1"},
                {"item_id": 3, "name": "Theme", "value": "Subthema 2"},
                {"item_id": 3, "name": "Area", "value": "Stadsdeel"},
                {"item_id": 3, "name": "Type", "value": "Type."},
                {"item_id": 3, "name": "Level", "value": "Niveau"},
                {"item_id": 3, "name": "Source", "value": "(Bestuurlijke)  Bron"},
            ],
            [
                {"item_id": 3, "name": "Image", "value": "afbeelding 1"},
                {"item_id": 3, "name": "Image", "value": "afbeelding 2"},
                {"item_id": 3, "name": "Image", "value": "afbeelding 3"},
                {"item_id": 3, "name": "Image", "value": "afbeelding 4"},
                {"item_id": 3, "name": "Image", "value": "afbeelding 5"},
                {"item_id": 3, "name": "Link", "value": "/download 1"},
                {"item_id": 3, "name": "Link", "value": "/download 2"},
                {"item_id": 3, "name": "SourceLink", "value": "(bestuurlijke)  bron"},
            ],
        )

        # Missing kerntekst => row skipped
        kerntekst = series["Kerntekst"]
        series["Kerntekst"] = ""
        response = import_hior.import_row(1, series)
        assert response == ({}, [], [])
        series["Kerntekst"] = kerntekst

        # Remove leading number for levels
        response = import_hior.import_row(1, series)
        niveau = series["Niveau "]
        series["Niveau "] = "12. " + niveau
        assert response == (
            {"id": 3, "text": "kerntekst", "description": "toelichting"},
            [
                {"item_id": 3, "name": "Theme", "value": "Thema"},
                {"item_id": 3, "name": "Theme", "value": "Subthema 1"},
                {"item_id": 3, "name": "Theme", "value": "Subthema 2"},
                {"item_id": 3, "name": "Area", "value": "Stadsdeel"},
                {"item_id": 3, "name": "Type", "value": "Type."},
                {"item_id": 3, "name": "Level", "value": "Niveau"},
                {"item_id": 3, "name": "Source", "value": "(Bestuurlijke)  Bron"},
            ],
            [
                {"item_id": 3, "name": "Image", "value": "afbeelding 1"},
                {"item_id": 3, "name": "Image", "value": "afbeelding 2"},
                {"item_id": 3, "name": "Image", "value": "afbeelding 3"},
                {"item_id": 3, "name": "Image", "value": "afbeelding 4"},
                {"item_id": 3, "name": "Image", "value": "afbeelding 5"},
                {"item_id": 3, "name": "Link", "value": "/download 1"},
                {"item_id": 3, "name": "Link", "value": "/download 2"},
                {"item_id": 3, "name": "SourceLink", "value": "(bestuurlijke)  bron"},
            ],
        )
        series["Niveau "] = niveau

        # Fail on missing properties
        stadsdeel = series["Stadsdeel"]
        series["Stadsdeel"] = ""
        response = import_hior.import_row(1, series)
        assert response == ({}, [], [])
        series["Stadsdeel"] = stadsdeel

    def test_import_row(self):
        series = {"Vraag": "  vraag  ", "Antwoord": "  antwoord  "}
        response = import_hior.import_faq_row(1, series)
        assert response == {"id": 3, "question": "vraag", "answer": "antwoord"}

        # Skip empty questions or empty answers
        series = {"Vraag": "  ", "Antwoord": "  toelichting  "}
        response = import_hior.import_faq_row(1, series)
        assert response == {}

        series = {"Vraag": "Vraag", "Antwoord": "   "}
        response = import_hior.import_faq_row(1, series)
        assert response == {}
