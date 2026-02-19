import unittest

import parser


class ParserUtilsTests(unittest.TestCase):
    def test_normalize_intel_uto_date(self):
        self.assertEqual(parser.normalize_intel_uto_date("January 2, YR2"), "January 2 of YR2")
        self.assertEqual(parser.normalize_intel_uto_date("  July 14,  YR11 "), "July 14 of YR11")
        self.assertIsNone(parser.normalize_intel_uto_date("not-a-date"))

    def test_infer_payload_coord_from_direct_value(self):
        payload = {"provinceCoord": "2:8"}
        self.assertEqual(parser.infer_payload_coord(payload, "province"), "2:8")

    def test_infer_payload_coord_from_kingdom_and_island(self):
        payload = {"targetKingdom": 2, "targetIsland": 1}
        self.assertEqual(parser.infer_payload_coord(payload, "target"), "2:1")

    def test_safe_number_parsing(self):
        self.assertEqual(parser.safe_int("1,024"), 1024)
        self.assertEqual(parser.safe_int("-17"), -17)
        self.assertIsNone(parser.safe_int("n/a"))
        self.assertEqual(parser.safe_float("57,907"), 57907.0)
        self.assertEqual(parser.safe_float(12), 12.0)
        self.assertIsNone(parser.safe_float("bad"))


if __name__ == "__main__":
    unittest.main()
