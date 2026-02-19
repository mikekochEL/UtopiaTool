import os
import sys
import unittest

HERE = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(HERE, ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import app


class AttackRulesTests(unittest.TestCase):
    def test_raze_in_war_does_not_count_land_loss(self):
        self.assertEqual(
            app.effective_land_impact(0, 131, "Raze", is_war_context=True),
            0,
        )

    def test_raze_outside_war_counts_loss(self):
        self.assertEqual(
            app.effective_land_impact(0, 131, "Raze", is_war_context=False),
            131,
        )

    def test_operation_kind_detection(self):
        self.assertEqual(app.classify_operation_kind("Spy on Throne", "A", "B"), "intel")
        self.assertEqual(app.classify_operation_kind("Minor Protection", "A", "A"), "support")
        self.assertEqual(app.classify_operation_kind("Night Strike", "A", "B"), "hostile")


if __name__ == "__main__":
    unittest.main()
