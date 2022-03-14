import os
import sys
import unittest
from tests import TEST_CASES
from main import main

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class GoodsTestCase(unittest.TestCase):

    def setUp(self):
        """Начальные условия для тестов."""
        self.test_cases = TEST_CASES

    def test_data_for_query(self):
        """Тест получения данных о товаре из словаря."""
        for test_case in self.test_cases:
            test_input = test_case.get("test_input")
            expected = test_case.get("expected")
            self.assertEqual(main(test_input), expected)
