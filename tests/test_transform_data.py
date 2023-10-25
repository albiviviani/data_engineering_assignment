import unittest
import pandas as pd
from pathlib import Path
import sys
path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))
from etl_app.main import *

class TestTransformData(unittest.TestCase):
    
    sample_data = [
        {'EmployeeID': 'E001', 'FirstName': 'John Doe', 'LastName': '@ Smi#th#', 'BirthDate': '1890-06-12', 'Department': 'Finance', 'Salary': '-2500A0'},
        {'EmployeeID': 'E001', 'FirstName': 'Alice_123', 'LastName': '__Johns*on', 'BirthDate': '2990-06-12', 'Department': 'Finance', 'Salary': '50Z000'},
        {'EmployeeID': 'E001', 'FirstName': 'Mike-1988', 'LastName': 'x12 Brown', 'BirthDate': '1990-16-12', 'Department': 'Finance', 'Salary': '75000*'},
        {'EmployeeID': 'E001', 'FirstName': 'Joey ** Marciano', 'LastName': '1984-10-23', 'BirthDate': '1990-06-12', 'Department': 'Finance', 'Salary': '155#000'}]

    def setUp(self):
        data = pd.DataFrame(self.sample_data)
        self.df = transform_data(data)

    # The following two tests have been commented after using and since some columns are dropped 

    def test_clean_first_names(self):
        self.assertEqual(self.df['FirstName'].tolist(), ["John Doe", "Alice", "Mike", "Joey"])

    def test_clean_last_names(self):
        self.assertEqual(self.df['LastName'].tolist(), ["Smith", "Johnson", "Brown", "Marciano"])

    def test_merge_names(self):
        self.assertEqual(self.df['FullName'].to_list(), ["John Doe Smith", "Alice Johnson", "Mike Brown", "Joey Marciano"])

    def test_calculate_age(self):
        self.assertEqual(self.df['Age'].to_list(), [99, 17, 32, 32])

    def test_assign_salary_bucket(self):
        self.assertEqual(self.df['SalaryBucket'].to_list(), ["A", "B", "B", "C"])

# Run the tests
if __name__ == '__main__':
    unittest.main()
