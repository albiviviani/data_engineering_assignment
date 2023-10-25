import unittest
import os
from pathlib import Path
import sys
path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))
from etl_app.main import *


class TestReadCsv(unittest.TestCase): 

    def test_read_csv(self):

        test_csv_content = """EmployeeID,FirstName,LastName,BirthDate,Department,Salary
E001,Micheal,Jordan,1963-02-17,Finance,155000
E001,Bruce,Brown,1995-08-15,Finance,255000
E001,Lebron,James,1984-12-30,Finance,355000
E001,Kemba,Walker,1990-05-08,Finance,55000
E001,Marco,Belinelli,1986-03-25,Finance,25000
"""
        with open('test.csv', 'w') as test_file :
            test_file.write(test_csv_content)

        data = read_csv('test.csv')

        expected_data = [
            {'EmployeeID': 'E001', 'FirstName': 'Micheal', 'LastName': 'Jordan', 'BirthDate': '1963-02-17', 'Department': 'Finance', 'Salary': '155000'},
            {'EmployeeID': 'E001', 'FirstName': 'Bruce', 'LastName': 'Brown', 'BirthDate': '1995-08-15', 'Department': 'Finance', 'Salary': '255000'},
            {'EmployeeID': 'E001', 'FirstName': 'Lebron', 'LastName': 'James', 'BirthDate': '1984-12-30', 'Department': 'Finance', 'Salary': '355000'},
            {'EmployeeID': 'E001', 'FirstName': 'Kemba', 'LastName': 'Walker', 'BirthDate': '1990-05-08', 'Department': 'Finance', 'Salary': '55000'},
            {'EmployeeID': 'E001', 'FirstName': 'Marco', 'LastName': 'Belinelli', 'BirthDate': '1986-03-25', 'Department': 'Finance', 'Salary': '25000'}
        ]

        self.assertEqual(data, expected_data)

        os.remove('test.csv')

if __name__ == '__main__':
    unittest.main()


