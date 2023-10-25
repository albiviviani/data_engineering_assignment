import unittest
from pathlib import Path
import sys
path_root = Path(__file__).parents[1]
sys.path.append(str(path_root))
from etl_app.main import *


class TestLoadmData(unittest.TestCase):

    def test_load_data(self):

        db_url = "mongodb://localhost:27017/"
        db_name = 'data_db'
        collection_name = 'data_collection'
        etl_app = ETLApp(db_url, db_name, collection_name)

        # Test DataFrame with sample data
        sample_data = [{
            'EmployeeID': 'E001',
            'Department': 'Finance',
            'Salary': '55000',
            'FullName': 'Alice White',
            'Age': '32',
            'SalaryBucket': 'B',
        }, 
        {
            'EmployeeID': 'E027',
            'Department': 'IT',
            'Salary': '60000',
            'FullName': 'Marylin Monroe',
            'Age': '44',
            'SalaryBucket': 'C',
        }]

        index_keys = ["EmployeeID","FirstName","LastName","BirthDate","Department","Salary"] 

        # Load data on the database and collection

        etl_app.load_data(sample_data, index_keys)

        # Retrieve data from the collection

        collection = etl_app.get_collection()

        result = list(collection.find())

        # Check if the data is loaded

        self.assertEqual(result, sample_data)

        # Drop database and close connection

        etl_app.client.drop_database(etl_app.db_name)
        
        etl_app.close_connection()

# Run the test
if __name__ == '__main__':
    unittest.main()