import pandas as pd
import csv
import re
import pymongo
import datetime
import os
import unidecode


def read_csv(filename):
    
    data = []
    with open(filename, mode='r', newline='') as file:
        csv_reader = csv.DictReader(file)
        
        # Get the header row
        header = csv_reader.fieldnames
        
        for row in csv_reader:
            # Check if the row is not empty and not the same as the header
            if not all(value == '' for value in row.values()) and not any(key == value for key, value in row.items()):
                data.append(row)

    return data

def convert_and_correct_date(date_str):

    date_str = re.sub(r'[^0-9-]', '', date_str)
    year, month, day = map(int, date_str.split('-'))

    # Normalize year in a reasonable period [1923-2005]
    year = 1923 if year < 1923 else year
    year = 2005 if year > 2005 else year

    # Nomalize month 
    month = 1 if month < 0 else month
    month = 12 if month > 12 else month

    # Correct wrongg day entry
    day = 1 if day < 1 else day
    day = 30 if day > 31 else day

    return datetime.datetime.strptime(f"{day:02d}-{month:02d}-{year:04d}","%d-%m-%Y")

def clean_and_transform_names(df):

    # Clean FirstNames
    # Remove accents
    df['FirstName'] = df['FirstName'].apply(lambda x: unidecode.unidecode(x))

    # Remove non-alphabetic characters
    df['FirstName'] = df['FirstName'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', x))

    # Extract and join capitalized words
    df['FirstName'] = df['FirstName'].apply(lambda x: ' '.join(re.findall(r'[A-Z][a-zA-Z]*', x)))

    # Find rows with a date-like pattern in LastName
    date_index = df[df['LastName'].str.match(r'\d{4}-\d{2}-\d{2}')].index

    # Process the matching rows
    for index in date_index:
        names = df.loc[index, 'FirstName'].split()
        df.loc[index, 'FirstName'] = names[0]
        # Move right only if date not already in BirthDate
        if re.match(r'\d{4}-\d{2}-\d{2}', df.loc[index, 'BirthDate']):
            pass
        else :
            df.iloc[index, df.columns.get_loc('LastName'):] = df.iloc[index, 1:-1].values
        df.loc[index, 'LastName'] = names[1]

    # Clean LastNames
    # Remove accents using unidecode library
    df['LastName'] = df['LastName'].apply(lambda x: unidecode.unidecode(x))

    # Remove non-alphabetic characters except when hyphens or single quotes are within words
    df['LastName'] = df['LastName'].str.strip().apply(lambda x: x if re.match(r"([A-Z][a-z]+)-([A-Z][a-z]+)|([A-Z]'[A-Za-z]+)", x) else re.sub(r'[^a-zA-Z\s\'\-@?]|(?<!\w)-+|-(?!\w)|(?<!\w)\'|\'(?!\w)|(?<=[A-Za-z])"(?=[A-Za-z])|\s{2,}', '', x))

    # Remove specific characters ('.', '@') from the LastName
    df['LastName'] = df['LastName'].apply(lambda x: x.replace('.', '').replace('@', 'a'))

    # Capitalize the first letter of every different word with a length greater than 2
    df['LastName'] = df['LastName'].apply(lambda x: ' '.join([word.capitalize() for word in x.split() if len(word) > 2]))

    return df

def transform_data(df):

    df.columns = df.columns.str.strip()
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Do some cleaning on FirstName and LastName columns when needed and remove any leading/trailing spaces.
    df = clean_and_transform_names(df)
    
    # Convert the BirthDate from the format YYYY-MM-DD to DD/MM/YYYY.
    df['BirthDate'] = df['BirthDate'].apply(convert_and_correct_date)

    # Merge the FirstName and LastName columns into a new column named FullName.
    df['FullName'] = df['FirstName'] + ' ' + df['LastName']

    # Calculate each employee's age from the BirthDate column using as reference Jan 1st, 2023. Add a new column named Age to store the computed age.
    reference_date = pd.to_datetime('01-01-2023', format='%d-%m-%Y')
    df['Age'] = df['BirthDate'].apply(lambda x: (reference_date - pd.to_datetime(x, format='%Y-%m-%d')).days // 365)

    # Add a new column named SalaryBucket to categorize the employees based on their salary as follows:
    # A for employees earning below 50.000
    # B for employees earning between 50.000 and 100.000
    # C for employees earning above 100.000

    df['Salary'] = df['Salary'].apply(lambda x: (re.sub(r'[^0-9-]', '', x)))
    df['Salary'] = df['Salary'].apply(lambda x: abs(int(x)))
    df['SalaryBucket'] = df['Salary'].apply(lambda x: 'A' if x < 50000 else 'C' if x > 100000 else 'B')

    # Drop columns FirstName, LastName, and BirthDate.
    df.drop(columns=['FirstName', 'LastName', 'BirthDate'], inplace=True)

    # Order columns
    column_order = ["EmployeeID","FullName","Age","Department","Salary","SalaryBucket"]

    # Create a new DataFrame with the desired column order
    df= df[column_order]

    return df

class ETLApp:
    def __init__(self, db_url, db_name, collection_name):
        self.db_url = db_url
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = pymongo.MongoClient(self.db_url)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def load_data(self, transformed_data, index_keys=None):

        try:
            # Insert the transformed data into the collection
            result = self.collection.insert_many(transformed_data)

            # Check if indexes need to be created
            if index_keys:
                for key in index_keys:
                    self.collection.create_index(key)

            return True
        except Exception as e:
            print(f"Error loading data into MongoDB: {str(e)}")
            return False

    def get_collection(self):
        return self.collection
    
    def close_connection(self):
        self.client.close()


if __name__ == "__main__":

    # Read data from the 'employee_details.csv' file
    data = read_csv("data/employee_details.csv")

    # Transform the data using a transformation function
    df = transform_data(pd.DataFrame(data, index=None))

    # Set up the MongoDB connection details
    db_url = 'mongodb://' + os.environ['MONGODB_HOSTNAME'] + ':27017/' + os.environ['MONGODB_DATABASE']
    db_name = 'data_db'
    collection_name = 'data_collection'

    # Check MongoDB connection status
    try:
        client = pymongo.MongoClient(db_url)
        client.server_info() # Check connection status
        
        # Connection is valid
        print("Connected to MongoDB.")

    except pymongo.errors.ServerSelectionTimeoutError as e:
        print(f"Error: MongoDB server is not available. Details: {e}")

    # Create an instance of the ETLApp class for MongoDB data loading
    etl_app = ETLApp(db_url, db_name, collection_name)

    # Convert the transformed data to a list of dictionaries
    transformed_data = df.to_dict(orient='records')

    # Specify index keys for the MongoDB collection
    index_keys = ["EmployeeID", "FullName", "Age", "Department", "Salary", "SalaryBucket"]

    # Load the transformed data into the MongoDB collection, creating indexes if specified
    success = etl_app.load_data(transformed_data, index_keys)

    # Print a success message or an error message based on the data loading result
    if success:
        print("Data loaded successfully.")
    else:
        print("Data loading failed.")

    # Close the MongoDB connection
    etl_app.close_connection()
