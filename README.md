## Project Structure

The project has the following directory structure:
```bash
data_engineering_assignment/
│
├── data/
│ ├── employee_details.csv
│
├── etl_app/
│ ├── main.py
│
├── tests/
│ ├── test_read_csv.py
│ ├── test_transform_data.py
│ ├── test_load_data.py
│
├── Dockerfile
│
├── docker-compose.yml
│
├── README.md
```
## Prerequisites

Before running the ETL pipeline, make sure you have the following prerequisites installed:

- [Python3.8+](https://www.python.org)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [MongoDB](https://www.mongodb.com)

## Setup

1. **Clone the Repository:**

   Clone this repository to your local machine

2. **Build Docker Containers:**

    In the project's root directory, build the Docker containers with the command:
    docker-compose up

3. **Run the ETL Pipeline:**

    The ETL pipeline will run automatically after the containers are up. It reads data from the CSV file, applies transformations, and loads it into the MongoDB database.

4. **Interact with the Database**

    Interact with the database by accessing the mongodb image on Docker with the following commands: 
    docker exec -it <docker-container-name> bash
    mongosh
    show dbs
