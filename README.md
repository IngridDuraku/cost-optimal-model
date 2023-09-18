<div align="center">
 <h1>Cost Optimal Model for running queries in the cloud </h1>
</div>
<hr/>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project 
<hr/>

Cost Optimal Model is initially based on the <a href="https://www.cs6.tf.fau.de/files/2021/04/costoptimal.pdf">"Towards Cost-Optimal Query Processing in the Cloud"</a> paper.
M4 is the implementation of the model introduced in the paper. It predicts query runtimes based on its metadata and aws instances characteristics 
and calculates the total cost of the query on each instance. As a result, it picks the lowest cost instance to schedule the query.

This project extends the M4 model to support multitenancy: 
- ILP Model 
    - Several queries can be scheduled in the same instance.
    - Finds the configuration with the lowest cost to schedule a batch of queries. 
    - There are two versions of the ilp model, depending on how queries running in the same instance share bandwidth:
      - ILP - queries share the bandwidth equally
      - ILP_BW - bandwidth is allocated in a proportional way

### Built with
- Python 3.10
- Pandas 2.0
- MIP 1.15

## Getting Started
Please follow the instructions to set up the project in your local machine.
<hr/>

### Prerequisites
- Make sure you have Python installed in your machine (at least Python 3.7).
- Clone/download the repository.
- Please install PostgreSQL to set up a local database. 
### Installation

1. Create python virtual environment:
 ```sh
    python -m venv venv
   ```

2. Activate environment:
```sh
   source venv/bin/activate
   ```

3. Install dependencies:
```sh
   pip install -r requirements.txt
   ```
### Set Up Snowset Database
This project makes use of the snowset dataset to generate test queries for testing the models. 
Please find the details for the dataset <a href="https://github.com/resource-disaggregation/snowset">here</a>. 
1. Please download <a href=" http://www.cs.cornell.edu/~midhul/snowset/snowset-main.csv.gz">snowset-main.csv.gz</a> and extract it.
2. For faster processing, run the split_snowset.py script to split the snowset-main.csv file into smaller chunks.
```sh
   python ./models/scripts/snowset/split_snowset.py
   ```
3. Create the snowdb database and snowset table. Please find the script to create the table at: /models/scripts/snowset/snowset_table.sql
4. Import the data by running load_db.sh script. Don't forget to fill in the missing parameters first!
```sh
   bash load_db.sh
   ```
5. Create .env file in the root directory: 
```
USERNAME=<username>
PASSWORD=<pass>
HOST=127.0.0.1
DATABASE=snowdb
   ```
## Usage
<hr/>

- To run M4 model: 
```sh
   python -m run_m4
   ```

- To run ILP model: 
```sh
   python -m run_ilp
   ```

- To run ILP-BW model: 
```sh
   python -m run_ilp_bw
   ```

Note: You can find/modify the testing data in the const.py file.

- To generate realistic test queries from the snowset please run snowflake.py first. After the execution you will find the queries at /input/snowflake_queries.csv
```sh
   python -m snowflake
   ```
- To run the experiment: 
```sh
   python -m snowflake
   ```

Note: The results can be found in the /output folder after running each script.