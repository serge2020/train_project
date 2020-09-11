# Train Project

In this repo you'll find a couple of Python scripts that perform some ETL tasks on [Finnish Transport Agency](https://www.digitraffic.fi/rautatieliikenne/#yhden-junan-tiedot) train schedule data. 
* **main.py:** This script that loads data from train schedule [API endpoint](http://rata.digitraffic.fi/api/v1/trains/) , normalizes data following star schema principles and saves entity data to csv files in the project "output" folder in theproject structure.
* **avg_time.py:** This script calculates average train arrival/departure times using the loaded data.

## Data Model
Descripttion of the data returned is available at the Finnish Transport Agency site [here](https://www.digitraffic.fi/rautatieliikenne/#junat)
The *main.py* script performs ETL tasks for achieving 3N data normalization and loading it into the schema described in the trains_erd.jpg file. Aslo, denormalized table containing all the loaded data is generated and saved to csv file.

## Installation

Clone the project locally with ```bash git clone ``` 


## Usage

Project is created using Python 3.7. virtual environment and all dependencies are included within it in the *venv* folder
Make shure you have *output* folder in the project root as all csv files produced by main.py are saved there.

To run the scripts from command line:
1. Get the absolute path of ./venv/Scripts folder in the project tree - Python interpreter is located there.
2. Navigate to the project root path ./train_project
3. Use the command *<absolute_path>/venv/Scripts/python.exe <script_name>.py <arguments>* to run the scripts
# Script arguments
***main.py***:
1. Train number
2. Period start date *YYYY-MM-dd*
3. Period end date *YYYY-MM-dd*
For example to get July 2020 train schedule data for train number 4  run the command:
```bash
<absolute_path>/venv/Scripts/python.exe main.py 4 2020-07-01 2020-07-31
```
 
***avg_time.py***:
Run this script only after you have executed main.py script and csv files with schedule data are present in the output folder.
1. Source file name (arrival or departure)
2. Station code (numeric, use data in station csv for reference)
For example to average arrival time to Helsnki station (code = 1) from the data loaded by previous command  run:
```bash
<absolute_path>/venv/Scripts/python.exe avg_arrival.py arrival_20200701-20200731.csv 1
```
