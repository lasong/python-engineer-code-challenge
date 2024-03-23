# python-engineer-code-challenge-quix

Python engineer tech challenge for Quix

## Local setup
- Install docker and docker-compose (if you do not have it yet)
- Install Python >= 3.9.13 (if you do not have it yet)
- Go to `aggregator` folder
- Create venv: ```python3 -m venv ./venv```
- activate venv: ````source ./venv/bin/activate````
- Install requirements: ```pip install -r requirements.txt```
- Install test requirements: ```pip install -r tests/requirements.txt```

## How to run

### Running kafka and data generator using docker container
Start kafka and the data generator using docker-compose by running

```bash
docker-compose up
```

To run the data generator in multiple processes, adjust the `data-generator` service environment variable `NUM_PROCESSES` in the `docker-compose.yml` file before starting.

To stop the container, run `docker-compose stop` and to remove, `docker-compose down`.

### Running the data aggregator
To run, open another terminal and go to the aggregator folder, `cd aggregator`. Then run the commands:

```bash
source venv/bin/activate

python main.py
```

To run in multiple processes, run the commands above in different terminals.

NOTE: The multi processes can be improved by updating the 'data' topic to have multiple partitions, and modifying the code so that each process consumes data from a partition. That is one process per partition.

## how to run tests in data aggregator

In the aggregator folder, run `source venv/bin/activate` to activate venv, if not yet done. Then run `pytest`.
