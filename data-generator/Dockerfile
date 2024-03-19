FROM python:3.10-slim

# This Dockerfile needs to be build from the parent directory to include the `lib` directory
# to its build context.

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
