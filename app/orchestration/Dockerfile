FROM prefecthq/prefect:2.14.8-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

# Add our flow code to the image
COPY flows /opt/prefect/flows

# Run our flow script when the container starts
CMD ["python3", "flows/cloud_deployment.py"]