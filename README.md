# Develop a Custom Processor Component Using Dynamic Plugins

## Introduction

The example illustrates the process of creating custom processor components for Redpanda Connect using the Redpanda Connect plugin framework in Python ([Plugins | Redpanda Connect](https://docs.redpanda.com/redpanda-connect/plugins/about/))

In our Redpanda custom input example, we created a custom processor to read data from AWS S3 buckets using static or dynamic configuration.

## Setup

1. Install uv for Python, more details: https://docs.astral.sh/uv/guides/install-python/ 
2. Create and activate a Python virtual environment

    ```bash
    uv venv
    source .venv/bin/activate
    ```

3. Install dependencies

    ```bash
    uv sync
    ```

## Configuration examples

### S3 object read example with static configuration

1. Update the Redpanda Connect pipeline file `connect_static.yaml`
2. Start the pipeline

    ```bash
    redpanda-connect run --rpc-plugins=./s3_monitor_input.yaml ./connect_static.yaml
    ```

### S3 object read example with dynamic configuration

We can use message metadata to set the S3 object key and bucket dynamically

1. Update the Redpanda Connect pipeline file `connect_dynamic.yaml`

2. Start the pipeline

    ```bash
    redpanda-connect run --rpc-plugins=./s3-object-read-processor.yaml ./connect_dynamic.yaml
    ```

### S3 object read example with dynamic configuration using custom s3 input component.

In the provided example, we use the custom s3 input component to periodically receive the s3 object changes and read the S3 object using the custom S3 processor

1. Update the Redpanda Connect pipeline file connect_s3_input_and_processor.yaml
2. Start the pipeline

    ```bash
    redpanda-connect run --rpc-plugins=./s3-object-read-processor.yaml ./connect_dynamic.yaml
    ```
