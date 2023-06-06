# What is Dataflow

<img align="right" src="https://user-images.githubusercontent.com/426452/202270773-8569adeb-7909-4498-b9f5-185242e5680c.png" width="500" />

It's a very simple minimalistic project to clean and deploy datasets in real time using python. Its ideal for small and mid-sized organizations that want to deploy into production a data-processing solution with very few steps and cheap costs. It supports **[batch](#running-in-batch)** and **[streaming](#streaming-data-into-pipelines)** processing and it comes with a tool to [quickly test pipline](#unit-testing-your-pipeline).

Deploy in heroku in minutes, create pipelines of data with multiple python functions to clean your dataset and save it into CSV, SQL or BigQuery.

## What is this project template?

This template is meant to be used for building data pipelines that can later be easily published into production using the [Dataflow Core](https://github.com/breatheco-de/dataflow). This template was built with the purpose that you don't need the dataflow core for creating and testing your pipeline in localhost.

You can independently work in your data pipeline as long as you need, we are trying to GUARANTEE that running your pipeline locally using CSV files will be a reliable way of testing and that not bugs should occur once you deploy it into production.

# Quick Start:

1. Clone this repo into your computer (or open it on gitpod).
2. Add your sample dataset's that will be used as inputs for your transformations into the `./sources` folder.
3. Create a new folder inside `./pipelines` with the name of your pipeline.
4. Add your transformations into the recently created folder `./pipelines/<pipeline_name>/`.
5. Configure the project.yml to specify the pipline and transformations in the order you want to execute them. Each pipeline must have at least one source and only one destination. You can have multiple sources if needed.
6. Add new transformation files as you need them, make sure to include `expected_inputs` and `expected_output` as examples. The expected inputs can be an array of dataframes for multiple sources.
7. Update your project.yml file as needed to change the order of the transformations.
8. Validate your transformations running `$ pipenv run validate`.
9. Run your pipline by running `$ pipenv run pipeline --name=<pipeline_slug>`
10. If you need to clean your outputs :`$ pipenv run clear`

## Examples

We have already deployed two pipelines into production and they seem to be working fine so far:

- [Events and Workshops](https://github.com/4GeeksAcademy/dataflow-project-events): Dashboard for displaying the company event and workshops activity, all pipelines are batch.
- [Form Entries](https://github.com/4GeeksAcademy/dataflow-project-form_entry) is a pipeline to process incoming marketing leads, it was created for building a dashboard that includes a dozen metrics, all pipelines are batch.
- [Public Support Chat](https://github.com/4GeeksAcademy/dataflow-project-public-support): Monitor support messages and track support agents, [here are the requirements](https://github.com/4GeeksAcademy/About-4Geeks-Academy/issues/3542), all pipelines are batch.

## Project.yml

All the project configuration is done in one single file, here is an example with comments:

```yml
name: "Lead Collection"
# You can have as many pipelines as you want
pipelines:
    # Unique name for your pipeline
  - slug: "clean_form_entries"
    # During local development these are CSV files located at te ./sources folder.
    sources:
      # The order is relevant, this sources will be passed as parameters to the run() function in the same order
      - form_entries
      - salutations
    # Ignored during local development, its the datasource to wich the pipeline data will be saved to
    destination: datasource_csv
    # Order is relevant, they will execute one on top of the other.
    transformations:
      - build_hello_world
      - add_salutation
```

## Sources

Dataflow can retrieve or store datasets of information from and into CSV files, SQL Databases and Google BigQuery. 

In this template you will find a [sources folder](/tree/main/sources) were you can put your CSV files that will simulate the production databases or datasets.

You will also find an [output folder]([/tree/main/sources](https://github.com/breatheco-de/dataflow-project-template/tree/main/output)) that will be automatically filled by the dataflow template each time you run one of your project pieplines using the `$ pipenv run pipeline --name=<pipeline_slug>` command.

## Pipelines

A pipeline is all the steps needed to clean an incoming source dataset and save it into another dataset.

- One pipeline is comprised with one or many data **transformations**.
- One pipeline has one or more sources of information (in batch or streaming).
- One piepline has one destination dataset.

## Transformations

Each transformation receives one or many dataframes and must return the cleaned version that will be passed into the next transformation or will become the pipeline output (if it was the last transformation in the pipeline).

```py
import pandas as pd
import numpy as np

def run(df, df2, df3...):
    # make some transformations here
    return df
```

## Unit testing your pipeline

The command to run your tests is `pipenv run validate`.  
Each transformation must have two variables: `expected_inputs` and `expected_output`.  

For example, let's say we want to create a transformation that receives a Pandas dataset with two columns (first_name and last_name) and creates a new column called `together` with the combination of the values in the other two. Here is the input and output to properly validate the transformation:

```py
expected_inputs = [[{
    'first_name': 'Hello',
    'last_name': 'World',
}], []]

expected_output = [{
    **expected_inputs[0][0],
    'together': 'Hello World',
}]


def run(df, df2):
    """
    It will create a full name property on the payload
    """
    df['together'] = df['first_name'] + ' ' + df['last_name']

    return df
```

When you run the `pipenv run validate` command, Dataflow will run the transformation function while passing it the specified inputs, then, it perform a deep comparison between the returned dataframe and the expected_output. If both contain similar values the validation will succeed.

## Running your pipeline

Once the pipeline transformations have been validated (unit tested) its time to test-run your pipeline as a whole using a real dataset, while working in locally dataflow will use the `./sources` folder to simulate the DataSources as CSV files. That means that for this example we must have two CSV files: `./sources/form_entries.csv` and `./sources/salutations.csv`.

When you run the `pipenv run pipeline --name=clean_form_entries` command, dataflow will fetch those two CSV files and pass them as dataframe parameters to the pipeline transformations. 

> Important note: The first dataframe on every transformation should be your cleaning target because it gets passed on from transformation to transformation as a buffer, ideally this first dataframe will become the output of your entire pipeline.

### Running in Batch

By default, pipelines run in batch, which basically means that one (or more) entire dataset is sent to the transformation queue to be cleaned.

The following command runs a pipeline in batch mode:

```bash
pipenv run pipeline --name=clean_form_entries
```

### Streaming data into pipelines

Sometimes you need to process a single incoming item into the dataset, instead of cleaning the whole dataset again you only want to clean that single item before adding it to the dataset (one at a time). This is what we call a `stream`.

To simulate a stream of data you can define a csv file where each row will behave like a single stream as it will come into the pipeline (one at a time). The entire pipeline transformation queue will be called for each stream of data.

The following command allows you to run `clean_publicsupport_fs_messages` as many times as rows found on `stream_sample.csv`:

```bash
pipenv run pipeline --name=form_entries_stream --stream=stream_sample.csv
```

Note: `--stream` is the path to a csv file that contains all the streams you want to process, if the CSV contains multiple rows, each of them will be considered a separate stream and the pipeline will run once for each stream.

> AGAIN: Dataflow will run each pipeline as many times as streams are found inside the `stream_sample.csv` file.

#### Declaring the stream parameter in the transformation

Make sure to specify one last optional paramter called `stream` in the transformation function, the incoming stream of data will be a dictionary with the exact payload received in the HTTP post request.

```py
import pandas as pd
import numpy as np

def run(df, df2, stream=None):
    # ...
    return df
```
