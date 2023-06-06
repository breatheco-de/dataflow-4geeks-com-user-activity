import random
import time
import sys
import os
import pandas
import inspect
from colorama import Fore, Back, Style
from utils.core import (
    load_pipelines_from_project, get_params, load_pipelines_from_project, get_transformation,
    validate_transformation_params
)

pipeline, sources, path_to_stream = get_params()
if pipeline is None:
    all = load_pipelines_from_project()
    raise Exception(f'Please specify pipeline to load from the following options: ' +
                    ",".join([p['slug'] for p in all]))
else:
    pipeline = load_pipelines_from_project(pipeline)[0]
    if "sources" not in pipeline:
        raise Exception(
            f"Pipeline {pipeline['slug']} is missing sources on the YML")
    else:
        sources = pipeline['sources']


if sources is None or len(sources) == 0:
    raise Exception(
        f"Missing CSV Files to for source from when running pipeline {pipeline['slug']}.\n Hint: please specify the name of the CSV files on the project.yml")
else:
    for i in range(len(sources)):
        if ".csv" not in sources[i]:
            sources[i] = sources[i] + ".csv"


streams = []
if path_to_stream is not None:
    streams = pandas.read_csv(
        "sources/"+path_to_stream).to_dict('records')
    print(Fore.BLUE +
          f"Stream param detected and {len(streams)} streams fetched. Pipeline {pipeline['slug']} will run once for each stream ...")
else:
    streams.append(None)

dfs = []
for source in sources:
    dfs.append(pandas.read_csv("sources/"+source))

df_out = None
for stream_index in range(len(streams)):

    kwargs = {}
    if streams[stream_index] is not None:
        print(Fore.WHITE + f"[] Running pipeline with stream #{stream_index}")
        kwargs['stream'] = streams[stream_index]

    count = 0
    for t in pipeline['transformations']:
        count += 1
        print(Fore.WHITE +
              f"[] Applying {count} transformation {t} with {len(dfs)} sources...")
        run, _in, _out, _stream = get_transformation(pipeline['slug'], t)
        if df_out is not None:
            dfs[0] = df_out

        arguments = validate_transformation_params(
            run, kwargs['stream'] if 'stream' in kwargs else None)

        df_out = run(*dfs[:len(arguments) - len(kwargs.keys())], **kwargs)

    file_name = source.split('.')[0]
    if not os.path.exists("output/"):
        os.mkdir("output/")

    df_out.to_csv(
        f"output/{pipeline['slug']}{str(round(time.time(),3)).replace('.','')}.csv")
