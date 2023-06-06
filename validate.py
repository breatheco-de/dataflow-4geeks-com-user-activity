import random
import sys
import pandas as pd
import os
import traceback

from colorama import Fore, Back, Style
from utils.core import (
    load_pipelines_from_project, get_params, get_transformation, scan_for_pipelines,
    scan_pipeline_transformations
)
from deepdiff import DeepDiff


def to_df(_lists):
    _dfs = []
    for l in _lists:
        if isinstance(l, list):
            try:
                _dfs.append(pd.DataFrame(l))
            except Exception as e:
                print(l)
                raise e
        elif isinstance(l, pd.DataFrame):
            _dfs.append(l)
        else:
            raise Exception(
                f'Invalid type {type(l)} for variable "expected_inputs" or "ouput" (expected list or DataFrame)')
    return _dfs


def to_list(_dfs):

    if isinstance(_dfs, pd.DataFrame):
        return [_dfs.to_dict('records')]

    if isinstance(_dfs, list) and len(_dfs) > 0:
        if isinstance(_dfs[0], dict):
            _dfs = [_dfs]

    _lists = []
    for df in _dfs:
        if isinstance(df, list):
            _lists.append(df)
        elif isinstance(df, pd.DataFrame):
            print("trying to make dfs a datagrame")
            _lists.append(df.to_dict('records'))
        else:
            raise Exception(
                f'Invalid type {type(df)} for variable "expected_inputs" or "ouput" (expected list or DataFrame)')
    return _lists


def validate_trans(q, t, _errors):
    try:
        run, _in, _out, _stream = get_transformation(q, t)

        if isinstance(_in, pd.DataFrame):
            _in = [_in]

        # list of dicts
        if len(_in) > 0 and isinstance(_in[0], dict):
            _in = [_in]

        # input its a list of lists
        if len(_in) > 0 and not isinstance(_in[0], pd.DataFrame):
            _in = to_df(_in)

        if _in is None or (isinstance(_in, list) and len(_in) == 0):
            raise Exception("Transformation expected_inputs are empty")

        # protect in from mutations
        in_backup = [df.copy() for df in _in]

        if _stream is not None:
            _stream = to_list(_stream)[0]
            print(_stream)
            print(
                Fore.BLUE + f'Found {len(_stream)} streams to validate, will run transformation {len(_stream)} times')
        else:
            _stream = [None]

        output = None
        buffer = None
        for stream_index in range(len(_stream)):

            kwargs = {}
            if _stream[stream_index] is not None:
                print(Fore.BLUE + f' Stream {stream_index} ...')
                kwargs['stream'] = _stream[stream_index]

            if buffer is not None:
                _in[0] = buffer

            output = run(*_in, **kwargs)
            if output is None:
                raise Exception("Transformation needs to return a dataset")
            buffer = output
            output = output.to_dict('records')

        # just in case the _in variable has mutated
        _in = in_backup

        in_out_same = DeepDiff(to_list(_in)[0], to_list(_out)[0], ignore_nan_inequality=True)

        if len(output) > 0:
            output = to_list(output)[0]

        if len(_out) > 0:
            _out = to_list(_out)[0]

        if len(output) == 0 and len(_out) > 0:
            raise Exception(
                'Transformation output its empty but you were expecting more')

        diff = DeepDiff(output, _out, ignore_nan_inequality=True)
    except Exception as e:
        _errors[q + '.' + t] = e

    if (q + '.' + t) in _errors:
        print(Fore.RED + q + '.' + t + ' ❌', end='')
    elif len(diff.keys()) != 0:
        print(Fore.RED + q + '.' + t + ' ❌', end='')
        _errors[q + '.' +
                t] = "\n".join(f"{k}: {v} \n" for k, v in diff.items())
    elif len(in_out_same.keys()) == 0:
        print(Fore.RED + q + '.' + t + ' ❌', end='')
        _errors[q + '.' + t] = 'The expected_inputs and expected_output variables have the same values'
    else:
        print(Fore.GREEN + q + '.' + t + ' ✅', end='')

    print(Style.RESET_ALL)
    print('')

    return _errors


# _stream_path is ignored because we are unit testing
pipeline, sources, _stream_path = get_params()
errors = {}
pipelines = load_pipelines_from_project()


for pipe in pipelines:

    if pipeline is not None and pipeline != pipe['slug']:
        continue

    if "sources" not in pipe:
        raise Exception(
            f"Pipeline {pipe['slug']} is missing sources on the YML")

    if "destination" not in pipe:
        raise Exception(
            f"Pipeline {pipe['slug']} is missing destination on the YML")
    elif isinstance(pipe["destination"], list):
        raise Exception(
            f"Pipeline {pipe['slug']} destinatino cannot be a list, you can only output to one destination")

    print('\n\nStarting to validate every pipeline and transformation...\n')
    transformations = scan_pipeline_transformations(pipe['slug'])
    for t in transformations:
        errors = validate_trans(pipe['slug'], t, errors)

if len(errors) > 0:
    print('\n')
    print(Back.RED + f'Report: {len(errors)} errors found:', end='')
    print(Style.RESET_ALL)
    print('\n')
    count = 0
    for e in errors:
        count += 1
        print(Fore.RED + f'{count}) In {e}: ', end='')
        print(errors[e])
        if not isinstance(errors[e], str):
            traceback.print_exception(
                type(errors[e]), errors[e], errors[e].__traceback__)

    print(Style.RESET_ALL + '\n')
    exit(1)
else:
    print(Back.GREEN +
          f'Report: All {len(pipelines)} transformations return the expected outputs 🙂', end='')
    print(Style.RESET_ALL)
    exit(0)
