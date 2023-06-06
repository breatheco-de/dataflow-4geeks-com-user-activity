import pandas as pd

expected_inputs = [
    [
        {
            'first_name': 'George',
            'last_name': 'Banks',
        }
    ],
    [
        {
            'salutation': 'Mr',
        }
    ]
]

expected_output = [{
    **expected_inputs[0][0],
    'salute': 'Mr George',
}]


def run(df, df2, stream=None):

    # create new DF with just one row that contains the same dolumns that the stream
    new_df = pd.DataFrame.from_dict({
        **stream,
        'together': [stream['first_name'] + ' ' + stream['last_name']], 
        'salutation': ['Mr']
    })

    # merge he new df with old one
    df = pd.concat([df, new_df], ignore_index=True)

    return df
