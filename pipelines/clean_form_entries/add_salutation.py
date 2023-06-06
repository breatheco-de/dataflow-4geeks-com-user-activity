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
    """
    It will create a full name property on the payload
    """
    df['salute'] = df2['salutation'] + ' ' + df['first_name']

    return df
