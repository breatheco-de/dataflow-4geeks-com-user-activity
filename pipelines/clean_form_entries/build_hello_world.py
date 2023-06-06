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
