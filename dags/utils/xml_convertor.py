import pandas as pd


def dataframe_to_xml(df, output_file):

    df.to_xml(
        output_file,
        root_name="records",
        row_name="record",
        index=False
    )