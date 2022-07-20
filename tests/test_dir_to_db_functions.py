import pytest
import pandas as pd
from  src.dir_to_db_functions import clean_table_names

@pytest.fixture
def sample_csv_files(tmpdir):
    csv1 = tmpdir.join("csv1_Ź.csv")
    csv2 = tmpdir.join("csv2.csv")
    with open(csv1,"w") as f:
        f.write("Height,Width\n"
            "1.801,201.411\n"
            "1.767,565.112\n"
            "2.002,333.209\n"
            "1990,782.911\n"
            "1.285,389.129\n"
        )
    with open(csv2,"w") as f:
        f.write("Height,Width\n"
            "1.801,201.411\n"
            "1.767,565.112\n"
            "2.002,333.209\n"
            "1990,782.911\n"
            "1.285,389.129\n"
        )
    only_csv_files = ['csv1.csv', 'csv2.csv']    
    yield csv1, csv2, only_csv_files


def test_for_getting_csv_list(sample_csv_files):
    csv1, csv2, only_csv_files = sample_csv_files
    assert clean_table_names(only_csv_files) == ['csv1','csv2']
