import pytest
from pyspark.sql import Row
from chispa import assert_df_equality
from lib import DataLoader, Transformations
from datetime import datetime, date
from lib.ConfigLoader import get_config
from lib.Utils import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType, Row
import pandas as pd

@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")
@pytest.fixture(scope='session')
def expected_party_rows():
    return [Row(load_date=date(2022, 8, 2), account_id='6982391060',
                party_id='9823462810', relation_type='F-N', relation_start_date=datetime(2019, 7, 29, 6, 21, 32)),
            Row(load_date=date(2022, 8, 2), account_id='6982391061', party_id='9823462811', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 31, 5, 27, 22)),
            Row(load_date=date(2022, 8, 2), account_id='6982391062', party_id='9823462812', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 25, 15, 50, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391063', party_id='9823462813', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 11, 7, 23, 28)),
            Row(load_date=date(2022, 8, 2), account_id='6982391064', party_id='9823462814', relation_type='F-N',
                relation_start_date=datetime(2019, 6, 6, 14, 18, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391065', party_id='9823462815', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 4, 5, 12, 37)),
            Row(load_date=date(2022, 8, 2), account_id='6982391066', party_id='9823462816', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 15, 10, 39, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462817', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 16, 9, 53, 4)),
            Row(load_date=date(2022, 8, 2), account_id='6982391068', party_id='9823462818', relation_type='F-N',
                relation_start_date=datetime(2017, 11, 27, 1, 20, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462820', relation_type='F-S',
                relation_start_date=datetime(2017, 11, 20, 14, 18, 5)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462821', relation_type='F-S',
                relation_start_date=datetime(2018, 7, 19, 18, 56, 57))]

@pytest.fixture(scope='session')
def expected_contract_df(spark):
    schema = StructType([StructField('account_id', StringType()),
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             ArrayType(StructType(
                                                                 [StructField('contractTitleLineType', StringType()),
                                                                  StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())]))])

    return spark.read.format("json").schema(schema).load("test_data/results/contract_df.json")


from packaging import version

def test_blank_test(spark):
    print(spark.version)
    assert version.parse(spark.version) >= version.parse("3.4.3")



def test_get_config():
    conf_local = get_config("LOCAL")
    conf_qa =get_config("QA")
    assert conf_local["kafka.topic"]=="sbdl_kafka_cloud"
    assert conf_qa["hive.database"]=="sbdl_db_qa"

# let's test the dataframes we created in DataLoader

def test_read_accounts(spark):
    accounts_df = DataLoader.read_accounts(spark,"LOCAL",None,False)
    assert accounts_df.count()==8

def test_read_parties(spark,expected_party_rows):
    read_df = DataLoader.read_parties(spark,"LOCAL",None,False).collect()
    assert expected_party_rows == read_df

def test_get_contract(spark, expected_contract_df):
    accounts_df = DataLoader.read_accounts(spark, "LOCAL", None, False)
    actual_contract_df = Transformations.get_contract(accounts_df)
    assert expected_contract_df.collect() == actual_contract_df.collect()
    assert_df_equality(expected_contract_df, actual_contract_df, ignore_schema=True)