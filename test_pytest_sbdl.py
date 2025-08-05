import pytest

from lib.Utils import get_spark_session


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")


from packaging import version

def test_blank_test(spark):
    print(spark.version)
    assert version.parse(spark.version) >= version.parse("3.4.3")



