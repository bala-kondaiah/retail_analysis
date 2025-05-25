import pytest
from lib.utility import  get_spark_session


@pytest.fixture
def spark():
    spark_session = get_spark_session("LOCAL")
    print("sparksession setup phase")
    yield spark_session
    spark_session.stop()
    print("🧹 TEARDOWN")
