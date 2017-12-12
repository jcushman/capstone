import os
import pytest
from rest_framework.request import Request
from test_data.factories import setup_case
from capapi import serializers


@pytest.mark.django_db(transaction=True)
def test_MetaCaseSerializer(api_url, api_request_factory, auth_client, case):
    # can get single case data
    url = os.path.join(api_url, "cases")
    request = api_request_factory.get(url)
    serializer_context = {'request': Request(request)}

    serializer = serializers.MetaCaseSerializer(data=case, context=serializer_context)
    serializer.is_valid()
    assert serializer.data['slug'] == case.slug
    assert 'casebody' in serializer.data.keys()

    # can get multiple cases' data
    cases = []
    for c in range(0, 3):
        case = setup_case()
        cases.append(case)

    serializer = serializers.MetaCaseSerializer(data=cases, many=True, context=serializer_context)
    serializer.is_valid()
    assert len(serializer.data) == 3
    for case in serializer.data:
        assert 'casebody' in case.keys()