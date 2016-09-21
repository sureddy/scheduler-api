def test_authorized_call(client, test_user):
    r = client.get("/cwl/", headers=test_user)
    print r.data
    assert r.status_code == 200


def test_unauthorized_call(client, test_user):
    r = client.get("/cwl/")
    assert r.status_code == 403