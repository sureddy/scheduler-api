def test_authorized_call(client, test_user):
    r = client.get("/jobs/", headers=test_user)
    print r.data
    assert r.status_code != 403


def test_unauthorized_call(client, test_user):
    r = client.get("/jobs/")
    assert r.status_code == 403