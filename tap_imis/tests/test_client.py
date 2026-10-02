"""Tests for IMISStream pagination and incremental query helpers."""

from tap_imis.client import imis_next_page_token, imis_replication_filter, iter_imis_list_records


def test_imis_next_page_token_respects_has_next():
    assert imis_next_page_token({"HasNext": True, "NextOffset": 100}) == 100
    assert imis_next_page_token({"HasNext": False, "NextOffset": 100}) is None


def test_iter_imis_list_records_reads_values_envelope():
    body = {"Items": {"$values": [{"PartyId": "1"}, {"PartyId": "2"}]}}
    assert list(iter_imis_list_records(body)) == [{"PartyId": "1"}, {"PartyId": "2"}]


def test_imis_replication_filter_uses_ge_syntax():
    assert imis_replication_filter("UpdatedOn", "2024-01-01T00:00:00+00:00") == {
        "UpdatedOn": "ge:2024-01-01T00:00:00+00:00",
    }
    assert imis_replication_filter(None, "2024-01-01") == {}
    assert imis_replication_filter("UpdatedOn", None) == {}
