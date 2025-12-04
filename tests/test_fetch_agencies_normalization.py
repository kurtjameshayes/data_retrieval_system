from fetch_agencies import normalize_payload_records


def test_normalize_payload_records_flattens_dict_of_lists():
    payload = {
        "IRON": [
            {
                "ori": "UT0110000",
                "counties": "IRON",
                "agency_name": "Iron County Sheriff's Office",
            }
        ],
        "JUAB, UTAH": [
            {
                "ori": "UT0251100",
                "counties": "JUAB, UTAH",
                "agency_name": "Santaquin/Genola Police Department",
            }
        ],
    }

    records = normalize_payload_records(payload)

    assert len(records) == 2
    assert {record["ori"] for record in records} == {"UT0110000", "UT0251100"}


def test_normalize_payload_records_keeps_regular_dict_shape():
    payload = {
        "ori": "UT9990000",
        "agency_name": "Example Agency",
        "counties": "TEST",
    }

    records = normalize_payload_records(payload)

    assert isinstance(records, list)
    assert records == [payload]
