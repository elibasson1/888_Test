import json

# Open result file for test
with open('result.txt') as json_file:
    result = json.load(json_file)


def test_if_only_us_country_exist():
    for country in result:
        assert (country["country"]) == "US"


def test_bet_type_status():
    for data in result:
        if (data["bet amount"]) > 100:
            assert data["bet type"] == "High"
        else:
            assert data["bet type"] == "Low"


def test_if_remove_currency_key_from_source_message():
    key = "currency"
    for data in result:
        if key in data:
            assert False, "There is an unnecessary key"
