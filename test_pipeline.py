import pytest
from bitcoin_price_pipeline import validate_data, clean_data

# Sample valid API response
valid_sample = {"bitcoin": {"usd": 67234.56}}

# Sample invalid responses
invalid_no_bitcoin = {"ethereum": {"usd": 3000}}
invalid_wrong_type = {"bitcoin": {"usd": "not_a_number"}}
invalid_missing_usd = {"bitcoin": {}}

def test_validate_data_valid():
    is_valid, message = validate_data(valid_sample)
    assert is_valid is True
    assert message == "Data is valid"

def test_validate_data_none():
    is_valid, message = validate_data(None)
    assert is_valid is False
    assert "No data received" in message

def test_validate_data_missing_bitcoin():
    is_valid, _ = validate_data(invalid_no_bitcoin)
    assert is_valid is False

def test_validate_data_wrong_type():
    is_valid, _ = validate_data(invalid_wrong_type)
    assert is_valid is False

def test_validate_data_missing_usd():
    is_valid, _ = validate_data(invalid_missing_usd)
    assert is_valid is False

def test_clean_data():
    cleaned = clean_data(valid_sample)
    assert "timestamp" in cleaned
    assert cleaned["price_usd"] == 67234.56
    # Timestamp should be a string in ISO format
    assert isinstance(cleaned["timestamp"], str)