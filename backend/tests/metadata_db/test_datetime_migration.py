"""Tests for datetime column migration and DICOM date/time converters."""

from __future__ import annotations

import pytest

from extract.dicom_mappings import _to_dicom_date, _to_dicom_time


class TestDicomDateConverter:
    """Tests for _to_dicom_date converter function."""

    def test_dicom_format_yyyymmdd(self):
        """Test standard DICOM date format."""
        assert _to_dicom_date("20200308") == "2020-03-08"
        assert _to_dicom_date("19991231") == "1999-12-31"
        assert _to_dicom_date("20000101") == "2000-01-01"

    def test_already_iso_format(self):
        """Test date already in ISO format."""
        assert _to_dicom_date("2020-03-08") == "2020-03-08"
        assert _to_dicom_date("1999-12-31") == "1999-12-31"

    def test_none_value(self):
        """Test None input."""
        assert _to_dicom_date(None) is None

    def test_empty_string(self):
        """Test empty string input."""
        assert _to_dicom_date("") is None
        assert _to_dicom_date("   ") is None

    def test_invalid_format(self):
        """Test invalid date formats return None."""
        assert _to_dicom_date("2020/03/08") is None
        assert _to_dicom_date("03-08-2020") is None
        assert _to_dicom_date("invalid") is None

    def test_numeric_input(self):
        """Test numeric input is converted."""
        assert _to_dicom_date(20200308) == "2020-03-08"


class TestDicomTimeConverter:
    """Tests for _to_dicom_time converter function."""

    def test_dicom_format_hhmmss(self):
        """Test standard DICOM time format without fractional seconds."""
        assert _to_dicom_time("164046") == "16:40:46"
        assert _to_dicom_time("113512") == "11:35:12"
        assert _to_dicom_time("000000") == "00:00:00"
        assert _to_dicom_time("235959") == "23:59:59"

    def test_dicom_format_with_microseconds(self):
        """Test DICOM time format with fractional seconds."""
        assert _to_dicom_time("114142.419000") == "11:41:42.419000"
        assert _to_dicom_time("131929.46") == "13:19:29.46"
        assert _to_dicom_time("084623.531000") == "08:46:23.531000"

    def test_already_formatted(self):
        """Test time already in HH:MM:SS format."""
        assert _to_dicom_time("16:40:46") == "16:40:46"
        assert _to_dicom_time("11:35:12.123456") == "11:35:12.123456"

    def test_none_value(self):
        """Test None input."""
        assert _to_dicom_time(None) is None

    def test_empty_string(self):
        """Test empty string input."""
        assert _to_dicom_time("") is None
        assert _to_dicom_time("   ") is None

    def test_invalid_format(self):
        """Test invalid time formats return None."""
        assert _to_dicom_time("invalid") is None
        assert _to_dicom_time("12345") is None  # Too short

    def test_numeric_input(self):
        """Test numeric input is converted to string first."""
        assert _to_dicom_time(164046) == "16:40:46"


class TestEdgeCases:
    """Tests for edge cases in date/time conversion."""

    def test_whitespace_handling(self):
        """Test that whitespace is properly stripped."""
        assert _to_dicom_date("  20200308  ") == "2020-03-08"
        assert _to_dicom_time("  164046  ") == "16:40:46"

    def test_boundary_dates(self):
        """Test boundary dates."""
        assert _to_dicom_date("19000101") == "1900-01-01"
        assert _to_dicom_date("29991231") == "2999-12-31"

    def test_midnight_time(self):
        """Test midnight time values."""
        assert _to_dicom_time("000000") == "00:00:00"
        assert _to_dicom_time("000000.000000") == "00:00:00.000000"
