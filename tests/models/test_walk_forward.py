"""Tests for walk-forward retrain/predict orchestrator."""
from datetime import date

from models.walk_forward import quarterly_cutoffs, models_dir_for


def test_quarterly_cutoffs_basic():
    """Cutoffs must be the 1st of Jan/Apr/Jul/Oct, no exceptions."""
    cuts = quarterly_cutoffs(date(2020, 1, 2), date(2021, 5, 15))
    expected = [date(2020, 4, 1), date(2020, 7, 1), date(2020, 10, 1),
                date(2021, 1, 1), date(2021, 4, 1)]
    assert cuts == expected


def test_quarterly_cutoffs_includes_quarter_start_when_start_is_quarter_start():
    cuts = quarterly_cutoffs(date(2020, 1, 1), date(2020, 7, 1))
    assert cuts == [date(2020, 1, 1), date(2020, 4, 1), date(2020, 7, 1)]


def test_quarterly_cutoffs_empty_when_end_before_start():
    assert quarterly_cutoffs(date(2024, 5, 1), date(2024, 4, 1)) == []


def test_quarterly_cutoffs_inclusive_of_end_quarter():
    """If end is 2024-12-31, cutoff list ends at 2024-10-01 (the Q4 retrain)."""
    cuts = quarterly_cutoffs(date(2024, 1, 2), date(2024, 12, 31))
    assert cuts[-1] == date(2024, 10, 1)


def test_models_dir_for_includes_iso_date():
    p = models_dir_for(date(2024, 4, 1))
    assert p.name == "2024-04-01"
    assert "ml_models" in str(p) and "walk" in str(p)
