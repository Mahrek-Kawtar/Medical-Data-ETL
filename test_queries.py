import pytest
import pandas as pd
from querying_filtering import get_visits_for_patient, filter_visits_by_diagnosis_or_date, visits_per_month, average_visits_per_patient


def test_get_visits_for_patient():
    patient_id = 'P001'
    result = get_visits_for_patient(patient_id)
    assert result is not None
    assert len(result) > 0
    assert 'patient_id' in result.columns
    assert result['patient_id'].iloc[0] == patient_id

def test_filter_visits_by_diagnosis_or_date():
    # Tester par diagnostic
    result = filter_visits_by_diagnosis_or_date(diagnosis='Depression')
    assert result is not None
    assert len(result) > 0
    assert 'Depression' in result['diagnosis'].values

    # Tester par plage de dates
    result = filter_visits_by_diagnosis_or_date(start_date='2023-01-01', end_date='2023-03-01')
    assert result is not None
    assert len(result) > 0
    assert pd.to_datetime(result['visit_date']).min() >= pd.to_datetime('2023-01-01')
    assert pd.to_datetime(result['visit_date']).max() <= pd.to_datetime('2023-03-01')

def test_visits_per_month():
    result = visits_per_month()
    assert result is not None
    assert 'month' in result.columns
    assert 'num_visits' in result.columns
    assert len(result) > 0

def test_average_visits_per_patient():
    result = average_visits_per_patient()
    assert result is not None
    assert 'patient_id' in result.columns
    assert 'avg_visits' in result.columns
    assert len(result) > 0