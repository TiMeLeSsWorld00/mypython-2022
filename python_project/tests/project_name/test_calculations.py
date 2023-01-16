from project_name.calculations import calculations

def test_calculations():
    assert calculations('GlobalLandTemperaturesByMajorCity.csv', 1980) == (7, 'Baghdad')