import pandas as pd
import math

def validate_and_convert_float(value):
    if value == '' or pd.isna(value) or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        float_val = float(value)
        if math.isnan(float_val):
            return None
        return float_val
    except (ValueError, TypeError):
        return None


def validate_and_convert_int(value):
    if pd.isna(value) or value == '':
        return 0
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return 0