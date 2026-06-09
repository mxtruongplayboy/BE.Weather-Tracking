"""Unit conversion utilities. All storm wind speeds stored in knots (kt)."""


def ms_to_kt(ms: float) -> float:
    return ms * 1.94384


def kmh_to_kt(kmh: float) -> float:
    return kmh * 0.539957


def mph_to_kt(mph: float) -> float:
    return mph * 0.868976


def kt_to_ms(kt: float) -> float:
    return kt / 1.94384


def kt_to_kmh(kt: float) -> float:
    return kt / 0.539957


def categorize_storm(wind_kt: float, basin: str = "") -> str:
    """
    Return storm category string based on max sustained wind (knots).
    Uses Saffir-Simpson for Atlantic/East Pacific, JMA scale for West Pacific.
    """
    if basin in ("WP", "IO", "SP"):
        # JMA / RSMC intensity grade
        if wind_kt >= 105:
            return "violent_typhoon"
        if wind_kt >= 85:
            return "very_strong_typhoon"
        if wind_kt >= 64:
            return "typhoon"
        if wind_kt >= 48:
            return "severe_tropical_storm"
        if wind_kt >= 34:
            return "tropical_storm"
        if wind_kt >= 28:
            return "tropical_depression"
        return "tropical_depression"
    else:
        # Saffir-Simpson
        if wind_kt >= 137:
            return "category_5"
        if wind_kt >= 113:
            return "category_4"
        if wind_kt >= 96:
            return "category_3"
        if wind_kt >= 83:
            return "category_2"
        if wind_kt >= 64:
            return "category_1"
        if wind_kt >= 34:
            return "tropical_storm"
        return "tropical_depression"
