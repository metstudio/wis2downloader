import enum

def get_todays_date():
    """
    Returns today's date in the format yyyy/mm/dd.
    """
    today = dt.now()
    yyyy = f"{today.year:04}"
    mm = f"{today.month:02}"
    dd = f"{today.day:02}"
    return yyyy, mm, dd


def map_media_type(media_type):
    _map = {
        "application/x-bufr": "bufr",
        "application/octet-stream": "bin",
        "application/xml": "xml",
        "image/jpeg": "jpeg",
        "application/x-grib": "grib",
        "application/grib;edition=2": "grib",
        "text/plain": "txt"
    }

    return _map.get(media_type, 'bin')


class VerificationMethods(enum.Enum):
    sha256 = 'sha256'
    sha384 = 'sha384'
    sha512 = 'sha512'
    sha3_256 = 'sha3_256'
    sha3_384 = 'sha3_384'
    sha3_512 = 'sha3_512'