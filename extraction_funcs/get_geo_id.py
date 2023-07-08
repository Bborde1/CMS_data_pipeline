import hashlib

# create unique geographic ids given address
# note: geocoding is more expensive, but would be more robust


def generate_geo_id(address):
    geoid = hashlib.sha256(address.encode()).hexdigest()
    return geoid
