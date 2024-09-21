import geoip2.database
import datetime

# Paths to your GeoLite2 databases
asn_db_path = "GeoLite2-ASN-Test.mmdb"  # Update this path to your ASN database file location
geo_db_path = "GeoLite2-City.mmdb"  # Update this path to your GeoLite2-City.mmdb file location

# Create readers for both the ASN and Geo databases
asn_reader = geoip2.database.Reader(asn_db_path)
geo_reader = geoip2.database.Reader(geo_db_path)

def get_asn_and_geo(ip_address):
    # Get ASN information
    try:
        asn_response = asn_reader.asn(ip_address)
        print(asn_response)
        asn = asn_response.autonomous_system_number
        asn_org = asn_response.autonomous_system_organization
    except geoip2.errors.AddressNotFoundError:
        asn = asn_org = None
    
    # Get Geo location information
    try:
        geo_response = geo_reader.city(ip_address)
        country = geo_response.country.name
        region = geo_response.subdivisions.most_specific.name
        city = geo_response.city.name
        latitude = geo_response.location.latitude
        longitude = geo_response.location.longitude
    except geoip2.errors.AddressNotFoundError:
        country = region = city = latitude = longitude = None
    
    return {
        "asn": asn,
        "asn_organisation": asn_org,
        "country": country,
        "region": region,
        "city": city,
        "latitude": latitude,
        "longitude": longitude
    }

def enrich_event(parsed_data):
    start_ts = datetime.datetime.now()
    
    # Enrich source IP
    src_enrichment = get_asn_and_geo(parsed_data['src_ip_addr'])
    
    # Enrich destination IP
    dest_enrichment = get_asn_and_geo(parsed_data['dest_ip_addr'])
    
    # Update the parsed data with enrichment information
    parsed_data['src_ip_enrichment'] = src_enrichment
    parsed_data['dest_ip_enrichment'] = dest_enrichment
    
    end_ts = datetime.datetime.now()
    latency = end_ts - start_ts
    parsed_data['event_enrich_latency_ms'] = int(latency.total_seconds() * 1000)
    # parsed_data['pkt_enrich_exit'] = int(time.time())
    
    return parsed_data