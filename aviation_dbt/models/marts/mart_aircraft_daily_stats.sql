select
    date(timestamp) as flight_date,
    icao24,
    any_value(callsign) as callsign,
    count(*) as position_count,
    min(altitude) as min_altitude,
    max(altitude) as max_altitude,
    avg(altitude) as avg_altitude

from {{ ref('int_aircraft_positions_deduplicated') }}

group by
    flight_date,
    icao24