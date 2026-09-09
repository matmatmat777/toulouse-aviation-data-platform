select
    icao24,
    {{ normalize_callsign('callsign') }} as callsign,
    latitude,
    longitude,
    altitude,
    timestamp

from {{ source('aviation_raw', 'aircraft_positions') }}