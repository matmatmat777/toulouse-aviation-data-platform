select
    icao24,
    callsign,
    latitude,
    longitude,
    altitude,
    timestamp

from {{ ref('stg_aircraft_positions') }}

qualify row_number() over (
    partition by icao24, timestamp
    order by timestamp
) = 1