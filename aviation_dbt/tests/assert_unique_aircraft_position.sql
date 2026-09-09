select
    icao24,
    timestamp,
    count(*) as number_of_rows

from {{ ref('int_aircraft_positions_deduplicated') }}

group by
    icao24,
    timestamp

having count(*) > 1