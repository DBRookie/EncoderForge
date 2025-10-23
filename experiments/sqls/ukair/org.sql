SET
    max_parallel_workers_per_gather = 3;

SET
    statement_timeout = 3600000;

SET
    enable_mergejoin = off;

SET
    enable_nestloop = off;

EXPLAIN ANALYZE
SELECT
    "Hour" :: float * -0.094827 :: float + "Month" :: float * -0.538514 :: float + "DayofWeek" :: float * -0.137126 :: float + "Altitude_m" :: float * 0.463857 :: float + "PM_10" :: float * 7.986671 :: float + "Environment_Type_0" :: float * -0.930519 :: float + "Environment_Type_1" :: float * 0.724066 :: float + "Environment_Type_2" :: float * 0.117994 :: float + "Environment_Type_3" :: float * 0.088459 :: float + "Site_Name" :: float * 0.017326 :: float + "Zone" :: float * -0.011424 :: float + 9.059806404687478 :: float
FROM
    (
        SELECT
            0.144148410030892 :: float * "Hour" :: float - 1.65757944722357 :: float AS "Hour",
            0.289018566041717 :: float * "Month" :: float - 1.89321412906852 :: float AS "Month",
            0.497459505282944 :: float * "DayofWeek" :: float - 1.98911100629868 :: float AS "DayofWeek",
            0.023370767290347 :: float * "Altitude_m" :: float - 1.05903940830843 :: float AS "Altitude_m",
            0.0853502010931776 :: float * "PM_10" :: float - 1.27192567278553 :: float AS "PM_10",
            CASE
                WHEN "Site_Name" = 'Aberdeen' THEN 0
                WHEN "Site_Name" = 'Auchencorth Moss' THEN 1
                WHEN "Site_Name" = 'Barnstaple A39' THEN 2
                WHEN "Site_Name" = 'Belfast Centre' THEN 3
                WHEN "Site_Name" = 'Birmingham A4540 Roadside' THEN 4
                WHEN "Site_Name" = 'Birmingham Tyburn' THEN 5
                WHEN "Site_Name" = 'Bristol St Paul_s' THEN 6
                WHEN "Site_Name" = 'Camden Kerbside' THEN 7
                WHEN "Site_Name" = 'Cardiff Centre' THEN 8
                WHEN "Site_Name" = 'Carlisle Roadside' THEN 9
                WHEN "Site_Name" = 'Chatham Roadside' THEN 10
                WHEN "Site_Name" = 'Chepstow A48' THEN 11
                WHEN "Site_Name" = 'Chesterfield Loundsley Green' THEN 12
                WHEN "Site_Name" = 'Chesterfield Roadside' THEN 13
                WHEN "Site_Name" = 'Chilbolton Observatory' THEN 14
                WHEN "Site_Name" = 'Derry Rosemount' THEN 15
                WHEN "Site_Name" = 'Edinburgh St Leonards' THEN 16
                WHEN "Site_Name" = 'Glasgow High Street' THEN 17
                WHEN "Site_Name" = 'Glasgow Townhead' THEN 18
                WHEN "Site_Name" = 'Grangemouth' THEN 19
                WHEN "Site_Name" = 'Greenock A8 Roadside' THEN 20
                WHEN "Site_Name" = 'Leamington Spa' THEN 21
                WHEN "Site_Name" = 'Leamington Spa Rugby Road' THEN 22
                WHEN "Site_Name" = 'Leeds Centre' THEN 23
                WHEN "Site_Name" = 'Leeds Headingley Kerbside' THEN 24
                WHEN "Site_Name" = 'Liverpool Speke' THEN 25
                WHEN "Site_Name" = 'London Bloomsbury' THEN 26
                WHEN "Site_Name" = 'London Harlington' THEN 27
                WHEN "Site_Name" = 'London Marylebone Road' THEN 28
                WHEN "Site_Name" = 'London N. Kensington' THEN 29
                WHEN "Site_Name" = 'Middlesbrough' THEN 30
                WHEN "Site_Name" = 'Newcastle Centre' THEN 31
                WHEN "Site_Name" = 'Newport' THEN 32
                WHEN "Site_Name" = 'Norwich Lakenfields' THEN 33
                WHEN "Site_Name" = 'Nottingham Centre' THEN 34
                WHEN "Site_Name" = 'Oxford St Ebbes' THEN 35
                WHEN "Site_Name" = 'Plymouth Centre' THEN 36
                WHEN "Site_Name" = 'Port Talbot Margam' THEN 37
                WHEN "Site_Name" = 'Portsmouth' THEN 38
                WHEN "Site_Name" = 'Reading New Town' THEN 39
                WHEN "Site_Name" = 'Rochester Stoke' THEN 40
                WHEN "Site_Name" = 'Salford Eccles' THEN 41
                WHEN "Site_Name" = 'Saltash Callington Road' THEN 42
                WHEN "Site_Name" = 'Sandy Roadside' THEN 43
                WHEN "Site_Name" = 'Sheffield Devonshire Green' THEN 44
                WHEN "Site_Name" = 'Southampton Centre' THEN 45
                WHEN "Site_Name" = 'Stanford-le-Hope Roadside' THEN 46
                WHEN "Site_Name" = 'Stockton-on-Tees Eaglescliffe' THEN 47
                WHEN "Site_Name" = 'Storrington Roadside' THEN 48
                WHEN "Site_Name" = 'Swansea Roadside' THEN 49
                WHEN "Site_Name" = 'Warrington' THEN 50
                WHEN "Site_Name" = 'York Bootham' THEN 51
                WHEN "Site_Name" = 'York Fishergate' THEN 52
            END AS "Site_Name",
CASE
                WHEN "Zone" = 'Belfast Metropolitan Urban Area' THEN 0
                WHEN "Zone" = 'Bristol Urban Area' THEN 1
                WHEN "Zone" = 'Cardiff Urban Area' THEN 2
                WHEN "Zone" = 'Central Scotland' THEN 3
                WHEN "Zone" = 'East Midlands' THEN 4
                WHEN "Zone" = 'Eastern' THEN 5
                WHEN "Zone" = 'Edinburgh Urban Area' THEN 6
                WHEN "Zone" = 'Glasgow Urban Area' THEN 7
                WHEN "Zone" = 'Greater London Urban Area' THEN 8
                WHEN "Zone" = 'Greater Manchester Urban Area' THEN 9
                WHEN "Zone" = 'Liverpool Urban Area' THEN 10
                WHEN "Zone" = 'North East' THEN 11
                WHEN "Zone" = 'North East Scotland' THEN 12
                WHEN "Zone" = 'North West & Merseyside' THEN 13
                WHEN "Zone" = 'Northern Ireland' THEN 14
                WHEN "Zone" = 'Nottingham Urban Area' THEN 15
                WHEN "Zone" = 'Portsmouth Urban Area' THEN 16
                WHEN "Zone" = 'Reading/Wokingham Urban Area' THEN 17
                WHEN "Zone" = 'Sheffield Urban Area' THEN 18
                WHEN "Zone" = 'South East' THEN 19
                WHEN "Zone" = 'South Wales' THEN 20
                WHEN "Zone" = 'South West' THEN 21
                WHEN "Zone" = 'Southampton Urban Area' THEN 22
                WHEN "Zone" = 'Swansea Urban Area' THEN 23
                WHEN "Zone" = 'Teesside Urban Area' THEN 24
                WHEN "Zone" = 'Tyneside' THEN 25
                WHEN "Zone" = 'West Midlands' THEN 26
                WHEN "Zone" = 'West Midlands Urban Area' THEN 27
                WHEN "Zone" = 'West Yorkshire Urban Area' THEN 28
                WHEN "Zone" = 'Yorkshire & Humberside' THEN 29
            END AS "Zone",
CASE
                WHEN "Environment_Type" = 'Background Rural' THEN 1
                ELSE 0
            END AS "Environment_Type_0",
CASE
                WHEN "Environment_Type" = 'Background Urban' THEN 1
                ELSE 0
            END AS "Environment_Type_1",
CASE
                WHEN "Environment_Type" = 'Industrial Urban' THEN 1
                ELSE 0
            END AS "Environment_Type_2",
CASE
                WHEN "Environment_Type" = 'Traffic Urban' THEN 1
                ELSE 0
            END AS "Environment_Type_3"
        FROM
            ukair
    ) AS data