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
            "Site_Name",
            "Zone",
            "Environment_Type_0",
            "Environment_Type_1",
            "Environment_Type_2",
            "Environment_Type_3"
        FROM
            (
                SELECT
                    "Hour",
                    "Month",
                    "DayofWeek",
                    "Altitude_m",
                    "PM_10",
                    "site_name_cat_c_cat_col" AS "Site_Name",
                    "zone_cat_c_cat_col" AS "Zone",
                    "environment_type_0" AS "Environment_Type_0",
                    "environment_type_1" AS "Environment_Type_1",
                    "environment_type_2" AS "Environment_Type_2",
                    "environment_type_3" AS "Environment_Type_3"
                FROM
                    ukair
                    left join site_name_cat_c_cat on ukair."Site_Name" = site_name_cat_c_cat."site_name"
                    left join zone_cat_c_cat on ukair."Zone" = zone_cat_c_cat."zone"
                    left join environment_type_expand on ukair."Environment_Type" = environment_type_expand."environment_type"
            ) AS data
    ) AS data