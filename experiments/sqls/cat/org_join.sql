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
    CASE
        WHEN class_0 >= class_1 Then 0
        WHEN class_1 >= class_0 Then 1
    END AS Score
FROM
    (
        SELECT
            (
                CASE
                    WHEN tree_0 = 0 THEN 1
                    ELSE 0
                END + CASE
                    WHEN tree_1 = 0 THEN 1
                    ELSE 0
                END + CASE
                    WHEN tree_2 = 0 THEN 1
                    ELSE 0
                END + CASE
                    WHEN tree_3 = 0 THEN 1
                    ELSE 0
                END + CASE
                    WHEN tree_4 = 0 THEN 1
                    ELSE 0
                END
            ) AS class_0,
            (
                CASE
                    WHEN tree_0 = 1 THEN 1
                    ELSE 0
                END + CASE
                    WHEN tree_1 = 1 THEN 1
                    ELSE 0
                END + CASE
                    WHEN tree_2 = 1 THEN 1
                    ELSE 0
                END + CASE
                    WHEN tree_3 = 1 THEN 1
                    ELSE 0
                END + CASE
                    WHEN tree_4 = 1 THEN 1
                    ELSE 0
                END
            ) AS class_1
        FROM
            (
                SELECT
                    CASE
                        WHEN "nom_4_0" <= 0.5 THEN CASE
                            WHEN "ord_5" <= 103.5 THEN CASE
                                WHEN "ord_1_2" <= 0.5 THEN CASE
                                    WHEN "ord_2_4" <= 0.5 THEN CASE
                                        WHEN "ord_2_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_1_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_3_0" <= 0.5 THEN CASE
                                        WHEN "ord_5" <= 34.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_2_5" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "ord_1_2" <= 0.5 THEN CASE
                                    WHEN "bin_1" <= 0.5 THEN CASE
                                        WHEN "month" <= 0.22727273404598236 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_1_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_2_2" <= 0.5 THEN CASE
                                        WHEN "ord_0" <= 0.25 THEN 1.0
                                        ELSE 1.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_4_19" <= 0.5 THEN 0.0
                                        ELSE 1.0
                                    END
                                END
                            END
                        END
                        ELSE CASE
                            WHEN "bin_4_1" <= 0.5 THEN CASE
                                WHEN "ord_1_4" <= 0.5 THEN CASE
                                    WHEN "bin_1" <= 0.5 THEN CASE
                                        WHEN "ord_3_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_4_18" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_2_1" <= 0.5 THEN CASE
                                        WHEN "ord_4_25" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_3_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "ord_1_2" <= 0.5 THEN CASE
                                    WHEN "ord_0" <= 0.75 THEN CASE
                                        WHEN "ord_2_2" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "month" <= 0.22727273404598236 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "nom_2_2" <= 0.5 THEN CASE
                                        WHEN "ord_4_25" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "day" <= 0.0833333358168602 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                        END
                    END AS tree_0,
                    CASE
                        WHEN "ord_5" <= 100.5 THEN CASE
                            WHEN "nom_1_0" <= 0.5 THEN CASE
                                WHEN "ord_4_0" <= 0.5 THEN CASE
                                    WHEN "ord_3_1" <= 0.5 THEN CASE
                                        WHEN "bin_4_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_5" <= 41.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "month" <= 0.5909090936183929 THEN CASE
                                        WHEN "ord_2_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_2_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "bin_4_1" <= 0.5 THEN CASE
                                    WHEN "ord_2_4" <= 0.5 THEN CASE
                                        WHEN "nom_0_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "month" <= 0.22727273404598236 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "nom_4_0" <= 0.5 THEN CASE
                                        WHEN "ord_3_11" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_2_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                        END
                        ELSE CASE
                            WHEN "bin_1" <= 0.5 THEN CASE
                                WHEN "ord_3_11" <= 0.5 THEN CASE
                                    WHEN "ord_1_4" <= 0.5 THEN CASE
                                        WHEN "ord_2_4" <= 0.5 THEN 0.0
                                        ELSE 1.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_0" <= 0.75 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "nom_0_0" <= 0.5 THEN CASE
                                        WHEN "nom_4_3" <= 0.5 THEN 1.0
                                        ELSE 1.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_1_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "nom_4_0" <= 0.5 THEN CASE
                                    WHEN "nom_0_1" <= 0.5 THEN CASE
                                        WHEN "ord_3_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "nom_3_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "bin_4_1" <= 0.5 THEN CASE
                                        WHEN "ord_4_25" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_2_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                        END
                    END AS tree_1,
                    CASE
                        WHEN "month" <= 0.3181818276643753 THEN CASE
                            WHEN "nom_1_0" <= 0.5 THEN CASE
                                WHEN "nom_0_1" <= 0.5 THEN CASE
                                    WHEN "ord_1_2" <= 0.5 THEN CASE
                                        WHEN "nom_3_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_2_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_3_11" <= 0.5 THEN CASE
                                        WHEN "ord_5" <= 105.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "nom_4_1" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "ord_5" <= 99.5 THEN CASE
                                    WHEN "ord_1_2" <= 0.5 THEN CASE
                                        WHEN "nom_9" <= 12.0 THEN 1.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "nom_0_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "nom_0_0" <= 0.5 THEN CASE
                                        WHEN "ord_5" <= 158.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "day" <= 0.2500000074505806 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                        END
                        ELSE CASE
                            WHEN "ord_1_2" <= 0.5 THEN CASE
                                WHEN "ord_2_2" <= 0.5 THEN CASE
                                    WHEN "ord_2_4" <= 0.5 THEN CASE
                                        WHEN "nom_1_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_5" <= 94.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "nom_0_0" <= 0.5 THEN CASE
                                        WHEN "ord_5" <= 90.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_3_11" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "nom_4_0" <= 0.5 THEN CASE
                                    WHEN "ord_2_2" <= 0.5 THEN CASE
                                        WHEN "ord_5" <= 84.5 THEN 0.0
                                        ELSE 1.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_4_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_2_4" <= 0.5 THEN CASE
                                        WHEN "ord_0" <= 0.25 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_4_3" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                        END
                    END AS tree_2,
                    CASE
                        WHEN "ord_3_0" <= 0.5 THEN CASE
                            WHEN "ord_2_4" <= 0.5 THEN CASE
                                WHEN "nom_2_2" <= 0.5 THEN CASE
                                    WHEN "bin_4_1" <= 0.5 THEN CASE
                                        WHEN "ord_2_2" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "nom_0_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_1_4" <= 0.5 THEN CASE
                                        WHEN "nom_4_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "bin_4_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "ord_1_2" <= 0.5 THEN CASE
                                    WHEN "ord_0" <= 0.25 THEN CASE
                                        WHEN "ord_3_14" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "nom_0_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_4_0" <= 0.5 THEN CASE
                                        WHEN "ord_4_5" <= 0.5 THEN 1.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "nom_1_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                        END
                        ELSE CASE
                            WHEN "nom_4_3" <= 0.5 THEN CASE
                                WHEN "ord_1_2" <= 0.5 THEN CASE
                                    WHEN "bin_4_1" <= 0.5 THEN CASE
                                        WHEN "nom_4_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_5" <= 110.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_2_4" <= 0.5 THEN CASE
                                        WHEN "month" <= 0.6818181872367859 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "bin_1" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "ord_4_6" <= 0.5 THEN CASE
                                    WHEN "nom_1_5" <= 0.5 THEN CASE
                                        WHEN "ord_1_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "nom_5" <= 24.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_0" <= 0.25 THEN CASE
                                        WHEN "month" <= 0.8636363744735718 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "nom_2_0" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                        END
                    END AS tree_3,
                    CASE
                        WHEN "ord_3_0" <= 0.5 THEN CASE
                            WHEN "ord_3_11" <= 0.5 THEN CASE
                                WHEN "ord_1_4" <= 0.5 THEN CASE
                                    WHEN "nom_4_3" <= 0.5 THEN CASE
                                        WHEN "ord_2_2" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_1_2" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_2_4" <= 0.5 THEN CASE
                                        WHEN "day" <= 0.2500000074505806 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_0" <= 0.25 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "nom_0_0" <= 0.5 THEN CASE
                                    WHEN "nom_3_0" <= 0.5 THEN CASE
                                        WHEN "ord_1_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_4_22" <= 0.5 THEN 0.0
                                        ELSE 1.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_2_4" <= 0.5 THEN CASE
                                        WHEN "day" <= 0.9166666567325592 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_1_2" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                        END
                        ELSE CASE
                            WHEN "ord_5" <= 105.5 THEN CASE
                                WHEN "nom_3_3" <= 0.5 THEN CASE
                                    WHEN "nom_4_0" <= 0.5 THEN CASE
                                        WHEN "ord_1_2" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "month" <= 0.7727272808551788 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "ord_2_2" <= 0.5 THEN CASE
                                        WHEN "ord_4_22" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "nom_6" <= 507.0 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                            ELSE CASE
                                WHEN "ord_1_2" <= 0.5 THEN CASE
                                    WHEN "ord_2_2" <= 0.5 THEN CASE
                                        WHEN "ord_1_4" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "month" <= 0.5909090936183929 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                                ELSE CASE
                                    WHEN "nom_3_3" <= 0.5 THEN CASE
                                        WHEN "ord_5" <= 148.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                    ELSE CASE
                                        WHEN "ord_2_1" <= 0.5 THEN 0.0
                                        ELSE 0.0
                                    END
                                END
                            END
                        END
                    END AS tree_4
                FROM
                    (
                        SELECT
                            1.0 * "bin_1" AS "bin_1",
                            0.5 * "ord_0" - 0.5 AS "ord_0",
                            0.166666666666667 * "day" - 0.166666666666667 AS "day",
                            0.0909090909090909 * "month" - 0.0909090909090909 AS "month",
                            "bin_4_0",
                            "bin_4_1",
                            "nom_0_0",
                            "nom_0_1",
                            "nom_1_0",
                            "nom_1_4",
                            "nom_1_5",
                            "nom_2_0",
                            "nom_2_2",
                            "nom_3_0",
                            "nom_3_3",
                            "nom_3_4",
                            "nom_4_0",
                            "nom_4_1",
                            "nom_4_3",
                            "ord_1_2",
                            "ord_1_4",
                            "ord_2_0",
                            "ord_2_1",
                            "ord_2_2",
                            "ord_2_4",
                            "ord_2_5",
                            "ord_3_0",
                            "ord_3_1",
                            "ord_3_4",
                            "ord_3_11",
                            "ord_3_14",
                            "ord_4_0",
                            "ord_4_3",
                            "ord_4_5",
                            "ord_4_6",
                            "ord_4_18",
                            "ord_4_19",
                            "ord_4_22",
                            "ord_4_25",
                            "nom_5",
                            "nom_6",
                            "ord_5",
                            "nom_9"
                        FROM
                            (
                                SELECT
                                    "bin_1",
                                    "ord_0",
                                    "day",
                                    "month",
                                    "bin_4_0" AS "bin_4_0",
                                    "bin_4_1" AS "bin_4_1",
                                    "nom_0_0" AS "nom_0_0",
                                    "nom_0_1" AS "nom_0_1",
                                    "nom_1_0" AS "nom_1_0",
                                    "nom_1_4" AS "nom_1_4",
                                    "nom_1_5" AS "nom_1_5",
                                    "nom_2_0" AS "nom_2_0",
                                    "nom_2_2" AS "nom_2_2",
                                    "nom_3_0" AS "nom_3_0",
                                    "nom_3_3" AS "nom_3_3",
                                    "nom_3_4" AS "nom_3_4",
                                    "nom_4_0" AS "nom_4_0",
                                    "nom_4_1" AS "nom_4_1",
                                    "nom_4_3" AS "nom_4_3",
                                    "ord_1_2" AS "ord_1_2",
                                    "ord_1_4" AS "ord_1_4",
                                    "ord_2_0" AS "ord_2_0",
                                    "ord_2_1" AS "ord_2_1",
                                    "ord_2_2" AS "ord_2_2",
                                    "ord_2_4" AS "ord_2_4",
                                    "ord_2_5" AS "ord_2_5",
                                    "ord_3_0" AS "ord_3_0",
                                    "ord_3_1" AS "ord_3_1",
                                    "ord_3_4" AS "ord_3_4",
                                    "ord_3_11" AS "ord_3_11",
                                    "ord_3_14" AS "ord_3_14",
                                    "ord_4_0" AS "ord_4_0",
                                    "ord_4_3" AS "ord_4_3",
                                    "ord_4_5" AS "ord_4_5",
                                    "ord_4_6" AS "ord_4_6",
                                    "ord_4_18" AS "ord_4_18",
                                    "ord_4_19" AS "ord_4_19",
                                    "ord_4_22" AS "ord_4_22",
                                    "ord_4_25" AS "ord_4_25",
                                    "nom_5_cat_c_cat_col" AS "nom_5",
                                    "nom_6_cat_c_cat_col" AS "nom_6",
                                    "ord_5_cat_c_cat_col" AS "ord_5",
                                    "nom_9_cat_c_cat_col" AS "nom_9"
                                FROM
                                    cat
                                    left join bin_4_expand on cat."bin_4" = bin_4_expand."bin_4"
                                    left join nom_0_expand on cat."nom_0" = nom_0_expand."nom_0"
                                    left join nom_1_expand on cat."nom_1" = nom_1_expand."nom_1"
                                    left join nom_2_expand on cat."nom_2" = nom_2_expand."nom_2"
                                    left join nom_3_expand on cat."nom_3" = nom_3_expand."nom_3"
                                    left join nom_4_expand on cat."nom_4" = nom_4_expand."nom_4"
                                    left join ord_1_expand on cat."ord_1" = ord_1_expand."ord_1"
                                    left join ord_2_expand on cat."ord_2" = ord_2_expand."ord_2"
                                    left join ord_3_expand on cat."ord_3" = ord_3_expand."ord_3"
                                    left join ord_4_expand on cat."ord_4" = ord_4_expand."ord_4"
                                    left join nom_5_cat_c_cat on cat."nom_5" = nom_5_cat_c_cat."nom_5"
                                    left join nom_6_cat_c_cat on cat."nom_6" = nom_6_cat_c_cat."nom_6"
                                    left join ord_5_cat_c_cat on cat."ord_5" = ord_5_cat_c_cat."ord_5"
                                    left join nom_9_cat_c_cat on cat."nom_9" = nom_9_cat_c_cat."nom_9"
                            ) AS data
                    ) AS data
            ) AS F
    ) AS F