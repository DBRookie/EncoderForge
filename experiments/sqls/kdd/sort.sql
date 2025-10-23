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
    "OSOURCE",
    "ZIP",
    "RFA_6",
    "RFA_7",
    "RFA_8",
    "RFA_9",
    "RFA_11",
    "RFA_12",
    "RFA_16",
    "RFA_17",
    "RFA_18",
    "RFA_19",
    "RFA_21",
    "RFA_22",
    "STATE_0",
    "STATE_1",
    "STATE_2",
    "STATE_3",
    "STATE_4",
    "STATE_5",
    "STATE_6",
    "STATE_7",
    "STATE_8",
    "STATE_9",
    "STATE_10",
    "STATE_11",
    "STATE_12",
    "STATE_13",
    "STATE_14",
    "STATE_15",
    "STATE_16",
    "STATE_17",
    "STATE_18",
    "STATE_19",
    "STATE_20",
    "STATE_21",
    "STATE_22",
    "STATE_23",
    "STATE_24",
    "STATE_25",
    "STATE_26",
    "STATE_27",
    "STATE_28",
    "STATE_29",
    "STATE_30",
    "STATE_31",
    "STATE_32",
    "STATE_33",
    "STATE_34",
    "STATE_35",
    "STATE_36",
    "STATE_37",
    "STATE_38",
    "STATE_39",
    "STATE_40",
    "STATE_41",
    "STATE_42",
    "STATE_43",
    "STATE_44",
    "STATE_45",
    "STATE_46",
    "STATE_47",
    "STATE_48",
    "STATE_49",
    "STATE_50",
    "STATE_51",
    "STATE_52",
    "STATE_53",
    "STATE_54",
    "STATE_55",
    "STATE_56",
    "RFA_4_0",
    "RFA_4_1",
    "RFA_4_2",
    "RFA_4_3",
    "RFA_4_4",
    "RFA_4_5",
    "RFA_4_6",
    "RFA_4_7",
    "RFA_4_8",
    "RFA_4_9",
    "RFA_4_10",
    "RFA_4_11",
    "RFA_4_12",
    "RFA_4_13",
    "RFA_4_14",
    "RFA_4_15",
    "RFA_4_16",
    "RFA_4_17",
    "RFA_4_18",
    "RFA_4_19",
    "RFA_4_20",
    "RFA_4_21",
    "RFA_4_22",
    "RFA_4_23",
    "RFA_4_24",
    "RFA_4_25",
    "RFA_4_26",
    "RFA_4_27",
    "RFA_4_28",
    "RFA_4_29",
    "RFA_4_30",
    "RFA_4_31",
    "RFA_4_32",
    "RFA_4_33",
    "RFA_4_34",
    "RFA_4_35",
    "RFA_4_36",
    "RFA_4_37",
    "RFA_4_38",
    "RFA_4_39",
    "RFA_4_40",
    "RFA_4_41",
    "RFA_4_42",
    "RFA_4_43",
    "RFA_4_44",
    "RFA_4_45",
    "RFA_4_46",
    "RFA_4_47",
    "RFA_4_48",
    "RFA_4_49",
    "RFA_4_50",
    "RFA_4_51",
    "RFA_4_52",
    "RFA_4_53",
    "RFA_4_54",
    "RFA_4_55",
    "RFA_4_56",
    "RFA_4_57",
    "RFA_4_58",
    "RFA_4_59",
    "RFA_4_60",
    "RFA_4_61",
    "RFA_4_62",
    CASE
        WHEN "MAILCODE" = 'B' THEN 0
        ELSE 1
    END AS "MAILCODE_0",
    CASE
        WHEN "MAILCODE" = ' ' THEN 0
        ELSE 1
    END AS "MAILCODE_1",
    CASE
        WHEN "PVASTATE" = ' ' THEN 1
        ELSE 0
    END AS "PVASTATE_0",
    CASE
        WHEN "PVASTATE" = 'E' THEN 1
        ELSE 0
    END AS "PVASTATE_1",
    CASE
        WHEN "PVASTATE" = 'P' THEN 1
        ELSE 0
    END AS "PVASTATE_2",
    CASE
        WHEN "NOEXCH" = ' ' THEN 1
        ELSE 0
    END AS "NOEXCH_0",
    CASE
        WHEN "NOEXCH" = '0' THEN 1
        ELSE 0
    END AS "NOEXCH_1",
    CASE
        WHEN "NOEXCH" = '1' THEN 1
        ELSE 0
    END AS "NOEXCH_2",
    CASE
        WHEN "NOEXCH" = 'X' THEN 1
        ELSE 0
    END AS "NOEXCH_3",
    CASE
        WHEN "CHILD18" = ' ' THEN 1
        ELSE 0
    END AS "CHILD18_0",
    CASE
        WHEN "CHILD18" = 'B' THEN 1
        ELSE 0
    END AS "CHILD18_1",
    CASE
        WHEN "CHILD18" = 'F' THEN 1
        ELSE 0
    END AS "CHILD18_2",
    CASE
        WHEN "CHILD18" = 'M' THEN 1
        ELSE 0
    END AS "CHILD18_3",
    CASE
        WHEN "GENDER" = ' ' THEN 1
        ELSE 0
    END AS "GENDER_0",
    CASE
        WHEN "GENDER" = 'A' THEN 1
        ELSE 0
    END AS "GENDER_1",
    CASE
        WHEN "GENDER" = 'C' THEN 1
        ELSE 0
    END AS "GENDER_2",
    CASE
        WHEN "GENDER" = 'F' THEN 1
        ELSE 0
    END AS "GENDER_3",
    CASE
        WHEN "GENDER" = 'J' THEN 1
        ELSE 0
    END AS "GENDER_4",
    CASE
        WHEN "GENDER" = 'M' THEN 1
        ELSE 0
    END AS "GENDER_5",
    CASE
        WHEN "GENDER" = 'U' THEN 1
        ELSE 0
    END AS "GENDER_6",
    CASE
        WHEN "DATASRCE" = ' ' THEN 1
        ELSE 0
    END AS "DATASRCE_0",
    CASE
        WHEN "DATASRCE" = '1' THEN 1
        ELSE 0
    END AS "DATASRCE_1",
    CASE
        WHEN "DATASRCE" = '2' THEN 1
        ELSE 0
    END AS "DATASRCE_2",
    CASE
        WHEN "DATASRCE" = '3' THEN 1
        ELSE 0
    END AS "DATASRCE_3",
    CASE
        WHEN "SOLP3" = ' ' THEN 1
        ELSE 0
    END AS "SOLP3_0",
    CASE
        WHEN "SOLP3" = '00' THEN 1
        ELSE 0
    END AS "SOLP3_1",
    CASE
        WHEN "SOLP3" = '01' THEN 1
        ELSE 0
    END AS "SOLP3_2",
    CASE
        WHEN "SOLP3" = '02' THEN 1
        ELSE 0
    END AS "SOLP3_3",
    CASE
        WHEN "SOLP3" = '12' THEN 1
        ELSE 0
    END AS "SOLP3_4",
    CASE
        WHEN "SOLIH" = ' ' THEN 1
        ELSE 0
    END AS "SOLIH_0",
    CASE
        WHEN "SOLIH" = '00' THEN 1
        ELSE 0
    END AS "SOLIH_1",
    CASE
        WHEN "SOLIH" = '01' THEN 1
        ELSE 0
    END AS "SOLIH_2",
    CASE
        WHEN "SOLIH" = '02' THEN 1
        ELSE 0
    END AS "SOLIH_3",
    CASE
        WHEN "SOLIH" = '03' THEN 1
        ELSE 0
    END AS "SOLIH_4",
    CASE
        WHEN "SOLIH" = '04' THEN 1
        ELSE 0
    END AS "SOLIH_5",
    CASE
        WHEN "SOLIH" = '06' THEN 1
        ELSE 0
    END AS "SOLIH_6",
    CASE
        WHEN "SOLIH" = '12' THEN 1
        ELSE 0
    END AS "SOLIH_7",
    "RFA_20_0",
    "RFA_20_1",
    "RFA_20_2",
    "RFA_20_3",
    "RFA_20_4",
    "RFA_20_5",
    "RFA_20_6",
    "RFA_20_7",
    "RFA_20_8",
    "RFA_20_9",
    "RFA_20_10",
    "RFA_20_11",
    "RFA_20_12",
    "RFA_20_13",
    "RFA_20_14",
    "RFA_20_15",
    "RFA_20_16",
    "RFA_20_17",
    "RFA_20_18",
    "RFA_20_19",
    "RFA_20_20",
    "RFA_20_21",
    "RFA_20_22",
    "RFA_20_23",
    "RFA_20_24",
    "RFA_20_25",
    "RFA_20_26",
    "RFA_20_27",
    "RFA_20_28",
    "RFA_20_29",
    "RFA_20_30",
    "RFA_20_31",
    "RFA_20_32",
    "RFA_20_33",
    "RFA_20_34",
    "RFA_20_35",
    "RFA_20_36",
    "RFA_20_37",
    "RFA_20_38",
    "RFA_20_39",
    "RFA_20_40",
    "RFA_20_41",
    "RFA_20_42",
    "RFA_20_43",
    "RFA_20_44",
    "RFA_20_45",
    "RFA_20_46",
    "RFA_20_47",
    "RFA_20_48",
    "RFA_20_49",
    "RFA_20_50",
    "RFA_20_51",
    "RFA_20_52",
    "RFA_20_53",
    "RFA_20_54",
    "RFA_20_55",
    "RFA_20_56",
    "RFA_20_57",
    "RFA_20_58",
    "RFA_20_59",
    "RFA_20_60",
    "RFA_20_61",
    "RFA_20_62",
    "RFA_20_63",
    "RFA_20_64",
    "RFA_20_65",
    "RFA_20_66",
    "RFA_20_67",
    "RFA_20_68",
    "RFA_20_69",
    "RFA_20_70",
    "RFA_20_71",
    "RFA_20_72",
    "RFA_20_73",
    "RFA_20_74",
    "RFA_20_75",
    "RFA_20_76",
    "RFA_20_77",
    "RFA_20_78",
    CASE
        WHEN "RECINHSE" = 'X' THEN 0
        ELSE 1
    END AS "RECINHSE_0",
    CASE
        WHEN "RECINHSE" = ' ' THEN 0
        ELSE 1
    END AS "RECINHSE_1",
    CASE
        WHEN "RECP3" = 'X' THEN 0
        ELSE 1
    END AS "RECP3_0",
    CASE
        WHEN "RECP3" = ' ' THEN 0
        ELSE 1
    END AS "RECP3_1",
    CASE
        WHEN "RECPGVG" = 'X' THEN 0
        ELSE 1
    END AS "RECPGVG_0",
    CASE
        WHEN "RECPGVG" = ' ' THEN 0
        ELSE 1
    END AS "RECPGVG_1",
    CASE
        WHEN "RECSWEEP" = 'X' THEN 0
        ELSE 1
    END AS "RECSWEEP_0",
    CASE
        WHEN "RECSWEEP" = ' ' THEN 0
        ELSE 1
    END AS "RECSWEEP_1",
    "MDMAUD_0",
    "MDMAUD_1",
    "MDMAUD_2",
    "MDMAUD_3",
    "MDMAUD_4",
    "MDMAUD_5",
    "MDMAUD_6",
    "MDMAUD_7",
    "MDMAUD_8",
    "MDMAUD_9",
    "MDMAUD_10",
    "MDMAUD_11",
    "MDMAUD_12",
    "MDMAUD_13",
    "MDMAUD_14",
    "MDMAUD_15",
    "MDMAUD_16",
    "MDMAUD_17",
    "MDMAUD_18",
    "MDMAUD_19",
    "MDMAUD_20",
    "MDMAUD_21",
    "MDMAUD_22",
    "MDMAUD_23",
    "MDMAUD_24",
    "MDMAUD_25",
    "MDMAUD_26",
    "RFA_15_0",
    "RFA_15_1",
    "RFA_15_2",
    "RFA_15_3",
    "RFA_15_4",
    "RFA_15_5",
    "RFA_15_6",
    "RFA_15_7",
    "RFA_15_8",
    "RFA_15_9",
    "RFA_15_10",
    "RFA_15_11",
    "RFA_15_12",
    "RFA_15_13",
    "RFA_15_14",
    "RFA_15_15",
    "RFA_15_16",
    "RFA_15_17",
    "RFA_15_18",
    "RFA_15_19",
    "RFA_15_20",
    "RFA_15_21",
    "RFA_15_22",
    "RFA_15_23",
    "RFA_15_24",
    "RFA_15_25",
    "RFA_15_26",
    "RFA_15_27",
    "RFA_15_28",
    "RFA_15_29",
    "RFA_15_30",
    "RFA_15_31",
    "RFA_15_32",
    "RFA_15_33",
    "RFA_2_0",
    "RFA_2_1",
    "RFA_2_2",
    "RFA_2_3",
    "RFA_2_4",
    "RFA_2_5",
    "RFA_2_6",
    "RFA_2_7",
    "RFA_2_8",
    "RFA_2_9",
    "RFA_2_10",
    "RFA_2_11",
    "RFA_2_12",
    "RFA_2_13",
    "DOMAIN_0",
    "DOMAIN_1",
    "DOMAIN_2",
    "DOMAIN_3",
    "DOMAIN_4",
    "DOMAIN_5",
    "DOMAIN_6",
    "DOMAIN_7",
    "DOMAIN_8",
    "DOMAIN_9",
    "DOMAIN_10",
    "DOMAIN_11",
    "DOMAIN_12",
    "DOMAIN_13",
    "DOMAIN_14",
    "DOMAIN_15",
    "DOMAIN_16",
    CASE
        WHEN "MAJOR" = 'X' THEN 0
        ELSE 1
    END AS "MAJOR_0",
    CASE
        WHEN "MAJOR" = ' ' THEN 0
        ELSE 1
    END AS "MAJOR_1",
    CASE
        WHEN "GEOCODE" = ' ' THEN 1
        ELSE 0
    END AS "GEOCODE_0",
    CASE
        WHEN "GEOCODE" = '01' THEN 1
        ELSE 0
    END AS "GEOCODE_1",
    CASE
        WHEN "GEOCODE" = '02' THEN 1
        ELSE 0
    END AS "GEOCODE_2",
    CASE
        WHEN "GEOCODE" = '03' THEN 1
        ELSE 0
    END AS "GEOCODE_3",
    CASE
        WHEN "GEOCODE" = '04' THEN 1
        ELSE 0
    END AS "GEOCODE_4",
    CASE
        WHEN "GEOCODE" = '05' THEN 1
        ELSE 0
    END AS "GEOCODE_5",
    CASE
        WHEN "GEOCODE" = '12' THEN 1
        ELSE 0
    END AS "GEOCODE_6",
    CASE
        WHEN "GEOCODE" = '14' THEN 1
        ELSE 0
    END AS "GEOCODE_7",
    CASE
        WHEN "COLLECT1" = 'Y' THEN 0
        ELSE 1
    END AS "COLLECT1_0",
    CASE
        WHEN "COLLECT1" = ' ' THEN 0
        ELSE 1
    END AS "COLLECT1_1",
    CASE
        WHEN "VETERANS" = 'Y' THEN 0
        ELSE 1
    END AS "VETERANS_0",
    CASE
        WHEN "VETERANS" = ' ' THEN 0
        ELSE 1
    END AS "VETERANS_1",
    CASE
        WHEN "BIBLE" = 'Y' THEN 0
        ELSE 1
    END AS "BIBLE_0",
    CASE
        WHEN "BIBLE" = ' ' THEN 0
        ELSE 1
    END AS "BIBLE_1",
    CASE
        WHEN "CATLG" = 'Y' THEN 0
        ELSE 1
    END AS "CATLG_0",
    CASE
        WHEN "CATLG" = ' ' THEN 0
        ELSE 1
    END AS "CATLG_1",
    "RFA_23_0",
    "RFA_23_1",
    "RFA_23_2",
    "RFA_23_3",
    "RFA_23_4",
    "RFA_23_5",
    "RFA_23_6",
    "RFA_23_7",
    "RFA_23_8",
    "RFA_23_9",
    "RFA_23_10",
    "RFA_23_11",
    "RFA_23_12",
    "RFA_23_13",
    "RFA_23_14",
    "RFA_23_15",
    "RFA_23_16",
    "RFA_23_17",
    "RFA_23_18",
    "RFA_23_19",
    "RFA_23_20",
    "RFA_23_21",
    "RFA_23_22",
    "RFA_23_23",
    "RFA_23_24",
    "RFA_23_25",
    "RFA_23_26",
    "RFA_23_27",
    "RFA_23_28",
    "RFA_23_29",
    "RFA_23_30",
    "RFA_23_31",
    "RFA_23_32",
    "RFA_23_33",
    "RFA_23_34",
    "RFA_23_35",
    "RFA_23_36",
    "RFA_23_37",
    "RFA_23_38",
    "RFA_23_39",
    "RFA_23_40",
    "RFA_23_41",
    "RFA_23_42",
    "RFA_23_43",
    "RFA_23_44",
    "RFA_23_45",
    "RFA_23_46",
    "RFA_23_47",
    "RFA_23_48",
    "RFA_23_49",
    "RFA_23_50",
    "RFA_23_51",
    "RFA_23_52",
    "RFA_23_53",
    "RFA_23_54",
    "RFA_23_55",
    "RFA_23_56",
    "RFA_23_57",
    "RFA_23_58",
    "RFA_23_59",
    "RFA_23_60",
    "RFA_23_61",
    "RFA_23_62",
    "RFA_23_63",
    "RFA_23_64",
    "RFA_23_65",
    "RFA_23_66",
    "RFA_23_67",
    "RFA_23_68",
    "RFA_23_69",
    "RFA_23_70",
    "RFA_23_71",
    "RFA_23_72",
    "RFA_23_73",
    "RFA_23_74",
    "RFA_23_75",
    "RFA_23_76",
    "RFA_23_77",
    "RFA_23_78",
    "RFA_23_79",
    "RFA_23_80",
    "RFA_23_81",
    "RFA_24_0",
    "RFA_24_1",
    "RFA_24_2",
    "RFA_24_3",
    "RFA_24_4",
    "RFA_24_5",
    "RFA_24_6",
    "RFA_24_7",
    "RFA_24_8",
    "RFA_24_9",
    "RFA_24_10",
    "RFA_24_11",
    "RFA_24_12",
    "RFA_24_13",
    "RFA_24_14",
    "RFA_24_15",
    "RFA_24_16",
    "RFA_24_17",
    "RFA_24_18",
    "RFA_24_19",
    "RFA_24_20",
    "RFA_24_21",
    "RFA_24_22",
    "RFA_24_23",
    "RFA_24_24",
    "RFA_24_25",
    "RFA_24_26",
    "RFA_24_27",
    "RFA_24_28",
    "RFA_24_29",
    "RFA_24_30",
    "RFA_24_31",
    "RFA_24_32",
    "RFA_24_33",
    "RFA_24_34",
    "RFA_24_35",
    "RFA_24_36",
    "RFA_24_37",
    "RFA_24_38",
    "RFA_24_39",
    "RFA_24_40",
    "RFA_24_41",
    "RFA_24_42",
    "RFA_24_43",
    "RFA_24_44",
    "RFA_24_45",
    "RFA_24_46",
    "RFA_24_47",
    "RFA_24_48",
    "RFA_24_49",
    "RFA_24_50",
    "RFA_24_51",
    "RFA_24_52",
    "RFA_24_53",
    "RFA_24_54",
    "RFA_24_55",
    "RFA_24_56",
    "RFA_24_57",
    "RFA_24_58",
    "RFA_24_59",
    "RFA_24_60",
    "RFA_24_61",
    "RFA_24_62",
    "RFA_24_63",
    "RFA_24_64",
    "RFA_24_65",
    "RFA_24_66",
    "RFA_24_67",
    "RFA_24_68",
    "RFA_24_69",
    "RFA_24_70",
    "RFA_24_71",
    "RFA_24_72",
    "RFA_24_73",
    "RFA_24_74",
    "RFA_24_75",
    "RFA_24_76",
    "RFA_24_77",
    "RFA_24_78",
    "RFA_24_79",
    "RFA_24_80",
    "RFA_24_81",
    "RFA_24_82",
    "RFA_24_83",
    "RFA_24_84",
    "RFA_24_85",
    "RFA_24_86",
    "RFA_24_87",
    "RFA_24_88",
    "RFA_24_89",
    "RFA_24_90",
    "RFA_24_91",
    "RFA_24_92",
    "RFA_24_93",
    "RFA_24_94",
    "RFA_24_95",
    1 AS "RFA_2R_0",
    "RFA_5_0",
    "RFA_5_1",
    "RFA_5_2",
    "RFA_5_3",
    "RFA_5_4",
    "RFA_5_5",
    "RFA_5_6",
    "RFA_5_7",
    "RFA_5_8",
    "RFA_5_9",
    "RFA_5_10",
    "RFA_5_11",
    "RFA_5_12",
    "RFA_5_13",
    "RFA_5_14",
    "RFA_5_15",
    "RFA_5_16",
    "RFA_5_17",
    "RFA_5_18",
    "RFA_5_19",
    "RFA_5_20",
    "RFA_5_21",
    "RFA_5_22",
    "RFA_5_23",
    "RFA_5_24",
    "RFA_5_25",
    "RFA_5_26",
    "RFA_5_27",
    "RFA_5_28",
    "RFA_5_29",
    "RFA_5_30",
    "RFA_5_31",
    "RFA_5_32",
    "RFA_5_33",
    "RFA_5_34",
    "RFA_5_35",
    "RFA_5_36",
    "RFA_5_37",
    "RFA_5_38",
    "RFA_5_39",
    "RFA_5_40",
    "CLUSTER_0",
    "CLUSTER_1",
    "CLUSTER_2",
    "CLUSTER_3",
    "CLUSTER_4",
    "CLUSTER_5",
    "CLUSTER_6",
    "CLUSTER_7",
    "CLUSTER_8",
    "CLUSTER_9",
    "CLUSTER_10",
    "CLUSTER_11",
    "CLUSTER_12",
    "CLUSTER_13",
    "CLUSTER_14",
    "CLUSTER_15",
    "CLUSTER_16",
    "CLUSTER_17",
    "CLUSTER_18",
    "CLUSTER_19",
    "CLUSTER_20",
    "CLUSTER_21",
    "CLUSTER_22",
    "CLUSTER_23",
    "CLUSTER_24",
    "CLUSTER_25",
    "CLUSTER_26",
    "CLUSTER_27",
    "CLUSTER_28",
    "CLUSTER_29",
    "CLUSTER_30",
    "CLUSTER_31",
    "CLUSTER_32",
    "CLUSTER_33",
    "CLUSTER_34",
    "CLUSTER_35",
    "CLUSTER_36",
    "CLUSTER_37",
    "CLUSTER_38",
    "CLUSTER_39",
    "CLUSTER_40",
    "CLUSTER_41",
    "CLUSTER_42",
    "CLUSTER_43",
    "CLUSTER_44",
    "CLUSTER_45",
    "CLUSTER_46",
    "CLUSTER_47",
    "CLUSTER_48",
    "CLUSTER_49",
    "CLUSTER_50",
    "CLUSTER_51",
    "CLUSTER_52",
    "CLUSTER_53",
    CASE
        WHEN "AGEFLAG" = ' ' THEN 1
        ELSE 0
    END AS "AGEFLAG_0",
    CASE
        WHEN "AGEFLAG" = 'E' THEN 1
        ELSE 0
    END AS "AGEFLAG_1",
    CASE
        WHEN "AGEFLAG" = 'I' THEN 1
        ELSE 0
    END AS "AGEFLAG_2",
    CASE
        WHEN "HOMEOWNR" = ' ' THEN 1
        ELSE 0
    END AS "HOMEOWNR_0",
    CASE
        WHEN "HOMEOWNR" = 'H' THEN 1
        ELSE 0
    END AS "HOMEOWNR_1",
    CASE
        WHEN "HOMEOWNR" = 'U' THEN 1
        ELSE 0
    END AS "HOMEOWNR_2",
    CASE
        WHEN "CHILD03" = ' ' THEN 1
        ELSE 0
    END AS "CHILD03_0",
    CASE
        WHEN "CHILD03" = 'B' THEN 1
        ELSE 0
    END AS "CHILD03_1",
    CASE
        WHEN "CHILD03" = 'F' THEN 1
        ELSE 0
    END AS "CHILD03_2",
    CASE
        WHEN "CHILD03" = 'M' THEN 1
        ELSE 0
    END AS "CHILD03_3",
    CASE
        WHEN "CHILD07" = ' ' THEN 1
        ELSE 0
    END AS "CHILD07_0",
    CASE
        WHEN "CHILD07" = 'B' THEN 1
        ELSE 0
    END AS "CHILD07_1",
    CASE
        WHEN "CHILD07" = 'F' THEN 1
        ELSE 0
    END AS "CHILD07_2",
    CASE
        WHEN "CHILD07" = 'M' THEN 1
        ELSE 0
    END AS "CHILD07_3",
    CASE
        WHEN "CHILD12" = ' ' THEN 1
        ELSE 0
    END AS "CHILD12_0",
    CASE
        WHEN "CHILD12" = 'B' THEN 1
        ELSE 0
    END AS "CHILD12_1",
    CASE
        WHEN "CHILD12" = 'F' THEN 1
        ELSE 0
    END AS "CHILD12_2",
    CASE
        WHEN "CHILD12" = 'M' THEN 1
        ELSE 0
    END AS "CHILD12_3",
    CASE
        WHEN "HOMEE" = 'Y' THEN 0
        ELSE 1
    END AS "HOMEE_0",
    CASE
        WHEN "HOMEE" = ' ' THEN 0
        ELSE 1
    END AS "HOMEE_1",
    CASE
        WHEN "PETS" = 'Y' THEN 0
        ELSE 1
    END AS "PETS_0",
    CASE
        WHEN "PETS" = ' ' THEN 0
        ELSE 1
    END AS "PETS_1",
    CASE
        WHEN "CDPLAY" = 'Y' THEN 0
        ELSE 1
    END AS "CDPLAY_0",
    CASE
        WHEN "CDPLAY" = ' ' THEN 0
        ELSE 1
    END AS "CDPLAY_1",
    CASE
        WHEN "STEREO" = 'Y' THEN 0
        ELSE 1
    END AS "STEREO_0",
    CASE
        WHEN "STEREO" = ' ' THEN 0
        ELSE 1
    END AS "STEREO_1",
    CASE
        WHEN "PCOWNERS" = 'Y' THEN 0
        ELSE 1
    END AS "PCOWNERS_0",
    CASE
        WHEN "PCOWNERS" = ' ' THEN 0
        ELSE 1
    END AS "PCOWNERS_1",
    CASE
        WHEN "PHOTO" = 'Y' THEN 0
        ELSE 1
    END AS "PHOTO_0",
    CASE
        WHEN "PHOTO" = ' ' THEN 0
        ELSE 1
    END AS "PHOTO_1",
    CASE
        WHEN "MDMAUD_R" = 'C' THEN 1
        ELSE 0
    END AS "MDMAUD_R_0",
    CASE
        WHEN "MDMAUD_R" = 'D' THEN 1
        ELSE 0
    END AS "MDMAUD_R_1",
    CASE
        WHEN "MDMAUD_R" = 'I' THEN 1
        ELSE 0
    END AS "MDMAUD_R_2",
    CASE
        WHEN "MDMAUD_R" = 'L' THEN 1
        ELSE 0
    END AS "MDMAUD_R_3",
    CASE
        WHEN "MDMAUD_R" = 'X' THEN 1
        ELSE 0
    END AS "MDMAUD_R_4",
    CASE
        WHEN "MDMAUD_F" = '1' THEN 1
        ELSE 0
    END AS "MDMAUD_F_0",
    CASE
        WHEN "MDMAUD_F" = '2' THEN 1
        ELSE 0
    END AS "MDMAUD_F_1",
    CASE
        WHEN "MDMAUD_F" = '5' THEN 1
        ELSE 0
    END AS "MDMAUD_F_2",
    CASE
        WHEN "MDMAUD_F" = 'X' THEN 1
        ELSE 0
    END AS "MDMAUD_F_3",
    CASE
        WHEN "CRAFTS" = 'Y' THEN 0
        ELSE 1
    END AS "CRAFTS_0",
    CASE
        WHEN "CRAFTS" = ' ' THEN 0
        ELSE 1
    END AS "CRAFTS_1",
    CASE
        WHEN "FISHER" = 'Y' THEN 0
        ELSE 1
    END AS "FISHER_0",
    CASE
        WHEN "FISHER" = ' ' THEN 0
        ELSE 1
    END AS "FISHER_1",
    CASE
        WHEN "GARDENIN" = 'Y' THEN 0
        ELSE 1
    END AS "GARDENIN_0",
    CASE
        WHEN "GARDENIN" = ' ' THEN 0
        ELSE 1
    END AS "GARDENIN_1",
    CASE
        WHEN "BOATS" = 'Y' THEN 0
        ELSE 1
    END AS "BOATS_0",
    CASE
        WHEN "BOATS" = ' ' THEN 0
        ELSE 1
    END AS "BOATS_1",
    CASE
        WHEN "WALKER" = 'Y' THEN 0
        ELSE 1
    END AS "WALKER_0",
    CASE
        WHEN "WALKER" = ' ' THEN 0
        ELSE 1
    END AS "WALKER_1",
    CASE
        WHEN "KIDSTUFF" = 'Y' THEN 0
        ELSE 1
    END AS "KIDSTUFF_0",
    CASE
        WHEN "KIDSTUFF" = ' ' THEN 0
        ELSE 1
    END AS "KIDSTUFF_1",
    CASE
        WHEN "CARDS" = 'Y' THEN 0
        ELSE 1
    END AS "CARDS_0",
    CASE
        WHEN "CARDS" = ' ' THEN 0
        ELSE 1
    END AS "CARDS_1",
    CASE
        WHEN "PLATES" = 'Y' THEN 0
        ELSE 1
    END AS "PLATES_0",
    CASE
        WHEN "PLATES" = ' ' THEN 0
        ELSE 1
    END AS "PLATES_1",
    CASE
        WHEN "LIFESRC" = ' ' THEN 1
        ELSE 0
    END AS "LIFESRC_0",
    CASE
        WHEN "LIFESRC" = '1' THEN 1
        ELSE 0
    END AS "LIFESRC_1",
    CASE
        WHEN "LIFESRC" = '2' THEN 1
        ELSE 0
    END AS "LIFESRC_2",
    CASE
        WHEN "LIFESRC" = '3' THEN 1
        ELSE 0
    END AS "LIFESRC_3",
    CASE
        WHEN "PEPSTRFL" = 'X' THEN 0
        ELSE 1
    END AS "PEPSTRFL_0",
    CASE
        WHEN "PEPSTRFL" = ' ' THEN 0
        ELSE 1
    END AS "PEPSTRFL_1",
    "RFA_3_0",
    "RFA_3_1",
    "RFA_3_2",
    "RFA_3_3",
    "RFA_3_4",
    "RFA_3_5",
    "RFA_3_6",
    "RFA_3_7",
    "RFA_3_8",
    "RFA_3_9",
    "RFA_3_10",
    "RFA_3_11",
    "RFA_3_12",
    "RFA_3_13",
    "RFA_3_14",
    "RFA_3_15",
    "RFA_3_16",
    "RFA_3_17",
    "RFA_3_18",
    "RFA_3_19",
    "RFA_3_20",
    "RFA_3_21",
    "RFA_3_22",
    "RFA_3_23",
    "RFA_3_24",
    "RFA_3_25",
    "RFA_3_26",
    "RFA_3_27",
    "RFA_3_28",
    "RFA_3_29",
    "RFA_3_30",
    "RFA_3_31",
    "RFA_3_32",
    "RFA_3_33",
    "RFA_3_34",
    "RFA_3_35",
    "RFA_3_36",
    "RFA_3_37",
    "RFA_3_38",
    "RFA_3_39",
    "RFA_3_40",
    "RFA_3_41",
    "RFA_3_42",
    "RFA_3_43",
    "RFA_3_44",
    "RFA_3_45",
    "RFA_3_46",
    "RFA_3_47",
    "RFA_3_48",
    "RFA_3_49",
    "RFA_3_50",
    "RFA_3_51",
    "RFA_3_52",
    "RFA_3_53",
    "RFA_3_54",
    "RFA_3_55",
    "RFA_3_56",
    "RFA_3_57",
    "RFA_3_58",
    "RFA_3_59",
    "RFA_3_60",
    "RFA_3_61",
    "RFA_3_62",
    "RFA_3_63",
    "RFA_3_64",
    "RFA_3_65",
    "RFA_3_66",
    "RFA_3_67",
    "RFA_3_68",
    CASE
        WHEN "RFA_2A" = 'D' THEN 1
        ELSE 0
    END AS "RFA_2A_0",
    CASE
        WHEN "RFA_2A" = 'E' THEN 1
        ELSE 0
    END AS "RFA_2A_1",
    CASE
        WHEN "RFA_2A" = 'F' THEN 1
        ELSE 0
    END AS "RFA_2A_2",
    CASE
        WHEN "RFA_2A" = 'G' THEN 1
        ELSE 0
    END AS "RFA_2A_3",
    "RFA_10_0",
    "RFA_10_1",
    "RFA_10_2",
    "RFA_10_3",
    "RFA_10_4",
    "RFA_10_5",
    "RFA_10_6",
    "RFA_10_7",
    "RFA_10_8",
    "RFA_10_9",
    "RFA_10_10",
    "RFA_10_11",
    "RFA_10_12",
    "RFA_10_13",
    "RFA_10_14",
    "RFA_10_15",
    "RFA_10_16",
    "RFA_10_17",
    "RFA_10_18",
    "RFA_10_19",
    "RFA_10_20",
    "RFA_10_21",
    "RFA_10_22",
    "RFA_10_23",
    "RFA_10_24",
    "RFA_10_25",
    "RFA_10_26",
    "RFA_10_27",
    "RFA_10_28",
    "RFA_10_29",
    "RFA_10_30",
    "RFA_10_31",
    "RFA_10_32",
    "RFA_10_33",
    "RFA_10_34",
    "RFA_10_35",
    "RFA_10_36",
    "RFA_10_37",
    "RFA_10_38",
    "RFA_10_39",
    "RFA_10_40",
    "RFA_10_41",
    "RFA_10_42",
    "RFA_10_43",
    "RFA_10_44",
    "RFA_10_45",
    "RFA_10_46",
    "RFA_10_47",
    "RFA_10_48",
    "RFA_10_49",
    "RFA_10_50",
    "RFA_10_51",
    "RFA_10_52",
    "RFA_10_53",
    "RFA_10_54",
    "RFA_10_55",
    "RFA_10_56",
    "RFA_10_57",
    "RFA_10_58",
    "RFA_10_59",
    "RFA_10_60",
    "RFA_10_61",
    "RFA_10_62",
    "RFA_10_63",
    "RFA_10_64",
    "RFA_10_65",
    "RFA_10_66",
    "RFA_10_67",
    "RFA_10_68",
    "RFA_10_69",
    "RFA_10_70",
    "RFA_10_71",
    "RFA_10_72",
    "RFA_10_73",
    "RFA_10_74",
    "RFA_10_75",
    "RFA_10_76",
    "RFA_10_77",
    "RFA_10_78",
    "RFA_10_79",
    "RFA_10_80",
    "RFA_10_81",
    "RFA_10_82",
    "RFA_10_83",
    "RFA_10_84",
    "RFA_10_85",
    "RFA_10_86",
    "RFA_10_87",
    "RFA_10_88",
    "RFA_10_89",
    "RFA_10_90",
    "RFA_10_91",
    "RFA_10_92",
    "RFA_13_0",
    "RFA_13_1",
    "RFA_13_2",
    "RFA_13_3",
    "RFA_13_4",
    "RFA_13_5",
    "RFA_13_6",
    "RFA_13_7",
    "RFA_13_8",
    "RFA_13_9",
    "RFA_13_10",
    "RFA_13_11",
    "RFA_13_12",
    "RFA_13_13",
    "RFA_13_14",
    "RFA_13_15",
    "RFA_13_16",
    "RFA_13_17",
    "RFA_13_18",
    "RFA_13_19",
    "RFA_13_20",
    "RFA_13_21",
    "RFA_13_22",
    "RFA_13_23",
    "RFA_13_24",
    "RFA_13_25",
    "RFA_13_26",
    "RFA_13_27",
    "RFA_13_28",
    "RFA_13_29",
    "RFA_13_30",
    "RFA_13_31",
    "RFA_13_32",
    "RFA_13_33",
    "RFA_13_34",
    "RFA_13_35",
    "RFA_13_36",
    "RFA_13_37",
    "RFA_13_38",
    "RFA_13_39",
    "RFA_13_40",
    "RFA_13_41",
    "RFA_13_42",
    "RFA_13_43",
    "RFA_13_44",
    "RFA_13_45",
    "RFA_13_46",
    "RFA_13_47",
    "RFA_13_48",
    "RFA_13_49",
    "RFA_13_50",
    "RFA_13_51",
    "RFA_13_52",
    "RFA_13_53",
    "RFA_13_54",
    "RFA_13_55",
    "RFA_13_56",
    "RFA_13_57",
    "RFA_13_58",
    "RFA_13_59",
    "RFA_13_60",
    "RFA_13_61",
    "RFA_13_62",
    "RFA_13_63",
    "RFA_13_64",
    "RFA_13_65",
    "RFA_13_66",
    "RFA_13_67",
    "RFA_13_68",
    "RFA_13_69",
    "RFA_13_70",
    "RFA_13_71",
    "RFA_13_72",
    "RFA_13_73",
    "RFA_13_74",
    "RFA_13_75",
    "RFA_13_76",
    "RFA_13_77",
    "RFA_13_78",
    "RFA_13_79",
    "RFA_13_80",
    "RFA_13_81",
    "RFA_13_82",
    "RFA_13_83",
    "RFA_13_84",
    "RFA_13_85",
    "RFA_13_86",
    "RFA_14_0",
    "RFA_14_1",
    "RFA_14_2",
    "RFA_14_3",
    "RFA_14_4",
    "RFA_14_5",
    "RFA_14_6",
    "RFA_14_7",
    "RFA_14_8",
    "RFA_14_9",
    "RFA_14_10",
    "RFA_14_11",
    "RFA_14_12",
    "RFA_14_13",
    "RFA_14_14",
    "RFA_14_15",
    "RFA_14_16",
    "RFA_14_17",
    "RFA_14_18",
    "RFA_14_19",
    "RFA_14_20",
    "RFA_14_21",
    "RFA_14_22",
    "RFA_14_23",
    "RFA_14_24",
    "RFA_14_25",
    "RFA_14_26",
    "RFA_14_27",
    "RFA_14_28",
    "RFA_14_29",
    "RFA_14_30",
    "RFA_14_31",
    "RFA_14_32",
    "RFA_14_33",
    "RFA_14_34",
    "RFA_14_35",
    "RFA_14_36",
    "RFA_14_37",
    "RFA_14_38",
    "RFA_14_39",
    "RFA_14_40",
    "RFA_14_41",
    "RFA_14_42",
    "RFA_14_43",
    "RFA_14_44",
    "RFA_14_45",
    "RFA_14_46",
    "RFA_14_47",
    "RFA_14_48",
    "RFA_14_49",
    "RFA_14_50",
    "RFA_14_51",
    "RFA_14_52",
    "RFA_14_53",
    "RFA_14_54",
    "RFA_14_55",
    "RFA_14_56",
    "RFA_14_57",
    "RFA_14_58",
    "RFA_14_59",
    "RFA_14_60",
    "RFA_14_61",
    "RFA_14_62",
    "RFA_14_63",
    "RFA_14_64",
    "RFA_14_65",
    "RFA_14_66",
    "RFA_14_67",
    "RFA_14_68",
    "RFA_14_69",
    "RFA_14_70",
    "RFA_14_71",
    "RFA_14_72",
    "RFA_14_73",
    "RFA_14_74",
    "RFA_14_75",
    "RFA_14_76",
    "RFA_14_77",
    "RFA_14_78",
    "RFA_14_79",
    "RFA_14_80",
    "RFA_14_81",
    "RFA_14_82",
    "RFA_14_83",
    "RFA_14_84",
    "RFA_14_85",
    "RFA_14_86",
    "RFA_14_87",
    "RFA_14_88",
    "RFA_14_89",
    "RFA_14_90",
    "RFA_14_91",
    "RFA_14_92",
    "RFA_14_93",
    "RFA_14_94",
    CASE
        WHEN "MDMAUD_A" = 'C' THEN 1
        ELSE 0
    END AS "MDMAUD_A_0",
    CASE
        WHEN "MDMAUD_A" = 'L' THEN 1
        ELSE 0
    END AS "MDMAUD_A_1",
    CASE
        WHEN "MDMAUD_A" = 'M' THEN 1
        ELSE 0
    END AS "MDMAUD_A_2",
    CASE
        WHEN "MDMAUD_A" = 'T' THEN 1
        ELSE 0
    END AS "MDMAUD_A_3",
    CASE
        WHEN "MDMAUD_A" = 'X' THEN 1
        ELSE 0
    END AS "MDMAUD_A_4",
    CASE
        WHEN "GEOCODE2" = ' ' THEN 1
        ELSE 0
    END AS "GEOCODE2_0",
    CASE
        WHEN "GEOCODE2" = 'A' THEN 1
        ELSE 0
    END AS "GEOCODE2_1",
    CASE
        WHEN "GEOCODE2" = 'B' THEN 1
        ELSE 0
    END AS "GEOCODE2_2",
    CASE
        WHEN "GEOCODE2" = 'C' THEN 1
        ELSE 0
    END AS "GEOCODE2_3",
    CASE
        WHEN "GEOCODE2" = 'D' THEN 1
        ELSE 0
    END AS "GEOCODE2_4",
    CASE
        WHEN "GEOCODE2" = 'nan' THEN 1
        ELSE 0
    END AS "GEOCODE2_5",
    CASE
        WHEN "AGE" >= 1.0
        AND "AGE" < 20.4 THEN 0
        WHEN "AGE" >= 20.4
        AND "AGE" < 39.8 THEN 1
        WHEN "AGE" >= 39.8
        AND "AGE" < 59.199999999999996 THEN 2
        WHEN "AGE" >= 59.199999999999996
        AND "AGE" < 78.6 THEN 3
        ELSE 4
    END AS "AGE",
    CASE
        WHEN "NUMCHLD" >= 1.0
        AND "NUMCHLD" < 2.2 THEN 0
        WHEN "NUMCHLD" >= 2.2
        AND "NUMCHLD" < 3.4 THEN 1
        WHEN "NUMCHLD" >= 3.4
        AND "NUMCHLD" < 4.6 THEN 2
        WHEN "NUMCHLD" >= 4.6
        AND "NUMCHLD" < 5.8 THEN 3
        ELSE 4
    END AS "NUMCHLD",
    CASE
        WHEN "INCOME" >= 1.0
        AND "INCOME" < 2.2 THEN 0
        WHEN "INCOME" >= 2.2
        AND "INCOME" < 3.4 THEN 1
        WHEN "INCOME" >= 3.4
        AND "INCOME" < 4.6 THEN 2
        WHEN "INCOME" >= 4.6
        AND "INCOME" < 5.8 THEN 3
        ELSE 4
    END AS "INCOME",
    CASE
        WHEN "WEALTH1" >= 0.0
        AND "WEALTH1" < 1.8 THEN 0
        WHEN "WEALTH1" >= 1.8
        AND "WEALTH1" < 3.6 THEN 1
        WHEN "WEALTH1" >= 3.6
        AND "WEALTH1" < 5.4 THEN 2
        WHEN "WEALTH1" >= 5.4
        AND "WEALTH1" < 7.2 THEN 3
        ELSE 4
    END AS "WEALTH1",
    CASE
        WHEN "MBCRAFT" >= 0.0
        AND "MBCRAFT" < 1.0 THEN 0
        WHEN "MBCRAFT" >= 1.0
        AND "MBCRAFT" < 2.0 THEN 1
        WHEN "MBCRAFT" >= 2.0
        AND "MBCRAFT" < 3.0 THEN 2
        WHEN "MBCRAFT" >= 3.0
        AND "MBCRAFT" < 4.0 THEN 3
        ELSE 4
    END AS "MBCRAFT",
    CASE
        WHEN "MBGARDEN" >= 0.0
        AND "MBGARDEN" < 0.8 THEN 0
        WHEN "MBGARDEN" >= 0.8
        AND "MBGARDEN" < 1.6 THEN 1
        WHEN "MBGARDEN" >= 1.6
        AND "MBGARDEN" < 2.4000000000000004 THEN 2
        WHEN "MBGARDEN" >= 2.4000000000000004
        AND "MBGARDEN" < 3.2 THEN 3
        ELSE 4
    END AS "MBGARDEN",
    CASE
        WHEN "MBBOOKS" >= 0.0
        AND "MBBOOKS" < 1.8 THEN 0
        WHEN "MBBOOKS" >= 1.8
        AND "MBBOOKS" < 3.6 THEN 1
        WHEN "MBBOOKS" >= 3.6
        AND "MBBOOKS" < 5.4 THEN 2
        WHEN "MBBOOKS" >= 5.4
        AND "MBBOOKS" < 7.2 THEN 3
        ELSE 4
    END AS "MBBOOKS",
    CASE
        WHEN "MBCOLECT" >= 0.0
        AND "MBCOLECT" < 1.2 THEN 0
        WHEN "MBCOLECT" >= 1.2
        AND "MBCOLECT" < 2.4 THEN 1
        WHEN "MBCOLECT" >= 2.4
        AND "MBCOLECT" < 3.5999999999999996 THEN 2
        WHEN "MBCOLECT" >= 3.5999999999999996
        AND "MBCOLECT" < 4.8 THEN 3
        ELSE 4
    END AS "MBCOLECT",
    CASE
        WHEN "MAGFAML" >= 0.0
        AND "MAGFAML" < 1.8 THEN 0
        WHEN "MAGFAML" >= 1.8
        AND "MAGFAML" < 3.6 THEN 1
        WHEN "MAGFAML" >= 3.6
        AND "MAGFAML" < 5.4 THEN 2
        WHEN "MAGFAML" >= 5.4
        AND "MAGFAML" < 7.2 THEN 3
        ELSE 4
    END AS "MAGFAML",
    CASE
        WHEN "MAGFEM" >= 0.0
        AND "MAGFEM" < 1.0 THEN 0
        WHEN "MAGFEM" >= 1.0
        AND "MAGFEM" < 2.0 THEN 1
        WHEN "MAGFEM" >= 2.0
        AND "MAGFEM" < 3.0 THEN 2
        WHEN "MAGFEM" >= 3.0
        AND "MAGFEM" < 4.0 THEN 3
        ELSE 4
    END AS "MAGFEM",
    CASE
        WHEN "MAGMALE" >= 0.0
        AND "MAGMALE" < 0.8 THEN 0
        WHEN "MAGMALE" >= 0.8
        AND "MAGMALE" < 1.6 THEN 1
        WHEN "MAGMALE" >= 1.6
        AND "MAGMALE" < 2.4000000000000004 THEN 2
        WHEN "MAGMALE" >= 2.4000000000000004
        AND "MAGMALE" < 3.2 THEN 3
        ELSE 4
    END AS "MAGMALE",
    CASE
        WHEN "PUBGARDN" >= 0.0
        AND "PUBGARDN" < 1.0 THEN 0
        WHEN "PUBGARDN" >= 1.0
        AND "PUBGARDN" < 2.0 THEN 1
        WHEN "PUBGARDN" >= 2.0
        AND "PUBGARDN" < 3.0 THEN 2
        WHEN "PUBGARDN" >= 3.0
        AND "PUBGARDN" < 4.0 THEN 3
        ELSE 4
    END AS "PUBGARDN",
    CASE
        WHEN "PUBCULIN" >= 0.0
        AND "PUBCULIN" < 1.2 THEN 0
        WHEN "PUBCULIN" >= 1.2
        AND "PUBCULIN" < 2.4 THEN 1
        WHEN "PUBCULIN" >= 2.4
        AND "PUBCULIN" < 3.5999999999999996 THEN 2
        WHEN "PUBCULIN" >= 3.5999999999999996
        AND "PUBCULIN" < 4.8 THEN 3
        ELSE 4
    END AS "PUBCULIN",
    CASE
        WHEN "PUBHLTH" >= 0.0
        AND "PUBHLTH" < 1.8 THEN 0
        WHEN "PUBHLTH" >= 1.8
        AND "PUBHLTH" < 3.6 THEN 1
        WHEN "PUBHLTH" >= 3.6
        AND "PUBHLTH" < 5.4 THEN 2
        WHEN "PUBHLTH" >= 5.4
        AND "PUBHLTH" < 7.2 THEN 3
        ELSE 4
    END AS "PUBHLTH",
    CASE
        WHEN "PUBDOITY" >= 0.0
        AND "PUBDOITY" < 1.6 THEN 0
        WHEN "PUBDOITY" >= 1.6
        AND "PUBDOITY" < 3.2 THEN 1
        WHEN "PUBDOITY" >= 3.2
        AND "PUBDOITY" < 4.800000000000001 THEN 2
        WHEN "PUBDOITY" >= 4.800000000000001
        AND "PUBDOITY" < 6.4 THEN 3
        ELSE 4
    END AS "PUBDOITY",
    CASE
        WHEN "PUBNEWFN" >= 0.0
        AND "PUBNEWFN" < 1.8 THEN 0
        WHEN "PUBNEWFN" >= 1.8
        AND "PUBNEWFN" < 3.6 THEN 1
        WHEN "PUBNEWFN" >= 3.6
        AND "PUBNEWFN" < 5.4 THEN 2
        WHEN "PUBNEWFN" >= 5.4
        AND "PUBNEWFN" < 7.2 THEN 3
        ELSE 4
    END AS "PUBNEWFN",
    CASE
        WHEN "PUBPHOTO" >= 0.0
        AND "PUBPHOTO" < 0.4 THEN 0
        WHEN "PUBPHOTO" >= 0.4
        AND "PUBPHOTO" < 0.8 THEN 1
        WHEN "PUBPHOTO" >= 0.8
        AND "PUBPHOTO" < 1.2000000000000002 THEN 2
        WHEN "PUBPHOTO" >= 1.2000000000000002
        AND "PUBPHOTO" < 1.6 THEN 3
        ELSE 4
    END AS "PUBPHOTO",
    CASE
        WHEN "PUBOPP" >= 0.0
        AND "PUBOPP" < 1.8 THEN 0
        WHEN "PUBOPP" >= 1.8
        AND "PUBOPP" < 3.6 THEN 1
        WHEN "PUBOPP" >= 3.6
        AND "PUBOPP" < 5.4 THEN 2
        WHEN "PUBOPP" >= 5.4
        AND "PUBOPP" < 7.2 THEN 3
        ELSE 4
    END AS "PUBOPP",
    CASE
        WHEN "WEALTH2" >= 0.0
        AND "WEALTH2" < 1.8 THEN 0
        WHEN "WEALTH2" >= 1.8
        AND "WEALTH2" < 3.6 THEN 1
        WHEN "WEALTH2" >= 3.6
        AND "WEALTH2" < 5.4 THEN 2
        WHEN "WEALTH2" >= 5.4
        AND "WEALTH2" < 7.2 THEN 3
        ELSE 4
    END AS "WEALTH2",
    CASE
        WHEN "MSA" >= 0.0
        AND "MSA" < 1872.0 THEN 0
        WHEN "MSA" >= 1872.0
        AND "MSA" < 3744.0 THEN 1
        WHEN "MSA" >= 3744.0
        AND "MSA" < 5616.0 THEN 2
        WHEN "MSA" >= 5616.0
        AND "MSA" < 7488.0 THEN 3
        ELSE 4
    END AS "MSA",
    CASE
        WHEN "ADI" >= 0.0
        AND "ADI" < 130.2 THEN 0
        WHEN "ADI" >= 130.2
        AND "ADI" < 260.4 THEN 1
        WHEN "ADI" >= 260.4
        AND "ADI" < 390.59999999999997 THEN 2
        WHEN "ADI" >= 390.59999999999997
        AND "ADI" < 520.8 THEN 3
        ELSE 4
    END AS "ADI",
    CASE
        WHEN "DMA" >= 0.0
        AND "DMA" < 176.2 THEN 0
        WHEN "DMA" >= 176.2
        AND "DMA" < 352.4 THEN 1
        WHEN "DMA" >= 352.4
        AND "DMA" < 528.5999999999999 THEN 2
        WHEN "DMA" >= 528.5999999999999
        AND "DMA" < 704.8 THEN 3
        ELSE 4
    END AS "DMA",
    CASE
        WHEN "ADATE_3" >= 9604.0
        AND "ADATE_3" < 9604.4 THEN 0
        WHEN "ADATE_3" >= 9604.4
        AND "ADATE_3" < 9604.8 THEN 1
        WHEN "ADATE_3" >= 9604.8
        AND "ADATE_3" < 9605.2 THEN 2
        WHEN "ADATE_3" >= 9605.2
        AND "ADATE_3" < 9605.6 THEN 3
        ELSE 4
    END AS "ADATE_3",
    CASE
        WHEN "ADATE_4" >= 9511.0
        AND "ADATE_4" < 9530.6 THEN 0
        WHEN "ADATE_4" >= 9530.6
        AND "ADATE_4" < 9550.2 THEN 1
        WHEN "ADATE_4" >= 9550.2
        AND "ADATE_4" < 9569.8 THEN 2
        WHEN "ADATE_4" >= 9569.8
        AND "ADATE_4" < 9589.4 THEN 3
        ELSE 4
    END AS "ADATE_4",
    CASE
        WHEN "ADATE_6" >= 9601.0
        AND "ADATE_6" < 9601.4 THEN 0
        WHEN "ADATE_6" >= 9601.4
        AND "ADATE_6" < 9601.8 THEN 1
        WHEN "ADATE_6" >= 9601.8
        AND "ADATE_6" < 9602.2 THEN 2
        WHEN "ADATE_6" >= 9602.2
        AND "ADATE_6" < 9602.6 THEN 3
        ELSE 4
    END AS "ADATE_6",
    CASE
        WHEN "ADATE_7" >= 9512.0
        AND "ADATE_7" < 9530.0 THEN 0
        WHEN "ADATE_7" >= 9530.0
        AND "ADATE_7" < 9548.0 THEN 1
        WHEN "ADATE_7" >= 9548.0
        AND "ADATE_7" < 9566.0 THEN 2
        WHEN "ADATE_7" >= 9566.0
        AND "ADATE_7" < 9584.0 THEN 3
        ELSE 4
    END AS "ADATE_7",
    CASE
        WHEN "ADATE_8" >= 9511.0
        AND "ADATE_8" < 9529.8 THEN 0
        WHEN "ADATE_8" >= 9529.8
        AND "ADATE_8" < 9548.6 THEN 1
        WHEN "ADATE_8" >= 9548.6
        AND "ADATE_8" < 9567.4 THEN 2
        WHEN "ADATE_8" >= 9567.4
        AND "ADATE_8" < 9586.2 THEN 3
        ELSE 4
    END AS "ADATE_8",
    CASE
        WHEN "ADATE_9" >= 9509.0
        AND "ADATE_9" < 9509.4 THEN 0
        WHEN "ADATE_9" >= 9509.4
        AND "ADATE_9" < 9509.8 THEN 1
        WHEN "ADATE_9" >= 9509.8
        AND "ADATE_9" < 9510.2 THEN 2
        WHEN "ADATE_9" >= 9510.2
        AND "ADATE_9" < 9510.6 THEN 3
        ELSE 4
    END AS "ADATE_9",
    CASE
        WHEN "ADATE_10" >= 9510.0
        AND "ADATE_10" < 9510.2 THEN 0
        WHEN "ADATE_10" >= 9510.2
        AND "ADATE_10" < 9510.4 THEN 1
        WHEN "ADATE_10" >= 9510.4
        AND "ADATE_10" < 9510.6 THEN 2
        WHEN "ADATE_10" >= 9510.6
        AND "ADATE_10" < 9510.8 THEN 3
        ELSE 4
    END AS "ADATE_10",
    CASE
        WHEN "ADATE_11" >= 9508.0
        AND "ADATE_11" < 9508.6 THEN 0
        WHEN "ADATE_11" >= 9508.6
        AND "ADATE_11" < 9509.2 THEN 1
        WHEN "ADATE_11" >= 9509.2
        AND "ADATE_11" < 9509.8 THEN 2
        WHEN "ADATE_11" >= 9509.8
        AND "ADATE_11" < 9510.4 THEN 3
        ELSE 4
    END AS "ADATE_11",
    CASE
        WHEN "ADATE_12" >= 9507.0
        AND "ADATE_12" < 9507.6 THEN 0
        WHEN "ADATE_12" >= 9507.6
        AND "ADATE_12" < 9508.2 THEN 1
        WHEN "ADATE_12" >= 9508.2
        AND "ADATE_12" < 9508.8 THEN 2
        WHEN "ADATE_12" >= 9508.8
        AND "ADATE_12" < 9509.4 THEN 3
        ELSE 4
    END AS "ADATE_12",
    CASE
        WHEN "ADATE_13" >= 9502.0
        AND "ADATE_13" < 9503.0 THEN 0
        WHEN "ADATE_13" >= 9503.0
        AND "ADATE_13" < 9504.0 THEN 1
        WHEN "ADATE_13" >= 9504.0
        AND "ADATE_13" < 9505.0 THEN 2
        WHEN "ADATE_13" >= 9505.0
        AND "ADATE_13" < 9506.0 THEN 3
        ELSE 4
    END AS "ADATE_13",
    CASE
        WHEN "ADATE_14" >= 9504.0
        AND "ADATE_14" < 9504.4 THEN 0
        WHEN "ADATE_14" >= 9504.4
        AND "ADATE_14" < 9504.8 THEN 1
        WHEN "ADATE_14" >= 9504.8
        AND "ADATE_14" < 9505.2 THEN 2
        WHEN "ADATE_14" >= 9505.2
        AND "ADATE_14" < 9505.6 THEN 3
        ELSE 4
    END AS "ADATE_14",
    CASE
        WHEN "ADATE_16" >= 9502.0
        AND "ADATE_16" < 9502.4 THEN 0
        WHEN "ADATE_16" >= 9502.4
        AND "ADATE_16" < 9502.8 THEN 1
        WHEN "ADATE_16" >= 9502.8
        AND "ADATE_16" < 9503.2 THEN 2
        WHEN "ADATE_16" >= 9503.2
        AND "ADATE_16" < 9503.6 THEN 3
        ELSE 4
    END AS "ADATE_16",
    CASE
        WHEN "ADATE_17" >= 9501.0
        AND "ADATE_17" < 9501.4 THEN 0
        WHEN "ADATE_17" >= 9501.4
        AND "ADATE_17" < 9501.8 THEN 1
        WHEN "ADATE_17" >= 9501.8
        AND "ADATE_17" < 9502.2 THEN 2
        WHEN "ADATE_17" >= 9502.2
        AND "ADATE_17" < 9502.6 THEN 3
        ELSE 4
    END AS "ADATE_17",
    CASE
        WHEN "ADATE_18" >= 9409.0
        AND "ADATE_18" < 9428.8 THEN 0
        WHEN "ADATE_18" >= 9428.8
        AND "ADATE_18" < 9448.6 THEN 1
        WHEN "ADATE_18" >= 9448.6
        AND "ADATE_18" < 9468.4 THEN 2
        WHEN "ADATE_18" >= 9468.4
        AND "ADATE_18" < 9488.2 THEN 3
        ELSE 4
    END AS "ADATE_18",
    CASE
        WHEN "ADATE_19" >= 9409.0
        AND "ADATE_19" < 9409.4 THEN 0
        WHEN "ADATE_19" >= 9409.4
        AND "ADATE_19" < 9409.8 THEN 1
        WHEN "ADATE_19" >= 9409.8
        AND "ADATE_19" < 9410.2 THEN 2
        WHEN "ADATE_19" >= 9410.2
        AND "ADATE_19" < 9410.6 THEN 3
        ELSE 4
    END AS "ADATE_19",
    CASE
        WHEN "ADATE_20" >= 9411.0
        AND "ADATE_20" < 9411.2 THEN 0
        WHEN "ADATE_20" >= 9411.2
        AND "ADATE_20" < 9411.4 THEN 1
        WHEN "ADATE_20" >= 9411.4
        AND "ADATE_20" < 9411.6 THEN 2
        WHEN "ADATE_20" >= 9411.6
        AND "ADATE_20" < 9411.8 THEN 3
        ELSE 4
    END AS "ADATE_20",
    CASE
        WHEN "ADATE_21" >= 9409.0
        AND "ADATE_21" < 9409.2 THEN 0
        WHEN "ADATE_21" >= 9409.2
        AND "ADATE_21" < 9409.4 THEN 1
        WHEN "ADATE_21" >= 9409.4
        AND "ADATE_21" < 9409.6 THEN 2
        WHEN "ADATE_21" >= 9409.6
        AND "ADATE_21" < 9409.8 THEN 3
        ELSE 4
    END AS "ADATE_21",
    CASE
        WHEN "ADATE_22" >= 9408.0
        AND "ADATE_22" < 9427.6 THEN 0
        WHEN "ADATE_22" >= 9427.6
        AND "ADATE_22" < 9447.2 THEN 1
        WHEN "ADATE_22" >= 9447.2
        AND "ADATE_22" < 9466.8 THEN 2
        WHEN "ADATE_22" >= 9466.8
        AND "ADATE_22" < 9486.4 THEN 3
        ELSE 4
    END AS "ADATE_22",
    CASE
        WHEN "ADATE_23" >= 9312.0
        AND "ADATE_23" < 9331.0 THEN 0
        WHEN "ADATE_23" >= 9331.0
        AND "ADATE_23" < 9350.0 THEN 1
        WHEN "ADATE_23" >= 9350.0
        AND "ADATE_23" < 9369.0 THEN 2
        WHEN "ADATE_23" >= 9369.0
        AND "ADATE_23" < 9388.0 THEN 3
        ELSE 4
    END AS "ADATE_23",
    CASE
        WHEN "ADATE_24" >= 9405.0
        AND "ADATE_24" < 9405.2 THEN 0
        WHEN "ADATE_24" >= 9405.2
        AND "ADATE_24" < 9405.4 THEN 1
        WHEN "ADATE_24" >= 9405.4
        AND "ADATE_24" < 9405.6 THEN 2
        WHEN "ADATE_24" >= 9405.6
        AND "ADATE_24" < 9405.8 THEN 3
        ELSE 4
    END AS "ADATE_24",
    CASE
        WHEN "RDATE_3" >= 9605.0
        AND "RDATE_3" < 9645.2 THEN 0
        WHEN "RDATE_3" >= 9645.2
        AND "RDATE_3" < 9685.4 THEN 1
        WHEN "RDATE_3" >= 9685.4
        AND "RDATE_3" < 9725.6 THEN 2
        WHEN "RDATE_3" >= 9725.6
        AND "RDATE_3" < 9765.8 THEN 3
        ELSE 4
    END AS "RDATE_3",
    CASE
        WHEN "RDATE_4" >= 9510.0
        AND "RDATE_4" < 9568.8 THEN 0
        WHEN "RDATE_4" >= 9568.8
        AND "RDATE_4" < 9627.6 THEN 1
        WHEN "RDATE_4" >= 9627.6
        AND "RDATE_4" < 9686.4 THEN 2
        WHEN "RDATE_4" >= 9686.4
        AND "RDATE_4" < 9745.2 THEN 3
        ELSE 4
    END AS "RDATE_4",
    CASE
        WHEN "RDATE_5" >= 9604.0
        AND "RDATE_5" < 9643.8 THEN 0
        WHEN "RDATE_5" >= 9643.8
        AND "RDATE_5" < 9683.6 THEN 1
        WHEN "RDATE_5" >= 9683.6
        AND "RDATE_5" < 9723.4 THEN 2
        WHEN "RDATE_5" >= 9723.4
        AND "RDATE_5" < 9763.2 THEN 3
        ELSE 4
    END AS "RDATE_5",
    CASE
        WHEN "RDATE_6" >= 9510.0
        AND "RDATE_6" < 9569.0 THEN 0
        WHEN "RDATE_6" >= 9569.0
        AND "RDATE_6" < 9628.0 THEN 1
        WHEN "RDATE_6" >= 9628.0
        AND "RDATE_6" < 9687.0 THEN 2
        WHEN "RDATE_6" >= 9687.0
        AND "RDATE_6" < 9746.0 THEN 3
        ELSE 4
    END AS "RDATE_6",
    CASE
        WHEN "RDATE_7" >= 9512.0
        AND "RDATE_7" < 9531.6 THEN 0
        WHEN "RDATE_7" >= 9531.6
        AND "RDATE_7" < 9551.2 THEN 1
        WHEN "RDATE_7" >= 9551.2
        AND "RDATE_7" < 9570.8 THEN 2
        WHEN "RDATE_7" >= 9570.8
        AND "RDATE_7" < 9590.4 THEN 3
        ELSE 4
    END AS "RDATE_7",
    0.35475994415879 * "RDATE_8" - 3406.0784590958 AS "RDATE_8",
    0.0736529183688274 * "RDATE_9" - 700.740471341439 AS "RDATE_9",
    0.105750750566278 * "RDATE_10" - 1005.90274929056 AS "RDATE_10",
    0.110510538948711 * "RDATE_11" - 1051.16399648773 AS "RDATE_11",
    0.103165026763003 * "RDATE_12" - 981.121757006199 AS "RDATE_12",
    0.27663778689733 * "RDATE_13" - 2630.31169484151 AS "RDATE_13",
    0.198261029703583 * "RDATE_14" - 1884.75430846944 AS "RDATE_14",
    0.40556947537381 * "RDATE_15" - 3854.976678099 AS "RDATE_15",
    0.166796936831177 * "RDATE_16" - 1585.30074056672 AS "RDATE_16",
    2.08368971247884 * "RDATE_17" - 19801.3543864328 AS "RDATE_17",
    0.359569121077053 * "RDATE_18" - 3416.32675287514 AS "RDATE_18",
    0.0745981727965584 * "RDATE_19" - 702.268613872128 AS "RDATE_19",
    0.11578063322427 * "RDATE_20" - 1089.82093917214 AS "RDATE_20",
    0.115855038078498 * "RDATE_21" - 1090.40570081763 AS "RDATE_21",
    0.112605238418379 * "RDATE_22" - 1059.61545579571 AS "RDATE_22",
    0.295057673132403 * "RDATE_23" - 2775.9356281454 AS "RDATE_23",
    0.209484876375292 * "RDATE_24" - 1970.659748626 AS "RDATE_24",
    2.02705469140191 * "RAMNT_3" - 20.2832012336152 AS "RAMNT_3",
    1.32763169184909 * "RAMNT_4" - 13.2920366802347 AS "RAMNT_4",
    5.0117452323917 * "RAMNT_5" - 25.065357799586 AS "RAMNT_5",
    0.990732592323802 * "RAMNT_6" - 9.94395300695424 AS "RAMNT_6",
    0.269011006501942 * "RAMNT_7" - 2.81603586207447 AS "RAMNT_7",
    0.174665427288573 * "RAMNT_8" - 2.64583195664035 AS "RAMNT_8",
    0.20237825870705 * "RAMNT_9" - 2.20481296074751 AS "RAMNT_9",
    0.222379221893779 * "RAMNT_10" - 2.35443334732855 AS "RAMNT_10",
    0.219775548381777 * "RAMNT_11" - 2.35440691313173 AS "RAMNT_11",
    0.169070178146272 * "RAMNT_12" - 1.91160993633668 AS "RAMNT_12",
    0.239280428899697 * "RAMNT_13" - 2.50153981256471 AS "RAMNT_13",
    0.206855192550989 * "RAMNT_14" - 2.22976634598981 AS "RAMNT_14",
    0.333456433155829 * "RAMNT_15" - 3.41718663919022 AS "RAMNT_15",
    0.166122867515821 * "RAMNT_16" - 1.85066946584412 AS "RAMNT_16",
    0.274686058810873 * "RAMNT_17" - 2.82045402469927 AS "RAMNT_17",
    0.176665666125966 * "RAMNT_18" - 1.85051289480183 AS "RAMNT_18",
    0.22243087759469 * "RAMNT_19" - 2.33817127303103 AS "RAMNT_19",
    0.32311526241399 * "RAMNT_20" - 3.34602746779761 AS "RAMNT_20",
    0.311129561008918 * "RAMNT_21" - 3.20179147592992 AS "RAMNT_21",
    0.222259689157574 * "RAMNT_22" - 2.33445449839576 AS "RAMNT_22",
    0.36417120353832 * "RAMNT_23" - 3.70529645443152 AS "RAMNT_23",
    0.262487477469897 * "RAMNT_24" - 2.69235598624724 AS "RAMNT_24",
    0.00831276551369329 * "RAMNTALL" - 0.870203072970058 AS "RAMNTALL",
    0.113919815037547 * "MINRAMNT" - 0.901909006787443 AS "MINRAMNT",
    0.0375901020998203 * "MAXRAMNT" - 0.751983824261169 AS "MAXRAMNT",
    0.0731447749794134 * "LASTGIFT" - 1.26467573717504 AS "LASTGIFT",
    0.00334600153227242 * "NEXTDATE" - 30.7426786400434 AS "NEXTDATE",
    0.121624882343486 * "TIMELAG" - 0.94591394365304 AS "TIMELAG",
    0.0925093786146467 * "AVGGIFT" - 1.23332507935618 AS "AVGGIFT",
    0.0533328419375041 * "CLUSTER2" - 1.68145079163372 AS "CLUSTER2"
FROM
    (
        SELECT
            "osource_cat_c_cat_col" AS "OSOURCE",
            "zip_cat_c_cat_col" AS "ZIP",
            "rfa_6_cat_c_cat_col" AS "RFA_6",
            "rfa_7_cat_c_cat_col" AS "RFA_7",
            "rfa_8_cat_c_cat_col" AS "RFA_8",
            "rfa_9_cat_c_cat_col" AS "RFA_9",
            "rfa_11_cat_c_cat_col" AS "RFA_11",
            "rfa_12_cat_c_cat_col" AS "RFA_12",
            "rfa_16_cat_c_cat_col" AS "RFA_16",
            "rfa_17_cat_c_cat_col" AS "RFA_17",
            "rfa_18_cat_c_cat_col" AS "RFA_18",
            "rfa_19_cat_c_cat_col" AS "RFA_19",
            "rfa_21_cat_c_cat_col" AS "RFA_21",
            "rfa_22_cat_c_cat_col" AS "RFA_22",
            "state_0" AS "STATE_0",
            "state_1" AS "STATE_1",
            "state_2" AS "STATE_2",
            "state_3" AS "STATE_3",
            "state_4" AS "STATE_4",
            "state_5" AS "STATE_5",
            "state_6" AS "STATE_6",
            "state_7" AS "STATE_7",
            "state_8" AS "STATE_8",
            "state_9" AS "STATE_9",
            "state_10" AS "STATE_10",
            "state_11" AS "STATE_11",
            "state_12" AS "STATE_12",
            "state_13" AS "STATE_13",
            "state_14" AS "STATE_14",
            "state_15" AS "STATE_15",
            "state_16" AS "STATE_16",
            "state_17" AS "STATE_17",
            "state_18" AS "STATE_18",
            "state_19" AS "STATE_19",
            "state_20" AS "STATE_20",
            "state_21" AS "STATE_21",
            "state_22" AS "STATE_22",
            "state_23" AS "STATE_23",
            "state_24" AS "STATE_24",
            "state_25" AS "STATE_25",
            "state_26" AS "STATE_26",
            "state_27" AS "STATE_27",
            "state_28" AS "STATE_28",
            "state_29" AS "STATE_29",
            "state_30" AS "STATE_30",
            "state_31" AS "STATE_31",
            "state_32" AS "STATE_32",
            "state_33" AS "STATE_33",
            "state_34" AS "STATE_34",
            "state_35" AS "STATE_35",
            "state_36" AS "STATE_36",
            "state_37" AS "STATE_37",
            "state_38" AS "STATE_38",
            "state_39" AS "STATE_39",
            "state_40" AS "STATE_40",
            "state_41" AS "STATE_41",
            "state_42" AS "STATE_42",
            "state_43" AS "STATE_43",
            "state_44" AS "STATE_44",
            "state_45" AS "STATE_45",
            "state_46" AS "STATE_46",
            "state_47" AS "STATE_47",
            "state_48" AS "STATE_48",
            "state_49" AS "STATE_49",
            "state_50" AS "STATE_50",
            "state_51" AS "STATE_51",
            "state_52" AS "STATE_52",
            "state_53" AS "STATE_53",
            "state_54" AS "STATE_54",
            "state_55" AS "STATE_55",
            "state_56" AS "STATE_56",
            "rfa_4_0" AS "RFA_4_0",
            "rfa_4_1" AS "RFA_4_1",
            "rfa_4_2" AS "RFA_4_2",
            "rfa_4_3" AS "RFA_4_3",
            "rfa_4_4" AS "RFA_4_4",
            "rfa_4_5" AS "RFA_4_5",
            "rfa_4_6" AS "RFA_4_6",
            "rfa_4_7" AS "RFA_4_7",
            "rfa_4_8" AS "RFA_4_8",
            "rfa_4_9" AS "RFA_4_9",
            "rfa_4_10" AS "RFA_4_10",
            "rfa_4_11" AS "RFA_4_11",
            "rfa_4_12" AS "RFA_4_12",
            "rfa_4_13" AS "RFA_4_13",
            "rfa_4_14" AS "RFA_4_14",
            "rfa_4_15" AS "RFA_4_15",
            "rfa_4_16" AS "RFA_4_16",
            "rfa_4_17" AS "RFA_4_17",
            "rfa_4_18" AS "RFA_4_18",
            "rfa_4_19" AS "RFA_4_19",
            "rfa_4_20" AS "RFA_4_20",
            "rfa_4_21" AS "RFA_4_21",
            "rfa_4_22" AS "RFA_4_22",
            "rfa_4_23" AS "RFA_4_23",
            "rfa_4_24" AS "RFA_4_24",
            "rfa_4_25" AS "RFA_4_25",
            "rfa_4_26" AS "RFA_4_26",
            "rfa_4_27" AS "RFA_4_27",
            "rfa_4_28" AS "RFA_4_28",
            "rfa_4_29" AS "RFA_4_29",
            "rfa_4_30" AS "RFA_4_30",
            "rfa_4_31" AS "RFA_4_31",
            "rfa_4_32" AS "RFA_4_32",
            "rfa_4_33" AS "RFA_4_33",
            "rfa_4_34" AS "RFA_4_34",
            "rfa_4_35" AS "RFA_4_35",
            "rfa_4_36" AS "RFA_4_36",
            "rfa_4_37" AS "RFA_4_37",
            "rfa_4_38" AS "RFA_4_38",
            "rfa_4_39" AS "RFA_4_39",
            "rfa_4_40" AS "RFA_4_40",
            "rfa_4_41" AS "RFA_4_41",
            "rfa_4_42" AS "RFA_4_42",
            "rfa_4_43" AS "RFA_4_43",
            "rfa_4_44" AS "RFA_4_44",
            "rfa_4_45" AS "RFA_4_45",
            "rfa_4_46" AS "RFA_4_46",
            "rfa_4_47" AS "RFA_4_47",
            "rfa_4_48" AS "RFA_4_48",
            "rfa_4_49" AS "RFA_4_49",
            "rfa_4_50" AS "RFA_4_50",
            "rfa_4_51" AS "RFA_4_51",
            "rfa_4_52" AS "RFA_4_52",
            "rfa_4_53" AS "RFA_4_53",
            "rfa_4_54" AS "RFA_4_54",
            "rfa_4_55" AS "RFA_4_55",
            "rfa_4_56" AS "RFA_4_56",
            "rfa_4_57" AS "RFA_4_57",
            "rfa_4_58" AS "RFA_4_58",
            "rfa_4_59" AS "RFA_4_59",
            "rfa_4_60" AS "RFA_4_60",
            "rfa_4_61" AS "RFA_4_61",
            "rfa_4_62" AS "RFA_4_62",
            "MAILCODE",
            "PVASTATE",
            "NOEXCH",
            "CHILD18",
            "GENDER",
            "DATASRCE",
            "SOLP3",
            "SOLIH",
            "rfa_20_0" AS "RFA_20_0",
            "rfa_20_1" AS "RFA_20_1",
            "rfa_20_2" AS "RFA_20_2",
            "rfa_20_3" AS "RFA_20_3",
            "rfa_20_4" AS "RFA_20_4",
            "rfa_20_5" AS "RFA_20_5",
            "rfa_20_6" AS "RFA_20_6",
            "rfa_20_7" AS "RFA_20_7",
            "rfa_20_8" AS "RFA_20_8",
            "rfa_20_9" AS "RFA_20_9",
            "rfa_20_10" AS "RFA_20_10",
            "rfa_20_11" AS "RFA_20_11",
            "rfa_20_12" AS "RFA_20_12",
            "rfa_20_13" AS "RFA_20_13",
            "rfa_20_14" AS "RFA_20_14",
            "rfa_20_15" AS "RFA_20_15",
            "rfa_20_16" AS "RFA_20_16",
            "rfa_20_17" AS "RFA_20_17",
            "rfa_20_18" AS "RFA_20_18",
            "rfa_20_19" AS "RFA_20_19",
            "rfa_20_20" AS "RFA_20_20",
            "rfa_20_21" AS "RFA_20_21",
            "rfa_20_22" AS "RFA_20_22",
            "rfa_20_23" AS "RFA_20_23",
            "rfa_20_24" AS "RFA_20_24",
            "rfa_20_25" AS "RFA_20_25",
            "rfa_20_26" AS "RFA_20_26",
            "rfa_20_27" AS "RFA_20_27",
            "rfa_20_28" AS "RFA_20_28",
            "rfa_20_29" AS "RFA_20_29",
            "rfa_20_30" AS "RFA_20_30",
            "rfa_20_31" AS "RFA_20_31",
            "rfa_20_32" AS "RFA_20_32",
            "rfa_20_33" AS "RFA_20_33",
            "rfa_20_34" AS "RFA_20_34",
            "rfa_20_35" AS "RFA_20_35",
            "rfa_20_36" AS "RFA_20_36",
            "rfa_20_37" AS "RFA_20_37",
            "rfa_20_38" AS "RFA_20_38",
            "rfa_20_39" AS "RFA_20_39",
            "rfa_20_40" AS "RFA_20_40",
            "rfa_20_41" AS "RFA_20_41",
            "rfa_20_42" AS "RFA_20_42",
            "rfa_20_43" AS "RFA_20_43",
            "rfa_20_44" AS "RFA_20_44",
            "rfa_20_45" AS "RFA_20_45",
            "rfa_20_46" AS "RFA_20_46",
            "rfa_20_47" AS "RFA_20_47",
            "rfa_20_48" AS "RFA_20_48",
            "rfa_20_49" AS "RFA_20_49",
            "rfa_20_50" AS "RFA_20_50",
            "rfa_20_51" AS "RFA_20_51",
            "rfa_20_52" AS "RFA_20_52",
            "rfa_20_53" AS "RFA_20_53",
            "rfa_20_54" AS "RFA_20_54",
            "rfa_20_55" AS "RFA_20_55",
            "rfa_20_56" AS "RFA_20_56",
            "rfa_20_57" AS "RFA_20_57",
            "rfa_20_58" AS "RFA_20_58",
            "rfa_20_59" AS "RFA_20_59",
            "rfa_20_60" AS "RFA_20_60",
            "rfa_20_61" AS "RFA_20_61",
            "rfa_20_62" AS "RFA_20_62",
            "rfa_20_63" AS "RFA_20_63",
            "rfa_20_64" AS "RFA_20_64",
            "rfa_20_65" AS "RFA_20_65",
            "rfa_20_66" AS "RFA_20_66",
            "rfa_20_67" AS "RFA_20_67",
            "rfa_20_68" AS "RFA_20_68",
            "rfa_20_69" AS "RFA_20_69",
            "rfa_20_70" AS "RFA_20_70",
            "rfa_20_71" AS "RFA_20_71",
            "rfa_20_72" AS "RFA_20_72",
            "rfa_20_73" AS "RFA_20_73",
            "rfa_20_74" AS "RFA_20_74",
            "rfa_20_75" AS "RFA_20_75",
            "rfa_20_76" AS "RFA_20_76",
            "rfa_20_77" AS "RFA_20_77",
            "rfa_20_78" AS "RFA_20_78",
            "RECINHSE",
            "RECP3",
            "RECPGVG",
            "RECSWEEP",
            "mdmaud_0" AS "MDMAUD_0",
            "mdmaud_1" AS "MDMAUD_1",
            "mdmaud_2" AS "MDMAUD_2",
            "mdmaud_3" AS "MDMAUD_3",
            "mdmaud_4" AS "MDMAUD_4",
            "mdmaud_5" AS "MDMAUD_5",
            "mdmaud_6" AS "MDMAUD_6",
            "mdmaud_7" AS "MDMAUD_7",
            "mdmaud_8" AS "MDMAUD_8",
            "mdmaud_9" AS "MDMAUD_9",
            "mdmaud_10" AS "MDMAUD_10",
            "mdmaud_11" AS "MDMAUD_11",
            "mdmaud_12" AS "MDMAUD_12",
            "mdmaud_13" AS "MDMAUD_13",
            "mdmaud_14" AS "MDMAUD_14",
            "mdmaud_15" AS "MDMAUD_15",
            "mdmaud_16" AS "MDMAUD_16",
            "mdmaud_17" AS "MDMAUD_17",
            "mdmaud_18" AS "MDMAUD_18",
            "mdmaud_19" AS "MDMAUD_19",
            "mdmaud_20" AS "MDMAUD_20",
            "mdmaud_21" AS "MDMAUD_21",
            "mdmaud_22" AS "MDMAUD_22",
            "mdmaud_23" AS "MDMAUD_23",
            "mdmaud_24" AS "MDMAUD_24",
            "mdmaud_25" AS "MDMAUD_25",
            "mdmaud_26" AS "MDMAUD_26",
            "rfa_15_0" AS "RFA_15_0",
            "rfa_15_1" AS "RFA_15_1",
            "rfa_15_2" AS "RFA_15_2",
            "rfa_15_3" AS "RFA_15_3",
            "rfa_15_4" AS "RFA_15_4",
            "rfa_15_5" AS "RFA_15_5",
            "rfa_15_6" AS "RFA_15_6",
            "rfa_15_7" AS "RFA_15_7",
            "rfa_15_8" AS "RFA_15_8",
            "rfa_15_9" AS "RFA_15_9",
            "rfa_15_10" AS "RFA_15_10",
            "rfa_15_11" AS "RFA_15_11",
            "rfa_15_12" AS "RFA_15_12",
            "rfa_15_13" AS "RFA_15_13",
            "rfa_15_14" AS "RFA_15_14",
            "rfa_15_15" AS "RFA_15_15",
            "rfa_15_16" AS "RFA_15_16",
            "rfa_15_17" AS "RFA_15_17",
            "rfa_15_18" AS "RFA_15_18",
            "rfa_15_19" AS "RFA_15_19",
            "rfa_15_20" AS "RFA_15_20",
            "rfa_15_21" AS "RFA_15_21",
            "rfa_15_22" AS "RFA_15_22",
            "rfa_15_23" AS "RFA_15_23",
            "rfa_15_24" AS "RFA_15_24",
            "rfa_15_25" AS "RFA_15_25",
            "rfa_15_26" AS "RFA_15_26",
            "rfa_15_27" AS "RFA_15_27",
            "rfa_15_28" AS "RFA_15_28",
            "rfa_15_29" AS "RFA_15_29",
            "rfa_15_30" AS "RFA_15_30",
            "rfa_15_31" AS "RFA_15_31",
            "rfa_15_32" AS "RFA_15_32",
            "rfa_15_33" AS "RFA_15_33",
            "rfa_2_0" AS "RFA_2_0",
            "rfa_2_1" AS "RFA_2_1",
            "rfa_2_2" AS "RFA_2_2",
            "rfa_2_3" AS "RFA_2_3",
            "rfa_2_4" AS "RFA_2_4",
            "rfa_2_5" AS "RFA_2_5",
            "rfa_2_6" AS "RFA_2_6",
            "rfa_2_7" AS "RFA_2_7",
            "rfa_2_8" AS "RFA_2_8",
            "rfa_2_9" AS "RFA_2_9",
            "rfa_2_10" AS "RFA_2_10",
            "rfa_2_11" AS "RFA_2_11",
            "rfa_2_12" AS "RFA_2_12",
            "rfa_2_13" AS "RFA_2_13",
            "domain_0" AS "DOMAIN_0",
            "domain_1" AS "DOMAIN_1",
            "domain_2" AS "DOMAIN_2",
            "domain_3" AS "DOMAIN_3",
            "domain_4" AS "DOMAIN_4",
            "domain_5" AS "DOMAIN_5",
            "domain_6" AS "DOMAIN_6",
            "domain_7" AS "DOMAIN_7",
            "domain_8" AS "DOMAIN_8",
            "domain_9" AS "DOMAIN_9",
            "domain_10" AS "DOMAIN_10",
            "domain_11" AS "DOMAIN_11",
            "domain_12" AS "DOMAIN_12",
            "domain_13" AS "DOMAIN_13",
            "domain_14" AS "DOMAIN_14",
            "domain_15" AS "DOMAIN_15",
            "domain_16" AS "DOMAIN_16",
            "MAJOR",
            "GEOCODE",
            "COLLECT1",
            "VETERANS",
            "BIBLE",
            "CATLG",
            "rfa_23_0" AS "RFA_23_0",
            "rfa_23_1" AS "RFA_23_1",
            "rfa_23_2" AS "RFA_23_2",
            "rfa_23_3" AS "RFA_23_3",
            "rfa_23_4" AS "RFA_23_4",
            "rfa_23_5" AS "RFA_23_5",
            "rfa_23_6" AS "RFA_23_6",
            "rfa_23_7" AS "RFA_23_7",
            "rfa_23_8" AS "RFA_23_8",
            "rfa_23_9" AS "RFA_23_9",
            "rfa_23_10" AS "RFA_23_10",
            "rfa_23_11" AS "RFA_23_11",
            "rfa_23_12" AS "RFA_23_12",
            "rfa_23_13" AS "RFA_23_13",
            "rfa_23_14" AS "RFA_23_14",
            "rfa_23_15" AS "RFA_23_15",
            "rfa_23_16" AS "RFA_23_16",
            "rfa_23_17" AS "RFA_23_17",
            "rfa_23_18" AS "RFA_23_18",
            "rfa_23_19" AS "RFA_23_19",
            "rfa_23_20" AS "RFA_23_20",
            "rfa_23_21" AS "RFA_23_21",
            "rfa_23_22" AS "RFA_23_22",
            "rfa_23_23" AS "RFA_23_23",
            "rfa_23_24" AS "RFA_23_24",
            "rfa_23_25" AS "RFA_23_25",
            "rfa_23_26" AS "RFA_23_26",
            "rfa_23_27" AS "RFA_23_27",
            "rfa_23_28" AS "RFA_23_28",
            "rfa_23_29" AS "RFA_23_29",
            "rfa_23_30" AS "RFA_23_30",
            "rfa_23_31" AS "RFA_23_31",
            "rfa_23_32" AS "RFA_23_32",
            "rfa_23_33" AS "RFA_23_33",
            "rfa_23_34" AS "RFA_23_34",
            "rfa_23_35" AS "RFA_23_35",
            "rfa_23_36" AS "RFA_23_36",
            "rfa_23_37" AS "RFA_23_37",
            "rfa_23_38" AS "RFA_23_38",
            "rfa_23_39" AS "RFA_23_39",
            "rfa_23_40" AS "RFA_23_40",
            "rfa_23_41" AS "RFA_23_41",
            "rfa_23_42" AS "RFA_23_42",
            "rfa_23_43" AS "RFA_23_43",
            "rfa_23_44" AS "RFA_23_44",
            "rfa_23_45" AS "RFA_23_45",
            "rfa_23_46" AS "RFA_23_46",
            "rfa_23_47" AS "RFA_23_47",
            "rfa_23_48" AS "RFA_23_48",
            "rfa_23_49" AS "RFA_23_49",
            "rfa_23_50" AS "RFA_23_50",
            "rfa_23_51" AS "RFA_23_51",
            "rfa_23_52" AS "RFA_23_52",
            "rfa_23_53" AS "RFA_23_53",
            "rfa_23_54" AS "RFA_23_54",
            "rfa_23_55" AS "RFA_23_55",
            "rfa_23_56" AS "RFA_23_56",
            "rfa_23_57" AS "RFA_23_57",
            "rfa_23_58" AS "RFA_23_58",
            "rfa_23_59" AS "RFA_23_59",
            "rfa_23_60" AS "RFA_23_60",
            "rfa_23_61" AS "RFA_23_61",
            "rfa_23_62" AS "RFA_23_62",
            "rfa_23_63" AS "RFA_23_63",
            "rfa_23_64" AS "RFA_23_64",
            "rfa_23_65" AS "RFA_23_65",
            "rfa_23_66" AS "RFA_23_66",
            "rfa_23_67" AS "RFA_23_67",
            "rfa_23_68" AS "RFA_23_68",
            "rfa_23_69" AS "RFA_23_69",
            "rfa_23_70" AS "RFA_23_70",
            "rfa_23_71" AS "RFA_23_71",
            "rfa_23_72" AS "RFA_23_72",
            "rfa_23_73" AS "RFA_23_73",
            "rfa_23_74" AS "RFA_23_74",
            "rfa_23_75" AS "RFA_23_75",
            "rfa_23_76" AS "RFA_23_76",
            "rfa_23_77" AS "RFA_23_77",
            "rfa_23_78" AS "RFA_23_78",
            "rfa_23_79" AS "RFA_23_79",
            "rfa_23_80" AS "RFA_23_80",
            "rfa_23_81" AS "RFA_23_81",
            "rfa_24_0" AS "RFA_24_0",
            "rfa_24_1" AS "RFA_24_1",
            "rfa_24_2" AS "RFA_24_2",
            "rfa_24_3" AS "RFA_24_3",
            "rfa_24_4" AS "RFA_24_4",
            "rfa_24_5" AS "RFA_24_5",
            "rfa_24_6" AS "RFA_24_6",
            "rfa_24_7" AS "RFA_24_7",
            "rfa_24_8" AS "RFA_24_8",
            "rfa_24_9" AS "RFA_24_9",
            "rfa_24_10" AS "RFA_24_10",
            "rfa_24_11" AS "RFA_24_11",
            "rfa_24_12" AS "RFA_24_12",
            "rfa_24_13" AS "RFA_24_13",
            "rfa_24_14" AS "RFA_24_14",
            "rfa_24_15" AS "RFA_24_15",
            "rfa_24_16" AS "RFA_24_16",
            "rfa_24_17" AS "RFA_24_17",
            "rfa_24_18" AS "RFA_24_18",
            "rfa_24_19" AS "RFA_24_19",
            "rfa_24_20" AS "RFA_24_20",
            "rfa_24_21" AS "RFA_24_21",
            "rfa_24_22" AS "RFA_24_22",
            "rfa_24_23" AS "RFA_24_23",
            "rfa_24_24" AS "RFA_24_24",
            "rfa_24_25" AS "RFA_24_25",
            "rfa_24_26" AS "RFA_24_26",
            "rfa_24_27" AS "RFA_24_27",
            "rfa_24_28" AS "RFA_24_28",
            "rfa_24_29" AS "RFA_24_29",
            "rfa_24_30" AS "RFA_24_30",
            "rfa_24_31" AS "RFA_24_31",
            "rfa_24_32" AS "RFA_24_32",
            "rfa_24_33" AS "RFA_24_33",
            "rfa_24_34" AS "RFA_24_34",
            "rfa_24_35" AS "RFA_24_35",
            "rfa_24_36" AS "RFA_24_36",
            "rfa_24_37" AS "RFA_24_37",
            "rfa_24_38" AS "RFA_24_38",
            "rfa_24_39" AS "RFA_24_39",
            "rfa_24_40" AS "RFA_24_40",
            "rfa_24_41" AS "RFA_24_41",
            "rfa_24_42" AS "RFA_24_42",
            "rfa_24_43" AS "RFA_24_43",
            "rfa_24_44" AS "RFA_24_44",
            "rfa_24_45" AS "RFA_24_45",
            "rfa_24_46" AS "RFA_24_46",
            "rfa_24_47" AS "RFA_24_47",
            "rfa_24_48" AS "RFA_24_48",
            "rfa_24_49" AS "RFA_24_49",
            "rfa_24_50" AS "RFA_24_50",
            "rfa_24_51" AS "RFA_24_51",
            "rfa_24_52" AS "RFA_24_52",
            "rfa_24_53" AS "RFA_24_53",
            "rfa_24_54" AS "RFA_24_54",
            "rfa_24_55" AS "RFA_24_55",
            "rfa_24_56" AS "RFA_24_56",
            "rfa_24_57" AS "RFA_24_57",
            "rfa_24_58" AS "RFA_24_58",
            "rfa_24_59" AS "RFA_24_59",
            "rfa_24_60" AS "RFA_24_60",
            "rfa_24_61" AS "RFA_24_61",
            "rfa_24_62" AS "RFA_24_62",
            "rfa_24_63" AS "RFA_24_63",
            "rfa_24_64" AS "RFA_24_64",
            "rfa_24_65" AS "RFA_24_65",
            "rfa_24_66" AS "RFA_24_66",
            "rfa_24_67" AS "RFA_24_67",
            "rfa_24_68" AS "RFA_24_68",
            "rfa_24_69" AS "RFA_24_69",
            "rfa_24_70" AS "RFA_24_70",
            "rfa_24_71" AS "RFA_24_71",
            "rfa_24_72" AS "RFA_24_72",
            "rfa_24_73" AS "RFA_24_73",
            "rfa_24_74" AS "RFA_24_74",
            "rfa_24_75" AS "RFA_24_75",
            "rfa_24_76" AS "RFA_24_76",
            "rfa_24_77" AS "RFA_24_77",
            "rfa_24_78" AS "RFA_24_78",
            "rfa_24_79" AS "RFA_24_79",
            "rfa_24_80" AS "RFA_24_80",
            "rfa_24_81" AS "RFA_24_81",
            "rfa_24_82" AS "RFA_24_82",
            "rfa_24_83" AS "RFA_24_83",
            "rfa_24_84" AS "RFA_24_84",
            "rfa_24_85" AS "RFA_24_85",
            "rfa_24_86" AS "RFA_24_86",
            "rfa_24_87" AS "RFA_24_87",
            "rfa_24_88" AS "RFA_24_88",
            "rfa_24_89" AS "RFA_24_89",
            "rfa_24_90" AS "RFA_24_90",
            "rfa_24_91" AS "RFA_24_91",
            "rfa_24_92" AS "RFA_24_92",
            "rfa_24_93" AS "RFA_24_93",
            "rfa_24_94" AS "RFA_24_94",
            "rfa_24_95" AS "RFA_24_95",
            "RFA_2R",
            "rfa_5_0" AS "RFA_5_0",
            "rfa_5_1" AS "RFA_5_1",
            "rfa_5_2" AS "RFA_5_2",
            "rfa_5_3" AS "RFA_5_3",
            "rfa_5_4" AS "RFA_5_4",
            "rfa_5_5" AS "RFA_5_5",
            "rfa_5_6" AS "RFA_5_6",
            "rfa_5_7" AS "RFA_5_7",
            "rfa_5_8" AS "RFA_5_8",
            "rfa_5_9" AS "RFA_5_9",
            "rfa_5_10" AS "RFA_5_10",
            "rfa_5_11" AS "RFA_5_11",
            "rfa_5_12" AS "RFA_5_12",
            "rfa_5_13" AS "RFA_5_13",
            "rfa_5_14" AS "RFA_5_14",
            "rfa_5_15" AS "RFA_5_15",
            "rfa_5_16" AS "RFA_5_16",
            "rfa_5_17" AS "RFA_5_17",
            "rfa_5_18" AS "RFA_5_18",
            "rfa_5_19" AS "RFA_5_19",
            "rfa_5_20" AS "RFA_5_20",
            "rfa_5_21" AS "RFA_5_21",
            "rfa_5_22" AS "RFA_5_22",
            "rfa_5_23" AS "RFA_5_23",
            "rfa_5_24" AS "RFA_5_24",
            "rfa_5_25" AS "RFA_5_25",
            "rfa_5_26" AS "RFA_5_26",
            "rfa_5_27" AS "RFA_5_27",
            "rfa_5_28" AS "RFA_5_28",
            "rfa_5_29" AS "RFA_5_29",
            "rfa_5_30" AS "RFA_5_30",
            "rfa_5_31" AS "RFA_5_31",
            "rfa_5_32" AS "RFA_5_32",
            "rfa_5_33" AS "RFA_5_33",
            "rfa_5_34" AS "RFA_5_34",
            "rfa_5_35" AS "RFA_5_35",
            "rfa_5_36" AS "RFA_5_36",
            "rfa_5_37" AS "RFA_5_37",
            "rfa_5_38" AS "RFA_5_38",
            "rfa_5_39" AS "RFA_5_39",
            "rfa_5_40" AS "RFA_5_40",
            "cluster_0" AS "CLUSTER_0",
            "cluster_1" AS "CLUSTER_1",
            "cluster_2" AS "CLUSTER_2",
            "cluster_3" AS "CLUSTER_3",
            "cluster_4" AS "CLUSTER_4",
            "cluster_5" AS "CLUSTER_5",
            "cluster_6" AS "CLUSTER_6",
            "cluster_7" AS "CLUSTER_7",
            "cluster_8" AS "CLUSTER_8",
            "cluster_9" AS "CLUSTER_9",
            "cluster_10" AS "CLUSTER_10",
            "cluster_11" AS "CLUSTER_11",
            "cluster_12" AS "CLUSTER_12",
            "cluster_13" AS "CLUSTER_13",
            "cluster_14" AS "CLUSTER_14",
            "cluster_15" AS "CLUSTER_15",
            "cluster_16" AS "CLUSTER_16",
            "cluster_17" AS "CLUSTER_17",
            "cluster_18" AS "CLUSTER_18",
            "cluster_19" AS "CLUSTER_19",
            "cluster_20" AS "CLUSTER_20",
            "cluster_21" AS "CLUSTER_21",
            "cluster_22" AS "CLUSTER_22",
            "cluster_23" AS "CLUSTER_23",
            "cluster_24" AS "CLUSTER_24",
            "cluster_25" AS "CLUSTER_25",
            "cluster_26" AS "CLUSTER_26",
            "cluster_27" AS "CLUSTER_27",
            "cluster_28" AS "CLUSTER_28",
            "cluster_29" AS "CLUSTER_29",
            "cluster_30" AS "CLUSTER_30",
            "cluster_31" AS "CLUSTER_31",
            "cluster_32" AS "CLUSTER_32",
            "cluster_33" AS "CLUSTER_33",
            "cluster_34" AS "CLUSTER_34",
            "cluster_35" AS "CLUSTER_35",
            "cluster_36" AS "CLUSTER_36",
            "cluster_37" AS "CLUSTER_37",
            "cluster_38" AS "CLUSTER_38",
            "cluster_39" AS "CLUSTER_39",
            "cluster_40" AS "CLUSTER_40",
            "cluster_41" AS "CLUSTER_41",
            "cluster_42" AS "CLUSTER_42",
            "cluster_43" AS "CLUSTER_43",
            "cluster_44" AS "CLUSTER_44",
            "cluster_45" AS "CLUSTER_45",
            "cluster_46" AS "CLUSTER_46",
            "cluster_47" AS "CLUSTER_47",
            "cluster_48" AS "CLUSTER_48",
            "cluster_49" AS "CLUSTER_49",
            "cluster_50" AS "CLUSTER_50",
            "cluster_51" AS "CLUSTER_51",
            "cluster_52" AS "CLUSTER_52",
            "cluster_53" AS "CLUSTER_53",
            "AGEFLAG",
            "HOMEOWNR",
            "CHILD03",
            "CHILD07",
            "CHILD12",
            "HOMEE",
            "PETS",
            "CDPLAY",
            "STEREO",
            "PCOWNERS",
            "PHOTO",
            "MDMAUD_R",
            "MDMAUD_F",
            "CRAFTS",
            "FISHER",
            "GARDENIN",
            "BOATS",
            "WALKER",
            "KIDSTUFF",
            "CARDS",
            "PLATES",
            "LIFESRC",
            "PEPSTRFL",
            "rfa_3_0" AS "RFA_3_0",
            "rfa_3_1" AS "RFA_3_1",
            "rfa_3_2" AS "RFA_3_2",
            "rfa_3_3" AS "RFA_3_3",
            "rfa_3_4" AS "RFA_3_4",
            "rfa_3_5" AS "RFA_3_5",
            "rfa_3_6" AS "RFA_3_6",
            "rfa_3_7" AS "RFA_3_7",
            "rfa_3_8" AS "RFA_3_8",
            "rfa_3_9" AS "RFA_3_9",
            "rfa_3_10" AS "RFA_3_10",
            "rfa_3_11" AS "RFA_3_11",
            "rfa_3_12" AS "RFA_3_12",
            "rfa_3_13" AS "RFA_3_13",
            "rfa_3_14" AS "RFA_3_14",
            "rfa_3_15" AS "RFA_3_15",
            "rfa_3_16" AS "RFA_3_16",
            "rfa_3_17" AS "RFA_3_17",
            "rfa_3_18" AS "RFA_3_18",
            "rfa_3_19" AS "RFA_3_19",
            "rfa_3_20" AS "RFA_3_20",
            "rfa_3_21" AS "RFA_3_21",
            "rfa_3_22" AS "RFA_3_22",
            "rfa_3_23" AS "RFA_3_23",
            "rfa_3_24" AS "RFA_3_24",
            "rfa_3_25" AS "RFA_3_25",
            "rfa_3_26" AS "RFA_3_26",
            "rfa_3_27" AS "RFA_3_27",
            "rfa_3_28" AS "RFA_3_28",
            "rfa_3_29" AS "RFA_3_29",
            "rfa_3_30" AS "RFA_3_30",
            "rfa_3_31" AS "RFA_3_31",
            "rfa_3_32" AS "RFA_3_32",
            "rfa_3_33" AS "RFA_3_33",
            "rfa_3_34" AS "RFA_3_34",
            "rfa_3_35" AS "RFA_3_35",
            "rfa_3_36" AS "RFA_3_36",
            "rfa_3_37" AS "RFA_3_37",
            "rfa_3_38" AS "RFA_3_38",
            "rfa_3_39" AS "RFA_3_39",
            "rfa_3_40" AS "RFA_3_40",
            "rfa_3_41" AS "RFA_3_41",
            "rfa_3_42" AS "RFA_3_42",
            "rfa_3_43" AS "RFA_3_43",
            "rfa_3_44" AS "RFA_3_44",
            "rfa_3_45" AS "RFA_3_45",
            "rfa_3_46" AS "RFA_3_46",
            "rfa_3_47" AS "RFA_3_47",
            "rfa_3_48" AS "RFA_3_48",
            "rfa_3_49" AS "RFA_3_49",
            "rfa_3_50" AS "RFA_3_50",
            "rfa_3_51" AS "RFA_3_51",
            "rfa_3_52" AS "RFA_3_52",
            "rfa_3_53" AS "RFA_3_53",
            "rfa_3_54" AS "RFA_3_54",
            "rfa_3_55" AS "RFA_3_55",
            "rfa_3_56" AS "RFA_3_56",
            "rfa_3_57" AS "RFA_3_57",
            "rfa_3_58" AS "RFA_3_58",
            "rfa_3_59" AS "RFA_3_59",
            "rfa_3_60" AS "RFA_3_60",
            "rfa_3_61" AS "RFA_3_61",
            "rfa_3_62" AS "RFA_3_62",
            "rfa_3_63" AS "RFA_3_63",
            "rfa_3_64" AS "RFA_3_64",
            "rfa_3_65" AS "RFA_3_65",
            "rfa_3_66" AS "RFA_3_66",
            "rfa_3_67" AS "RFA_3_67",
            "rfa_3_68" AS "RFA_3_68",
            "RFA_2A",
            "rfa_10_0" AS "RFA_10_0",
            "rfa_10_1" AS "RFA_10_1",
            "rfa_10_2" AS "RFA_10_2",
            "rfa_10_3" AS "RFA_10_3",
            "rfa_10_4" AS "RFA_10_4",
            "rfa_10_5" AS "RFA_10_5",
            "rfa_10_6" AS "RFA_10_6",
            "rfa_10_7" AS "RFA_10_7",
            "rfa_10_8" AS "RFA_10_8",
            "rfa_10_9" AS "RFA_10_9",
            "rfa_10_10" AS "RFA_10_10",
            "rfa_10_11" AS "RFA_10_11",
            "rfa_10_12" AS "RFA_10_12",
            "rfa_10_13" AS "RFA_10_13",
            "rfa_10_14" AS "RFA_10_14",
            "rfa_10_15" AS "RFA_10_15",
            "rfa_10_16" AS "RFA_10_16",
            "rfa_10_17" AS "RFA_10_17",
            "rfa_10_18" AS "RFA_10_18",
            "rfa_10_19" AS "RFA_10_19",
            "rfa_10_20" AS "RFA_10_20",
            "rfa_10_21" AS "RFA_10_21",
            "rfa_10_22" AS "RFA_10_22",
            "rfa_10_23" AS "RFA_10_23",
            "rfa_10_24" AS "RFA_10_24",
            "rfa_10_25" AS "RFA_10_25",
            "rfa_10_26" AS "RFA_10_26",
            "rfa_10_27" AS "RFA_10_27",
            "rfa_10_28" AS "RFA_10_28",
            "rfa_10_29" AS "RFA_10_29",
            "rfa_10_30" AS "RFA_10_30",
            "rfa_10_31" AS "RFA_10_31",
            "rfa_10_32" AS "RFA_10_32",
            "rfa_10_33" AS "RFA_10_33",
            "rfa_10_34" AS "RFA_10_34",
            "rfa_10_35" AS "RFA_10_35",
            "rfa_10_36" AS "RFA_10_36",
            "rfa_10_37" AS "RFA_10_37",
            "rfa_10_38" AS "RFA_10_38",
            "rfa_10_39" AS "RFA_10_39",
            "rfa_10_40" AS "RFA_10_40",
            "rfa_10_41" AS "RFA_10_41",
            "rfa_10_42" AS "RFA_10_42",
            "rfa_10_43" AS "RFA_10_43",
            "rfa_10_44" AS "RFA_10_44",
            "rfa_10_45" AS "RFA_10_45",
            "rfa_10_46" AS "RFA_10_46",
            "rfa_10_47" AS "RFA_10_47",
            "rfa_10_48" AS "RFA_10_48",
            "rfa_10_49" AS "RFA_10_49",
            "rfa_10_50" AS "RFA_10_50",
            "rfa_10_51" AS "RFA_10_51",
            "rfa_10_52" AS "RFA_10_52",
            "rfa_10_53" AS "RFA_10_53",
            "rfa_10_54" AS "RFA_10_54",
            "rfa_10_55" AS "RFA_10_55",
            "rfa_10_56" AS "RFA_10_56",
            "rfa_10_57" AS "RFA_10_57",
            "rfa_10_58" AS "RFA_10_58",
            "rfa_10_59" AS "RFA_10_59",
            "rfa_10_60" AS "RFA_10_60",
            "rfa_10_61" AS "RFA_10_61",
            "rfa_10_62" AS "RFA_10_62",
            "rfa_10_63" AS "RFA_10_63",
            "rfa_10_64" AS "RFA_10_64",
            "rfa_10_65" AS "RFA_10_65",
            "rfa_10_66" AS "RFA_10_66",
            "rfa_10_67" AS "RFA_10_67",
            "rfa_10_68" AS "RFA_10_68",
            "rfa_10_69" AS "RFA_10_69",
            "rfa_10_70" AS "RFA_10_70",
            "rfa_10_71" AS "RFA_10_71",
            "rfa_10_72" AS "RFA_10_72",
            "rfa_10_73" AS "RFA_10_73",
            "rfa_10_74" AS "RFA_10_74",
            "rfa_10_75" AS "RFA_10_75",
            "rfa_10_76" AS "RFA_10_76",
            "rfa_10_77" AS "RFA_10_77",
            "rfa_10_78" AS "RFA_10_78",
            "rfa_10_79" AS "RFA_10_79",
            "rfa_10_80" AS "RFA_10_80",
            "rfa_10_81" AS "RFA_10_81",
            "rfa_10_82" AS "RFA_10_82",
            "rfa_10_83" AS "RFA_10_83",
            "rfa_10_84" AS "RFA_10_84",
            "rfa_10_85" AS "RFA_10_85",
            "rfa_10_86" AS "RFA_10_86",
            "rfa_10_87" AS "RFA_10_87",
            "rfa_10_88" AS "RFA_10_88",
            "rfa_10_89" AS "RFA_10_89",
            "rfa_10_90" AS "RFA_10_90",
            "rfa_10_91" AS "RFA_10_91",
            "rfa_10_92" AS "RFA_10_92",
            "rfa_13_0" AS "RFA_13_0",
            "rfa_13_1" AS "RFA_13_1",
            "rfa_13_2" AS "RFA_13_2",
            "rfa_13_3" AS "RFA_13_3",
            "rfa_13_4" AS "RFA_13_4",
            "rfa_13_5" AS "RFA_13_5",
            "rfa_13_6" AS "RFA_13_6",
            "rfa_13_7" AS "RFA_13_7",
            "rfa_13_8" AS "RFA_13_8",
            "rfa_13_9" AS "RFA_13_9",
            "rfa_13_10" AS "RFA_13_10",
            "rfa_13_11" AS "RFA_13_11",
            "rfa_13_12" AS "RFA_13_12",
            "rfa_13_13" AS "RFA_13_13",
            "rfa_13_14" AS "RFA_13_14",
            "rfa_13_15" AS "RFA_13_15",
            "rfa_13_16" AS "RFA_13_16",
            "rfa_13_17" AS "RFA_13_17",
            "rfa_13_18" AS "RFA_13_18",
            "rfa_13_19" AS "RFA_13_19",
            "rfa_13_20" AS "RFA_13_20",
            "rfa_13_21" AS "RFA_13_21",
            "rfa_13_22" AS "RFA_13_22",
            "rfa_13_23" AS "RFA_13_23",
            "rfa_13_24" AS "RFA_13_24",
            "rfa_13_25" AS "RFA_13_25",
            "rfa_13_26" AS "RFA_13_26",
            "rfa_13_27" AS "RFA_13_27",
            "rfa_13_28" AS "RFA_13_28",
            "rfa_13_29" AS "RFA_13_29",
            "rfa_13_30" AS "RFA_13_30",
            "rfa_13_31" AS "RFA_13_31",
            "rfa_13_32" AS "RFA_13_32",
            "rfa_13_33" AS "RFA_13_33",
            "rfa_13_34" AS "RFA_13_34",
            "rfa_13_35" AS "RFA_13_35",
            "rfa_13_36" AS "RFA_13_36",
            "rfa_13_37" AS "RFA_13_37",
            "rfa_13_38" AS "RFA_13_38",
            "rfa_13_39" AS "RFA_13_39",
            "rfa_13_40" AS "RFA_13_40",
            "rfa_13_41" AS "RFA_13_41",
            "rfa_13_42" AS "RFA_13_42",
            "rfa_13_43" AS "RFA_13_43",
            "rfa_13_44" AS "RFA_13_44",
            "rfa_13_45" AS "RFA_13_45",
            "rfa_13_46" AS "RFA_13_46",
            "rfa_13_47" AS "RFA_13_47",
            "rfa_13_48" AS "RFA_13_48",
            "rfa_13_49" AS "RFA_13_49",
            "rfa_13_50" AS "RFA_13_50",
            "rfa_13_51" AS "RFA_13_51",
            "rfa_13_52" AS "RFA_13_52",
            "rfa_13_53" AS "RFA_13_53",
            "rfa_13_54" AS "RFA_13_54",
            "rfa_13_55" AS "RFA_13_55",
            "rfa_13_56" AS "RFA_13_56",
            "rfa_13_57" AS "RFA_13_57",
            "rfa_13_58" AS "RFA_13_58",
            "rfa_13_59" AS "RFA_13_59",
            "rfa_13_60" AS "RFA_13_60",
            "rfa_13_61" AS "RFA_13_61",
            "rfa_13_62" AS "RFA_13_62",
            "rfa_13_63" AS "RFA_13_63",
            "rfa_13_64" AS "RFA_13_64",
            "rfa_13_65" AS "RFA_13_65",
            "rfa_13_66" AS "RFA_13_66",
            "rfa_13_67" AS "RFA_13_67",
            "rfa_13_68" AS "RFA_13_68",
            "rfa_13_69" AS "RFA_13_69",
            "rfa_13_70" AS "RFA_13_70",
            "rfa_13_71" AS "RFA_13_71",
            "rfa_13_72" AS "RFA_13_72",
            "rfa_13_73" AS "RFA_13_73",
            "rfa_13_74" AS "RFA_13_74",
            "rfa_13_75" AS "RFA_13_75",
            "rfa_13_76" AS "RFA_13_76",
            "rfa_13_77" AS "RFA_13_77",
            "rfa_13_78" AS "RFA_13_78",
            "rfa_13_79" AS "RFA_13_79",
            "rfa_13_80" AS "RFA_13_80",
            "rfa_13_81" AS "RFA_13_81",
            "rfa_13_82" AS "RFA_13_82",
            "rfa_13_83" AS "RFA_13_83",
            "rfa_13_84" AS "RFA_13_84",
            "rfa_13_85" AS "RFA_13_85",
            "rfa_13_86" AS "RFA_13_86",
            "rfa_14_0" AS "RFA_14_0",
            "rfa_14_1" AS "RFA_14_1",
            "rfa_14_2" AS "RFA_14_2",
            "rfa_14_3" AS "RFA_14_3",
            "rfa_14_4" AS "RFA_14_4",
            "rfa_14_5" AS "RFA_14_5",
            "rfa_14_6" AS "RFA_14_6",
            "rfa_14_7" AS "RFA_14_7",
            "rfa_14_8" AS "RFA_14_8",
            "rfa_14_9" AS "RFA_14_9",
            "rfa_14_10" AS "RFA_14_10",
            "rfa_14_11" AS "RFA_14_11",
            "rfa_14_12" AS "RFA_14_12",
            "rfa_14_13" AS "RFA_14_13",
            "rfa_14_14" AS "RFA_14_14",
            "rfa_14_15" AS "RFA_14_15",
            "rfa_14_16" AS "RFA_14_16",
            "rfa_14_17" AS "RFA_14_17",
            "rfa_14_18" AS "RFA_14_18",
            "rfa_14_19" AS "RFA_14_19",
            "rfa_14_20" AS "RFA_14_20",
            "rfa_14_21" AS "RFA_14_21",
            "rfa_14_22" AS "RFA_14_22",
            "rfa_14_23" AS "RFA_14_23",
            "rfa_14_24" AS "RFA_14_24",
            "rfa_14_25" AS "RFA_14_25",
            "rfa_14_26" AS "RFA_14_26",
            "rfa_14_27" AS "RFA_14_27",
            "rfa_14_28" AS "RFA_14_28",
            "rfa_14_29" AS "RFA_14_29",
            "rfa_14_30" AS "RFA_14_30",
            "rfa_14_31" AS "RFA_14_31",
            "rfa_14_32" AS "RFA_14_32",
            "rfa_14_33" AS "RFA_14_33",
            "rfa_14_34" AS "RFA_14_34",
            "rfa_14_35" AS "RFA_14_35",
            "rfa_14_36" AS "RFA_14_36",
            "rfa_14_37" AS "RFA_14_37",
            "rfa_14_38" AS "RFA_14_38",
            "rfa_14_39" AS "RFA_14_39",
            "rfa_14_40" AS "RFA_14_40",
            "rfa_14_41" AS "RFA_14_41",
            "rfa_14_42" AS "RFA_14_42",
            "rfa_14_43" AS "RFA_14_43",
            "rfa_14_44" AS "RFA_14_44",
            "rfa_14_45" AS "RFA_14_45",
            "rfa_14_46" AS "RFA_14_46",
            "rfa_14_47" AS "RFA_14_47",
            "rfa_14_48" AS "RFA_14_48",
            "rfa_14_49" AS "RFA_14_49",
            "rfa_14_50" AS "RFA_14_50",
            "rfa_14_51" AS "RFA_14_51",
            "rfa_14_52" AS "RFA_14_52",
            "rfa_14_53" AS "RFA_14_53",
            "rfa_14_54" AS "RFA_14_54",
            "rfa_14_55" AS "RFA_14_55",
            "rfa_14_56" AS "RFA_14_56",
            "rfa_14_57" AS "RFA_14_57",
            "rfa_14_58" AS "RFA_14_58",
            "rfa_14_59" AS "RFA_14_59",
            "rfa_14_60" AS "RFA_14_60",
            "rfa_14_61" AS "RFA_14_61",
            "rfa_14_62" AS "RFA_14_62",
            "rfa_14_63" AS "RFA_14_63",
            "rfa_14_64" AS "RFA_14_64",
            "rfa_14_65" AS "RFA_14_65",
            "rfa_14_66" AS "RFA_14_66",
            "rfa_14_67" AS "RFA_14_67",
            "rfa_14_68" AS "RFA_14_68",
            "rfa_14_69" AS "RFA_14_69",
            "rfa_14_70" AS "RFA_14_70",
            "rfa_14_71" AS "RFA_14_71",
            "rfa_14_72" AS "RFA_14_72",
            "rfa_14_73" AS "RFA_14_73",
            "rfa_14_74" AS "RFA_14_74",
            "rfa_14_75" AS "RFA_14_75",
            "rfa_14_76" AS "RFA_14_76",
            "rfa_14_77" AS "RFA_14_77",
            "rfa_14_78" AS "RFA_14_78",
            "rfa_14_79" AS "RFA_14_79",
            "rfa_14_80" AS "RFA_14_80",
            "rfa_14_81" AS "RFA_14_81",
            "rfa_14_82" AS "RFA_14_82",
            "rfa_14_83" AS "RFA_14_83",
            "rfa_14_84" AS "RFA_14_84",
            "rfa_14_85" AS "RFA_14_85",
            "rfa_14_86" AS "RFA_14_86",
            "rfa_14_87" AS "RFA_14_87",
            "rfa_14_88" AS "RFA_14_88",
            "rfa_14_89" AS "RFA_14_89",
            "rfa_14_90" AS "RFA_14_90",
            "rfa_14_91" AS "RFA_14_91",
            "rfa_14_92" AS "RFA_14_92",
            "rfa_14_93" AS "RFA_14_93",
            "rfa_14_94" AS "RFA_14_94",
            "MDMAUD_A",
            "GEOCODE2",
            COALESCE("AGE", 50.0) AS "AGE",
            COALESCE("NUMCHLD", 1.0) AS "NUMCHLD",
            COALESCE("INCOME", 5.0) AS "INCOME",
            COALESCE("WEALTH1", 9.0) AS "WEALTH1",
            COALESCE("MBCRAFT", 0.0) AS "MBCRAFT",
            COALESCE("MBGARDEN", 0.0) AS "MBGARDEN",
            COALESCE("MBBOOKS", 0.0) AS "MBBOOKS",
            COALESCE("MBCOLECT", 0.0) AS "MBCOLECT",
            COALESCE("MAGFAML", 0.0) AS "MAGFAML",
            COALESCE("MAGFEM", 0.0) AS "MAGFEM",
            COALESCE("MAGMALE", 0.0) AS "MAGMALE",
            COALESCE("PUBGARDN", 0.0) AS "PUBGARDN",
            COALESCE("PUBCULIN", 0.0) AS "PUBCULIN",
            COALESCE("PUBHLTH", 0.0) AS "PUBHLTH",
            COALESCE("PUBDOITY", 0.0) AS "PUBDOITY",
            COALESCE("PUBNEWFN", 0.0) AS "PUBNEWFN",
            COALESCE("PUBPHOTO", 0.0) AS "PUBPHOTO",
            COALESCE("PUBOPP", 0.0) AS "PUBOPP",
            COALESCE("WEALTH2", 9.0) AS "WEALTH2",
            COALESCE("MSA", 0.0) AS "MSA",
            COALESCE("ADI", 13.0) AS "ADI",
            COALESCE("DMA", 803.0) AS "DMA",
            COALESCE("ADATE_3", 9606.0) AS "ADATE_3",
            COALESCE("ADATE_4", 9604.0) AS "ADATE_4",
            COALESCE("ADATE_6", 9603.0) AS "ADATE_6",
            COALESCE("ADATE_7", 9602.0) AS "ADATE_7",
            COALESCE("ADATE_8", 9601.0) AS "ADATE_8",
            COALESCE("ADATE_9", 9511.0) AS "ADATE_9",
            COALESCE("ADATE_10", 9510.0) AS "ADATE_10",
            COALESCE("ADATE_11", 9510.0) AS "ADATE_11",
            COALESCE("ADATE_12", 9508.0) AS "ADATE_12",
            COALESCE("ADATE_13", 9507.0) AS "ADATE_13",
            COALESCE("ADATE_14", 9506.0) AS "ADATE_14",
            COALESCE("ADATE_16", 9503.0) AS "ADATE_16",
            COALESCE("ADATE_17", 9502.0) AS "ADATE_17",
            COALESCE("ADATE_18", 9501.0) AS "ADATE_18",
            COALESCE("ADATE_19", 9411.0) AS "ADATE_19",
            COALESCE("ADATE_20", 9411.0) AS "ADATE_20",
            COALESCE("ADATE_21", 9410.0) AS "ADATE_21",
            COALESCE("ADATE_22", 9409.0) AS "ADATE_22",
            COALESCE("ADATE_23", 9407.0) AS "ADATE_23",
            COALESCE("ADATE_24", 9406.0) AS "ADATE_24",
            COALESCE("RDATE_3", 9606.0) AS "RDATE_3",
            COALESCE("RDATE_4", 9605.0) AS "RDATE_4",
            COALESCE("RDATE_5", 9604.0) AS "RDATE_5",
            COALESCE("RDATE_6", 9603.0) AS "RDATE_6",
            COALESCE("RDATE_7", 9603.0) AS "RDATE_7",
            COALESCE("RDATE_8", 9601.0) AS "RDATE_8",
            COALESCE("RDATE_9", 9512.0) AS "RDATE_9",
            COALESCE("RDATE_10", 9511.0) AS "RDATE_10",
            COALESCE("RDATE_11", 9511.0) AS "RDATE_11",
            COALESCE("RDATE_12", 9509.0) AS "RDATE_12",
            COALESCE("RDATE_13", 9508.0) AS "RDATE_13",
            COALESCE("RDATE_14", 9506.0) AS "RDATE_14",
            COALESCE("RDATE_15", 9505.0) AS "RDATE_15",
            COALESCE("RDATE_16", 9504.0) AS "RDATE_16",
            COALESCE("RDATE_17", 9503.0) AS "RDATE_17",
            COALESCE("RDATE_18", 9501.0) AS "RDATE_18",
            COALESCE("RDATE_19", 9412.0) AS "RDATE_19",
            COALESCE("RDATE_20", 9412.0) AS "RDATE_20",
            COALESCE("RDATE_21", 9411.0) AS "RDATE_21",
            COALESCE("RDATE_22", 9409.0) AS "RDATE_22",
            COALESCE("RDATE_23", 9408.0) AS "RDATE_23",
            COALESCE("RDATE_24", 9407.0) AS "RDATE_24",
            COALESCE("RAMNT_3", 10.0) AS "RAMNT_3",
            COALESCE("RAMNT_4", 10.0) AS "RAMNT_4",
            COALESCE("RAMNT_5", 5.0) AS "RAMNT_5",
            COALESCE("RAMNT_6", 10.0) AS "RAMNT_6",
            COALESCE("RAMNT_7", 10.0) AS "RAMNT_7",
            COALESCE("RAMNT_8", 15.0) AS "RAMNT_8",
            COALESCE("RAMNT_9", 10.0) AS "RAMNT_9",
            COALESCE("RAMNT_10", 10.0) AS "RAMNT_10",
            COALESCE("RAMNT_11", 10.0) AS "RAMNT_11",
            COALESCE("RAMNT_12", 10.0) AS "RAMNT_12",
            COALESCE("RAMNT_13", 10.0) AS "RAMNT_13",
            COALESCE("RAMNT_14", 10.0) AS "RAMNT_14",
            COALESCE("RAMNT_15", 10.0) AS "RAMNT_15",
            COALESCE("RAMNT_16", 10.0) AS "RAMNT_16",
            COALESCE("RAMNT_17", 10.0) AS "RAMNT_17",
            COALESCE("RAMNT_18", 10.0) AS "RAMNT_18",
            COALESCE("RAMNT_19", 10.0) AS "RAMNT_19",
            COALESCE("RAMNT_20", 10.0) AS "RAMNT_20",
            COALESCE("RAMNT_21", 10.0) AS "RAMNT_21",
            COALESCE("RAMNT_22", 10.0) AS "RAMNT_22",
            COALESCE("RAMNT_23", 10.0) AS "RAMNT_23",
            COALESCE("RAMNT_24", 10.0) AS "RAMNT_24",
            "RAMNTALL",
            "MINRAMNT",
            "MAXRAMNT",
            "LASTGIFT",
            COALESCE("NEXTDATE", 9504.0) AS "NEXTDATE",
            COALESCE("TIMELAG", 5.0) AS "TIMELAG",
            "AVGGIFT",
            COALESCE("CLUSTER2", 13.0) AS "CLUSTER2"
        FROM
            kdd
            left join osource_cat_c_cat on kdd."OSOURCE" = osource_cat_c_cat."osource"
            left join zip_cat_c_cat on kdd."ZIP" = zip_cat_c_cat."zip"
            left join rfa_6_cat_c_cat on kdd."RFA_6" = rfa_6_cat_c_cat."rfa_6"
            left join rfa_7_cat_c_cat on kdd."RFA_7" = rfa_7_cat_c_cat."rfa_7"
            left join rfa_8_cat_c_cat on kdd."RFA_8" = rfa_8_cat_c_cat."rfa_8"
            left join rfa_9_cat_c_cat on kdd."RFA_9" = rfa_9_cat_c_cat."rfa_9"
            left join rfa_11_cat_c_cat on kdd."RFA_11" = rfa_11_cat_c_cat."rfa_11"
            left join rfa_12_cat_c_cat on kdd."RFA_12" = rfa_12_cat_c_cat."rfa_12"
            left join rfa_16_cat_c_cat on kdd."RFA_16" = rfa_16_cat_c_cat."rfa_16"
            left join rfa_17_cat_c_cat on kdd."RFA_17" = rfa_17_cat_c_cat."rfa_17"
            left join rfa_18_cat_c_cat on kdd."RFA_18" = rfa_18_cat_c_cat."rfa_18"
            left join rfa_19_cat_c_cat on kdd."RFA_19" = rfa_19_cat_c_cat."rfa_19"
            left join rfa_21_cat_c_cat on kdd."RFA_21" = rfa_21_cat_c_cat."rfa_21"
            left join rfa_22_cat_c_cat on kdd."RFA_22" = rfa_22_cat_c_cat."rfa_22"
            left join merged_state_rfa_4_table on kdd."STATE" = merged_state_rfa_4_table."state"
            AND kdd."RFA_4" = merged_state_rfa_4_table."rfa_4"
            left join rfa_20_expand on kdd."RFA_20" = rfa_20_expand."rfa_20"
            left join merged_mdmaud_rfa_15_table on kdd."MDMAUD" = merged_mdmaud_rfa_15_table."mdmaud"
            AND kdd."RFA_15" = merged_mdmaud_rfa_15_table."rfa_15"
            left join merged_rfa_2_domain_table on kdd."RFA_2" = merged_rfa_2_domain_table."rfa_2"
            AND kdd."DOMAIN" = merged_rfa_2_domain_table."domain"
            left join rfa_23_expand on kdd."RFA_23" = rfa_23_expand."rfa_23"
            left join rfa_24_expand on kdd."RFA_24" = rfa_24_expand."rfa_24"
            left join merged_rfa_5_cluster_table on kdd."RFA_5" = merged_rfa_5_cluster_table."rfa_5"
            AND kdd."CLUSTER" = merged_rfa_5_cluster_table."cluster"
            left join rfa_3_expand on kdd."RFA_3" = rfa_3_expand."rfa_3"
            left join rfa_10_expand on kdd."RFA_10" = rfa_10_expand."rfa_10"
            left join rfa_13_expand on kdd."RFA_13" = rfa_13_expand."rfa_13"
            left join rfa_14_expand on kdd."RFA_14" = rfa_14_expand."rfa_14"
    ) AS data