use datafusion_fuzzer::fuzz_context::RunnerConfig;
use datafusion_fuzzer::oracle::ConfiguredOracle;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Runs the fuzzer end-to-end with a fixed seed.
///
/// After fuzzer feature changes, update the snapshot and review the SQL manually to
/// make sure it still looks reasonable. This also ensures the fuzzer runs are
/// deterministic.
///
/// To update the snapshot after changes, run:
/// `cargo insta test --accept --test integration_test`
#[test]
fn full_run_logs_expected_queries_and_stats_for_no_crash_oracle() -> Result<(), Box<dyn Error>> {
    let log_dir = make_temp_log_dir("integration")?;
    let config_path = generate_default_config_with_oracles(&log_dir, &[ConfiguredOracle::NoCrash])?;
    let run_output = run_fuzzer_once(&config_path)?;

    insta::assert_snapshot!(run_output.query_log, @r#"
    === round=1 query=1 oracle=NoCrashOracle query_seed=310304 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT ((200 * (160 * 117)) = 37.35675), ((INTERVAL '-11 MONS 18 DAYS -0.000000001 SECS' + INTERVAL '-5 MONS 15 DAYS -0.000000001 SECS') - INTERVAL '7 MONS -15 DAYS 0.047470428 SECS'), 7
    FROM t0, t2, t1
    WHERE ((116 / 57) <> ('RL ::F>r^dc32dchu%Btuh9xzgoMXx@*p2(B-p' ~~* to_char(INTERVAL '1 MONS 25 DAYS 0.779232956 SECS', 'iU?,/e)~<')))

    === round=1 query=2 oracle=NoCrashOracle query_seed=310305 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT (44.048763824020284 - -27.66953398751086), 6.6229095
    FROM t0, t2, t1
    WHERE NULL

    === round=1 query=3 oracle=NoCrashOracle query_seed=310306 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT NULL
    FROM t2
    WHERE false

    === round=1 query=4 oracle=NoCrashOracle query_seed=310307 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT (NULL + ((t0.col_t0_2_uint32 % t0.col_t0_2_uint32) / t0.col_t0_2_uint32)), -74
    FROM t0
    WHERE (t0.col_t0_1_float64 = CAST('1976-12-24T08:10:02.711975514+09:00' AS TIMESTAMP))

    === round=1 query=5 oracle=NoCrashOracle query_seed=310308 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT (((4.604354650451057 / t0.col_t0_1_float64) / (t0.col_t0_1_float64 * -63.63222029633913)) / t0.col_t0_1_float64), (((102 % 141) - 176) / (160 * 52)), NULL
    FROM t2, t0, t1
    WHERE false

    === round=2 query=1 oracle=NoCrashOracle query_seed=311304 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT (t1.col_t1_4_decimal128 * ((97585.0000000000 * -29056.0000000000000000000000) - 42588.00000000000000000000000000000)), (((-93 - 4) * (-39 - -82)) - (to_unixtime(t0.col_t0_4_string, 'tvmf:B,seoX0);hvl>4zAj?tsovnkiMb?') * (33 + NULL))), (((25 + 88) + -38) / ((-28 - -84) + -25))
    FROM t1, t0
    WHERE ('"Q^(|bxiMBB@?:%@w' !~~* t0.col_t0_4_string)

    === round=2 query=2 oracle=NoCrashOracle query_seed=311305 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT CAST('16:41:33.438661926' AS TIME), 22
    FROM t2, t0
    RIGHT SEMI JOIN t1 ON ('L/fQTV' !~ to_char((CAST('23:17:05.272779257' AS TIME) + CAST('03:07:03.128481284' AS TIME)), t0.col_t0_4_string))
    WHERE ((t0.col_t0_4_string !~* to_char(INTERVAL '-3 MONS 7 DAYS -0.000000001 SECS', 'UANHM J(=="$M@XZkO9lj$9qO>]!2v7EoH8@mjXdBVXO?:Fk')) OR (t0.col_t0_4_string ~~* 'jM.Vsi3r~8o o>d3).1]#I@O@7lR'))

    === round=2 query=3 oracle=NoCrashOracle query_seed=311306 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT to_unixtime((-74.19841 * -5.758835)), NULL, 86
    FROM t2
    WHERE (CAST('18:52:46.811802572' AS TIME) <> CAST('1985-09-28T16:37:51.509903929-09:00' AS TIMESTAMP))

    === round=2 query=4 oracle=NoCrashOracle query_seed=311307 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT [-86, -86, 24, -7], 76, NULL
    FROM t0, t1
    WHERE false

    === round=2 query=5 oracle=NoCrashOracle query_seed=311308 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT (-24 / (94 - NULL))
    FROM t2
    WHERE true
    "#);

    insta::assert_snapshot!(run_output.stats_summary, @r"
    ============================================================
    🎯 DataFusion Fuzzer - Final Statistics
    ============================================================
    📊 Execution Summary:
      • Rounds Completed: 2
      • Queries Executed: 10
      • Query Success Rate: 60.00%
    ");

    fs::remove_dir_all(&log_dir)?;

    Ok(())
}

#[test]
fn full_run_logs_expected_queries_for_tlp_where_oracle() -> Result<(), Box<dyn Error>> {
    let log_dir = make_temp_log_dir("integration-tlp-where")?;
    let config_path =
        generate_default_config_with_oracles(&log_dir, &[ConfiguredOracle::TlpWhere])?;
    let run_output = run_fuzzer_once(&config_path)?;

    insta::assert_snapshot!(run_output.query_log, @r#"
    === round=1 query=1 oracle=TlpWhereOracle query_seed=310304 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t0

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t0
    WHERE (NULL)
    UNION ALL
    SELECT *
    FROM t0
    WHERE NOT (NULL)
    UNION ALL
    SELECT *
    FROM t0
    WHERE (NULL) IS NULL

    === round=1 query=2 oracle=TlpWhereOracle query_seed=310305 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t1

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t1
    WHERE (('WvuO,(0!!gxEC/Nw>(H.A-N6%fT@)%ks.4TUe{udaTm?i' ~ to_char((CAST('07:52:38.802916459' AS TIME) - CAST('12:17:59.096520490' AS TIME)), to_char(CAST('2047-03-23' AS DATE), 'r,DH7t~Z{cO}[`Wh!3`2^@Byf3sXK@5~D&&0.,_= &9mdV'))))
    UNION ALL
    SELECT *
    FROM t1
    WHERE NOT (('WvuO,(0!!gxEC/Nw>(H.A-N6%fT@)%ks.4TUe{udaTm?i' ~ to_char((CAST('07:52:38.802916459' AS TIME) - CAST('12:17:59.096520490' AS TIME)), to_char(CAST('2047-03-23' AS DATE), 'r,DH7t~Z{cO}[`Wh!3`2^@Byf3sXK@5~D&&0.,_= &9mdV'))))
    UNION ALL
    SELECT *
    FROM t1
    WHERE (('WvuO,(0!!gxEC/Nw>(H.A-N6%fT@)%ks.4TUe{udaTm?i' ~ to_char((CAST('07:52:38.802916459' AS TIME) - CAST('12:17:59.096520490' AS TIME)), to_char(CAST('2047-03-23' AS DATE), 'r,DH7t~Z{cO}[`Wh!3`2^@Byf3sXK@5~D&&0.,_= &9mdV')))) IS NULL

    === round=1 query=3 oracle=TlpWhereOracle query_seed=310306 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t2

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t2
    WHERE (NULL)
    UNION ALL
    SELECT *
    FROM t2
    WHERE NOT (NULL)
    UNION ALL
    SELECT *
    FROM t2
    WHERE (NULL) IS NULL

    === round=1 query=4 oracle=TlpWhereOracle query_seed=310307 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t0

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t0
    WHERE ((to_char(current_time(), ':#y`') IS NOT DISTINCT FROM CAST('2052-05-02' AS DATE)))
    UNION ALL
    SELECT *
    FROM t0
    WHERE NOT ((to_char(current_time(), ':#y`') IS NOT DISTINCT FROM CAST('2052-05-02' AS DATE)))
    UNION ALL
    SELECT *
    FROM t0
    WHERE ((to_char(current_time(), ':#y`') IS NOT DISTINCT FROM CAST('2052-05-02' AS DATE))) IS NULL

    === round=1 query=5 oracle=TlpWhereOracle query_seed=310308 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t0

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t0
    WHERE ((to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17T08:39:22.305135405-09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"'))))
    UNION ALL
    SELECT *
    FROM t0
    WHERE NOT ((to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17T08:39:22.305135405-09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"'))))
    UNION ALL
    SELECT *
    FROM t0
    WHERE ((to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17T08:39:22.305135405-09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))) IS NULL

    === round=2 query=1 oracle=TlpWhereOracle query_seed=311304 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t0

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t0
    WHERE (false)
    UNION ALL
    SELECT *
    FROM t0
    WHERE NOT (false)
    UNION ALL
    SELECT *
    FROM t0
    WHERE (false) IS NULL

    === round=2 query=2 oracle=TlpWhereOracle query_seed=311305 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t0

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t0
    WHERE ((to_char(t0.col_t0_5_timestamp, 'r@$-i2|ckDaNwE:cNhmtN_0$e3gjJYAb|$~9F') ~ to_char(CAST('06:17:58.412287082' AS TIME), to_char(t0.col_t0_5_timestamp, 'l*$;sI8,7DmIx'))))
    UNION ALL
    SELECT *
    FROM t0
    WHERE NOT ((to_char(t0.col_t0_5_timestamp, 'r@$-i2|ckDaNwE:cNhmtN_0$e3gjJYAb|$~9F') ~ to_char(CAST('06:17:58.412287082' AS TIME), to_char(t0.col_t0_5_timestamp, 'l*$;sI8,7DmIx'))))
    UNION ALL
    SELECT *
    FROM t0
    WHERE ((to_char(t0.col_t0_5_timestamp, 'r@$-i2|ckDaNwE:cNhmtN_0$e3gjJYAb|$~9F') ~ to_char(CAST('06:17:58.412287082' AS TIME), to_char(t0.col_t0_5_timestamp, 'l*$;sI8,7DmIx')))) IS NULL

    === round=2 query=3 oracle=TlpWhereOracle query_seed=311306 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t2

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t2
    WHERE (true)
    UNION ALL
    SELECT *
    FROM t2
    WHERE NOT (true)
    UNION ALL
    SELECT *
    FROM t2
    WHERE (true) IS NULL

    === round=2 query=4 oracle=TlpWhereOracle query_seed=311307 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t1

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t1
    WHERE (true)
    UNION ALL
    SELECT *
    FROM t1
    WHERE NOT (true)
    UNION ALL
    SELECT *
    FROM t1
    WHERE (true) IS NULL

    === round=2 query=5 oracle=TlpWhereOracle query_seed=311308 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t2

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t2
    WHERE (true)
    UNION ALL
    SELECT *
    FROM t2
    WHERE NOT (true)
    UNION ALL
    SELECT *
    FROM t2
    WHERE (true) IS NULL
    "#);
    insta::assert_snapshot!(run_output.stats_summary, @"
    ============================================================
    🎯 DataFusion Fuzzer - Final Statistics
    ============================================================
    📊 Execution Summary:
      • Rounds Completed: 2
      • Queries Executed: 20
      • Query Success Rate: 85.00%
    ");

    fs::remove_dir_all(&log_dir)?;

    Ok(())
}

#[test]
fn full_run_logs_expected_queries_for_tlp_having_oracle() -> Result<(), Box<dyn Error>> {
    let log_dir = make_temp_log_dir("integration-tlp-having")?;
    let config_path =
        generate_default_config_with_oracles(&log_dir, &[ConfiguredOracle::TlpHaving])?;
    let run_output = run_fuzzer_once(&config_path)?;

    insta::assert_snapshot!(run_output.query_log, @r#"
    === round=1 query=1 oracle=TlpHavingOracle query_seed=310304 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t0.col_t0_2_uint32, t0.col_t0_1_float64
    FROM t0
    GROUP BY t0.col_t0_2_uint32, t0.col_t0_1_float64

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t0.col_t0_2_uint32, t0.col_t0_1_float64
    FROM t0
    GROUP BY t0.col_t0_2_uint32, t0.col_t0_1_float64
    HAVING (false)
    UNION ALL
    SELECT t0.col_t0_2_uint32, t0.col_t0_1_float64
    FROM t0
    GROUP BY t0.col_t0_2_uint32, t0.col_t0_1_float64
    HAVING NOT (false)
    UNION ALL
    SELECT t0.col_t0_2_uint32, t0.col_t0_1_float64
    FROM t0
    GROUP BY t0.col_t0_2_uint32, t0.col_t0_1_float64
    HAVING (false) IS NULL

    === round=1 query=2 oracle=TlpHavingOracle query_seed=310305 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t1.col_t1_1_int32, t1.col_t1_3_decimal128, t1.col_t1_2_int32
    FROM t1
    GROUP BY t1.col_t1_1_int32, t1.col_t1_3_decimal128, t1.col_t1_2_int32

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t1.col_t1_1_int32, t1.col_t1_3_decimal128, t1.col_t1_2_int32
    FROM t1
    GROUP BY t1.col_t1_1_int32, t1.col_t1_3_decimal128, t1.col_t1_2_int32
    HAVING (true)
    UNION ALL
    SELECT t1.col_t1_1_int32, t1.col_t1_3_decimal128, t1.col_t1_2_int32
    FROM t1
    GROUP BY t1.col_t1_1_int32, t1.col_t1_3_decimal128, t1.col_t1_2_int32
    HAVING NOT (true)
    UNION ALL
    SELECT t1.col_t1_1_int32, t1.col_t1_3_decimal128, t1.col_t1_2_int32
    FROM t1
    GROUP BY t1.col_t1_1_int32, t1.col_t1_3_decimal128, t1.col_t1_2_int32
    HAVING (true) IS NULL

    === round=1 query=3 oracle=TlpHavingOracle query_seed=310306 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t2.col_t2_1_int32_array
    FROM t2
    WHERE NULL
    GROUP BY t2.col_t2_1_int32_array

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t2.col_t2_1_int32_array
    FROM t2
    WHERE NULL
    GROUP BY t2.col_t2_1_int32_array
    HAVING (true)
    UNION ALL
    SELECT t2.col_t2_1_int32_array
    FROM t2
    WHERE NULL
    GROUP BY t2.col_t2_1_int32_array
    HAVING NOT (true)
    UNION ALL
    SELECT t2.col_t2_1_int32_array
    FROM t2
    WHERE NULL
    GROUP BY t2.col_t2_1_int32_array
    HAVING (true) IS NULL

    === round=1 query=4 oracle=TlpHavingOracle query_seed=310307 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t0.col_t0_3_date32
    FROM t0
    WHERE (to_char(current_time(), ':#y`') IS NOT DISTINCT FROM CAST('2052-05-02' AS DATE))
    GROUP BY t0.col_t0_3_date32

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t0.col_t0_3_date32
    FROM t0
    WHERE (to_char(current_time(), ':#y`') IS NOT DISTINCT FROM CAST('2052-05-02' AS DATE))
    GROUP BY t0.col_t0_3_date32
    HAVING (false)
    UNION ALL
    SELECT t0.col_t0_3_date32
    FROM t0
    WHERE (to_char(current_time(), ':#y`') IS NOT DISTINCT FROM CAST('2052-05-02' AS DATE))
    GROUP BY t0.col_t0_3_date32
    HAVING NOT (false)
    UNION ALL
    SELECT t0.col_t0_3_date32
    FROM t0
    WHERE (to_char(current_time(), ':#y`') IS NOT DISTINCT FROM CAST('2052-05-02' AS DATE))
    GROUP BY t0.col_t0_3_date32
    HAVING (false) IS NULL

    === round=1 query=5 oracle=TlpHavingOracle query_seed=310308 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t0.col_t0_2_uint32, t0.col_t0_3_date32, t0.col_t0_1_float64
    FROM t0
    WHERE (to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17T08:39:22.305135405-09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))
    GROUP BY t0.col_t0_2_uint32, t0.col_t0_3_date32, t0.col_t0_1_float64

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t0.col_t0_2_uint32, t0.col_t0_3_date32, t0.col_t0_1_float64
    FROM t0
    WHERE (to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17T08:39:22.305135405-09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))
    GROUP BY t0.col_t0_2_uint32, t0.col_t0_3_date32, t0.col_t0_1_float64
    HAVING ((to_char(INTERVAL '-7 MONS 29 DAYS -0.000000001 SECS', '%X `B') !~* '0SsYa@-p]yc`qTL8PvF #c;Tei9))DXs:^wgv['))
    UNION ALL
    SELECT t0.col_t0_2_uint32, t0.col_t0_3_date32, t0.col_t0_1_float64
    FROM t0
    WHERE (to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17T08:39:22.305135405-09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))
    GROUP BY t0.col_t0_2_uint32, t0.col_t0_3_date32, t0.col_t0_1_float64
    HAVING NOT ((to_char(INTERVAL '-7 MONS 29 DAYS -0.000000001 SECS', '%X `B') !~* '0SsYa@-p]yc`qTL8PvF #c;Tei9))DXs:^wgv['))
    UNION ALL
    SELECT t0.col_t0_2_uint32, t0.col_t0_3_date32, t0.col_t0_1_float64
    FROM t0
    WHERE (to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17T08:39:22.305135405-09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))
    GROUP BY t0.col_t0_2_uint32, t0.col_t0_3_date32, t0.col_t0_1_float64
    HAVING ((to_char(INTERVAL '-7 MONS 29 DAYS -0.000000001 SECS', '%X `B') !~* '0SsYa@-p]yc`qTL8PvF #c;Tei9))DXs:^wgv[')) IS NULL

    === round=2 query=1 oracle=TlpHavingOracle query_seed=311304 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t0.col_t0_2_date32, t0.col_t0_5_timestamp, t0.col_t0_4_string
    FROM t0
    WHERE false
    GROUP BY t0.col_t0_2_date32, t0.col_t0_5_timestamp, t0.col_t0_4_string

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t0.col_t0_2_date32, t0.col_t0_5_timestamp, t0.col_t0_4_string
    FROM t0
    WHERE false
    GROUP BY t0.col_t0_2_date32, t0.col_t0_5_timestamp, t0.col_t0_4_string
    HAVING (false)
    UNION ALL
    SELECT t0.col_t0_2_date32, t0.col_t0_5_timestamp, t0.col_t0_4_string
    FROM t0
    WHERE false
    GROUP BY t0.col_t0_2_date32, t0.col_t0_5_timestamp, t0.col_t0_4_string
    HAVING NOT (false)
    UNION ALL
    SELECT t0.col_t0_2_date32, t0.col_t0_5_timestamp, t0.col_t0_4_string
    FROM t0
    WHERE false
    GROUP BY t0.col_t0_2_date32, t0.col_t0_5_timestamp, t0.col_t0_4_string
    HAVING (false) IS NULL

    === round=2 query=2 oracle=TlpHavingOracle query_seed=311305 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t0.col_t0_3_float64, t0.col_t0_1_float32
    FROM t0
    WHERE (to_char(t0.col_t0_5_timestamp, 'r@$-i2|ckDaNwE:cNhmtN_0$e3gjJYAb|$~9F') ~ to_char(CAST('06:17:58.412287082' AS TIME), to_char(t0.col_t0_5_timestamp, 'l*$;sI8,7DmIx')))
    GROUP BY t0.col_t0_3_float64, t0.col_t0_1_float32

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t0.col_t0_3_float64, t0.col_t0_1_float32
    FROM t0
    WHERE (to_char(t0.col_t0_5_timestamp, 'r@$-i2|ckDaNwE:cNhmtN_0$e3gjJYAb|$~9F') ~ to_char(CAST('06:17:58.412287082' AS TIME), to_char(t0.col_t0_5_timestamp, 'l*$;sI8,7DmIx')))
    GROUP BY t0.col_t0_3_float64, t0.col_t0_1_float32
    HAVING (((-25722.00000000 - (29474.000000000000000000000000000000 / 50715.00000000)) IS DISTINCT FROM -0.0000052674000000000000000000000000000000))
    UNION ALL
    SELECT t0.col_t0_3_float64, t0.col_t0_1_float32
    FROM t0
    WHERE (to_char(t0.col_t0_5_timestamp, 'r@$-i2|ckDaNwE:cNhmtN_0$e3gjJYAb|$~9F') ~ to_char(CAST('06:17:58.412287082' AS TIME), to_char(t0.col_t0_5_timestamp, 'l*$;sI8,7DmIx')))
    GROUP BY t0.col_t0_3_float64, t0.col_t0_1_float32
    HAVING NOT (((-25722.00000000 - (29474.000000000000000000000000000000 / 50715.00000000)) IS DISTINCT FROM -0.0000052674000000000000000000000000000000))
    UNION ALL
    SELECT t0.col_t0_3_float64, t0.col_t0_1_float32
    FROM t0
    WHERE (to_char(t0.col_t0_5_timestamp, 'r@$-i2|ckDaNwE:cNhmtN_0$e3gjJYAb|$~9F') ~ to_char(CAST('06:17:58.412287082' AS TIME), to_char(t0.col_t0_5_timestamp, 'l*$;sI8,7DmIx')))
    GROUP BY t0.col_t0_3_float64, t0.col_t0_1_float32
    HAVING (((-25722.00000000 - (29474.000000000000000000000000000000 / 50715.00000000)) IS DISTINCT FROM -0.0000052674000000000000000000000000000000)) IS NULL

    === round=2 query=3 oracle=TlpHavingOracle query_seed=311306 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t2.col_t2_1_uint16
    FROM t2
    WHERE true
    GROUP BY t2.col_t2_1_uint16

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t2.col_t2_1_uint16
    FROM t2
    WHERE true
    GROUP BY t2.col_t2_1_uint16
    HAVING (false)
    UNION ALL
    SELECT t2.col_t2_1_uint16
    FROM t2
    WHERE true
    GROUP BY t2.col_t2_1_uint16
    HAVING NOT (false)
    UNION ALL
    SELECT t2.col_t2_1_uint16
    FROM t2
    WHERE true
    GROUP BY t2.col_t2_1_uint16
    HAVING (false) IS NULL

    === round=2 query=4 oracle=TlpHavingOracle query_seed=311307 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t1.col_t1_4_decimal128
    FROM t1
    GROUP BY t1.col_t1_4_decimal128

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t1.col_t1_4_decimal128
    FROM t1
    GROUP BY t1.col_t1_4_decimal128
    HAVING (true)
    UNION ALL
    SELECT t1.col_t1_4_decimal128
    FROM t1
    GROUP BY t1.col_t1_4_decimal128
    HAVING NOT (true)
    UNION ALL
    SELECT t1.col_t1_4_decimal128
    FROM t1
    GROUP BY t1.col_t1_4_decimal128
    HAVING (true) IS NULL

    === round=2 query=5 oracle=TlpHavingOracle query_seed=311308 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t2.col_t2_1_uint16
    FROM t2
    GROUP BY t2.col_t2_1_uint16

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t2.col_t2_1_uint16
    FROM t2
    GROUP BY t2.col_t2_1_uint16
    HAVING (((-54.097916 + 82.92108) < to_timestamp_seconds(-33, to_char(CAST('2012-07-21T16:46:18.355033437-09:00' AS TIMESTAMP), 'clQaw=v(!#dUGtlPSU4GzDeJ{KF$%HvHn~'))))
    UNION ALL
    SELECT t2.col_t2_1_uint16
    FROM t2
    GROUP BY t2.col_t2_1_uint16
    HAVING NOT (((-54.097916 + 82.92108) < to_timestamp_seconds(-33, to_char(CAST('2012-07-21T16:46:18.355033437-09:00' AS TIMESTAMP), 'clQaw=v(!#dUGtlPSU4GzDeJ{KF$%HvHn~'))))
    UNION ALL
    SELECT t2.col_t2_1_uint16
    FROM t2
    GROUP BY t2.col_t2_1_uint16
    HAVING (((-54.097916 + 82.92108) < to_timestamp_seconds(-33, to_char(CAST('2012-07-21T16:46:18.355033437-09:00' AS TIMESTAMP), 'clQaw=v(!#dUGtlPSU4GzDeJ{KF$%HvHn~')))) IS NULL
    "#);
    insta::assert_snapshot!(run_output.stats_summary, @"
    ============================================================
    🎯 DataFusion Fuzzer - Final Statistics
    ============================================================
    📊 Execution Summary:
      • Rounds Completed: 2
      • Queries Executed: 20
      • Query Success Rate: 75.00%
    ");

    fs::remove_dir_all(&log_dir)?;

    Ok(())
}

struct RunOutput {
    query_log: String,
    stats_summary: String,
}

fn run_fuzzer_once(config_path: &Path) -> Result<RunOutput, Box<dyn Error>> {
    let config = RunnerConfig::from_file(config_path)?;
    let log_dir = config
        .log_path
        .ok_or("expected test config to include a log_path")?;

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion-fuzzer"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(["--config"])
        .arg(config_path)
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "fuzzer run failed with status {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
        .into());
    }

    let query_log_path = log_dir.join("queries.log");
    if !query_log_path.exists() {
        return Err(format!(
            "expected query log at '{}', but it was not created\nstdout:\n{}\nstderr:\n{}",
            query_log_path.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
        .into());
    }

    let stdout = String::from_utf8(output.stdout)?;

    Ok(RunOutput {
        query_log: fs::read_to_string(query_log_path)?,
        stats_summary: extract_stats_summary(&stdout)?,
    })
}

fn generate_default_config_with_oracles(
    log_dir: &Path,
    oracles: &[ConfiguredOracle],
) -> Result<PathBuf, Box<dyn Error>> {
    let config_path = log_dir.join("integration.toml");
    let config = RunnerConfig {
        rounds: 2,
        queries_per_round: 5,
        log_path: Some(log_dir.to_path_buf()),
        enable_tui: false,
        oracles: oracles.to_vec(),
        ..RunnerConfig::default()
    };

    // Generate an integration-test config from the default config shape while
    // letting each test pin its oracle set and deterministic runtime knobs.
    fs::write(&config_path, toml::to_string(&config)?)?;

    Ok(config_path)
}

fn extract_stats_summary(stdout: &str) -> Result<String, Box<dyn Error>> {
    let lines: Vec<&str> = stdout.lines().collect();
    let start = lines
        .iter()
        .position(|line| *line == "============================================================")
        .ok_or("failed to find statistics header in stdout")?;

    let end = start + 6;
    if end >= lines.len() {
        return Err("stdout did not contain the expected statistics summary lines".into());
    }

    Ok(lines[start..=end].join("\n"))
}

fn make_temp_log_dir(label: &str) -> Result<PathBuf, Box<dyn Error>> {
    let unique_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = std::env::temp_dir().join(format!(
        "datafusion-fuzzer-e2e-{label}-{}-{unique_id}",
        std::process::id()
    ));
    fs::create_dir_all(&path)?;
    Ok(path)
}
