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
    SELECT (31905.000000000000 + 42185.000000000000000000000), NULL, ((26.72593219656791 * (-31.000798999170783 + -47.79515993907295)) % 8.375943795966606)
    FROM t0, t2, t1
    WHERE true

    === round=1 query=2 oracle=NoCrashOracle query_seed=310305 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT ((current_time() + current_time()) < -55), (65.706448177695 * ((NULL + -0.06514454367550115) + -85.93427693961893))
    FROM t0, t2, t1
    WHERE true

    === round=1 query=3 oracle=NoCrashOracle query_seed=310306 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT NULL
    FROM t2
    WHERE false

    === round=1 query=4 oracle=NoCrashOracle query_seed=310307 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT (NULL + ((t0.col_t0_2_float32 % t0.col_t0_2_float32) / t0.col_t0_2_float32)), -28
    FROM t0
    WHERE false

    === round=1 query=5 oracle=NoCrashOracle query_seed=310308 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT (to_char((CAST('05:08:41.131473326' AS TIME) + NULL), '7{"kwxZt~K:1-?59') ~~* ';g o|<e1t5PeUZgc1*<DsT[MNI;W=ly5GZ9::]%%=y'), (((72 + NULL) % (110 % 131)) / 68), (49.672875494573475 / (-53.663513437485165 + NULL))
    FROM t2, t0, t1
    WHERE (t2.col_t2_1_string ~* 'xTd4vs>d>OZr?2F')

    === round=2 query=1 oracle=NoCrashOracle query_seed=311304 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT (t1.col_t1_3_decimal128 * ((97585.0000000000 * -29056.0000000000000000000000) - 42588.00000000000000000000000000000)), (((192 - 122) - (NULL % 70)) - (136 * (179 % 20))), (((53 / 91) - (59 - 22)) % 80)
    FROM t1, t0
    WHERE (CAST('17:49:46.025384417' AS TIME) = ((-36.406612 / -26.527832) / 96.30975))

    === round=2 query=2 oracle=NoCrashOracle query_seed=311305 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT t0.col_t0_2_time64_nanosecond, 146
    FROM t2, t0
    RIGHT SEMI JOIN t1 ON ('/*L/' !~ to_char((t0.col_t0_4_interval_month_day_nano - INTERVAL '4 MONS 29 DAYS -0.138771504 SECS'), '3v3F=3`'))
    WHERE (to_char(t0.col_t0_4_interval_month_day_nano, NULL) !~ to_char(NULL, 'yUANHM J(=="$M@XZkO9lj$9qO>]!2v7EoH8@mjXdBV'))

    === round=2 query=3 oracle=NoCrashOracle query_seed=311306 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT (118 % 25), -56, (((54 + -74) / (-45 * -27)) + 23)
    FROM t2
    WHERE (-30 > (175 * NULL))

    === round=2 query=4 oracle=NoCrashOracle query_seed=311307 ===
    --- statement=1 context=Random Query No-Crash Test ---
    SELECT to_char(t0.col_t0_5_timestamp, '64kk&7.OusV5v6J"/nN,9z"c >$+qIc}I'), (-43 * ((98 % 80) / (-50 % 94))), CAST('2000-12-22' AS DATE)
    FROM t0, t1
    WHERE (37.643173 < (NULL + (NULL % 47)))

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
    WHERE (((NULL > NULL) OR (CAST('13:24:10.016648859' AS TIME) > (-44 + -91))))
    UNION ALL
    SELECT *
    FROM t0
    WHERE NOT (((NULL > NULL) OR (CAST('13:24:10.016648859' AS TIME) > (-44 + -91))))
    UNION ALL
    SELECT *
    FROM t0
    WHERE (((NULL > NULL) OR (CAST('13:24:10.016648859' AS TIME) > (-44 + -91)))) IS NULL

    === round=1 query=2 oracle=TlpWhereOracle query_seed=310305 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t1

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t1
    WHERE (false)
    UNION ALL
    SELECT *
    FROM t1
    WHERE NOT (false)
    UNION ALL
    SELECT *
    FROM t1
    WHERE (false) IS NULL

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

    === round=1 query=5 oracle=TlpWhereOracle query_seed=310308 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t0

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t0
    WHERE ((to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17 08:39:22.305135405 -09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"'))))
    UNION ALL
    SELECT *
    FROM t0
    WHERE NOT ((to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17 08:39:22.305135405 -09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"'))))
    UNION ALL
    SELECT *
    FROM t0
    WHERE ((to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17 08:39:22.305135405 -09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))) IS NULL

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
    WHERE (true)
    UNION ALL
    SELECT *
    FROM t0
    WHERE NOT (true)
    UNION ALL
    SELECT *
    FROM t0
    WHERE (true) IS NULL

    === round=2 query=3 oracle=TlpWhereOracle query_seed=311306 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t2

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t2
    WHERE (false)
    UNION ALL
    SELECT *
    FROM t2
    WHERE NOT (false)
    UNION ALL
    SELECT *
    FROM t2
    WHERE (false) IS NULL

    === round=2 query=4 oracle=TlpWhereOracle query_seed=311307 ===
    --- statement=1 context=TLP-WHERE all ---
    SELECT *
    FROM t1

    --- statement=2 context=TLP-WHERE p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT *
    FROM t1
    WHERE ((96 >= ((-7.129738 - 23.446228) % (47.11673 / 88.10098))))
    UNION ALL
    SELECT *
    FROM t1
    WHERE NOT ((96 >= ((-7.129738 - 23.446228) % (47.11673 / 88.10098))))
    UNION ALL
    SELECT *
    FROM t1
    WHERE ((96 >= ((-7.129738 - 23.446228) % (47.11673 / 88.10098)))) IS NULL

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
    insta::assert_snapshot!(run_output.stats_summary, @r#"
    ============================================================
    🎯 DataFusion Fuzzer - Final Statistics
    ============================================================
    📊 Execution Summary:
      • Rounds Completed: 2
      • Queries Executed: 18
      • Query Success Rate: 88.89%
    "#);

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
    SELECT t0.col_t0_3_date32, t0.col_t0_1_decimal128
    FROM t0
    GROUP BY t0.col_t0_3_date32, t0.col_t0_1_decimal128

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t0.col_t0_3_date32, t0.col_t0_1_decimal128
    FROM t0
    GROUP BY t0.col_t0_3_date32, t0.col_t0_1_decimal128
    HAVING ((INTERVAL '-7 MONS 16 DAYS -0.000000001 SECS' = 1193.6000000000000000000000000000000))
    UNION ALL
    SELECT t0.col_t0_3_date32, t0.col_t0_1_decimal128
    FROM t0
    GROUP BY t0.col_t0_3_date32, t0.col_t0_1_decimal128
    HAVING NOT ((INTERVAL '-7 MONS 16 DAYS -0.000000001 SECS' = 1193.6000000000000000000000000000000))
    UNION ALL
    SELECT t0.col_t0_3_date32, t0.col_t0_1_decimal128
    FROM t0
    GROUP BY t0.col_t0_3_date32, t0.col_t0_1_decimal128
    HAVING ((INTERVAL '-7 MONS 16 DAYS -0.000000001 SECS' = 1193.6000000000000000000000000000000)) IS NULL

    === round=1 query=2 oracle=TlpHavingOracle query_seed=310305 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t1.col_t1_1_int64, t1.col_t1_3_date32, t1.col_t1_2_int64
    FROM t1
    GROUP BY t1.col_t1_1_int64, t1.col_t1_3_date32, t1.col_t1_2_int64

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t1.col_t1_1_int64, t1.col_t1_3_date32, t1.col_t1_2_int64
    FROM t1
    GROUP BY t1.col_t1_1_int64, t1.col_t1_3_date32, t1.col_t1_2_int64
    HAVING (NULL)
    UNION ALL
    SELECT t1.col_t1_1_int64, t1.col_t1_3_date32, t1.col_t1_2_int64
    FROM t1
    GROUP BY t1.col_t1_1_int64, t1.col_t1_3_date32, t1.col_t1_2_int64
    HAVING NOT (NULL)
    UNION ALL
    SELECT t1.col_t1_1_int64, t1.col_t1_3_date32, t1.col_t1_2_int64
    FROM t1
    GROUP BY t1.col_t1_1_int64, t1.col_t1_3_date32, t1.col_t1_2_int64
    HAVING (NULL) IS NULL

    === round=1 query=3 oracle=TlpHavingOracle query_seed=310306 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t2.col_t2_1_string
    FROM t2
    WHERE NULL
    GROUP BY t2.col_t2_1_string

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t2.col_t2_1_string
    FROM t2
    WHERE NULL
    GROUP BY t2.col_t2_1_string
    HAVING (false)
    UNION ALL
    SELECT t2.col_t2_1_string
    FROM t2
    WHERE NULL
    GROUP BY t2.col_t2_1_string
    HAVING NOT (false)
    UNION ALL
    SELECT t2.col_t2_1_string
    FROM t2
    WHERE NULL
    GROUP BY t2.col_t2_1_string
    HAVING (false) IS NULL

    === round=1 query=5 oracle=TlpHavingOracle query_seed=310308 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t0.col_t0_2_float32, t0.col_t0_1_decimal128, t0.col_t0_3_date32
    FROM t0
    WHERE (to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17 08:39:22.305135405 -09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))
    GROUP BY t0.col_t0_2_float32, t0.col_t0_1_decimal128, t0.col_t0_3_date32

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t0.col_t0_2_float32, t0.col_t0_1_decimal128, t0.col_t0_3_date32
    FROM t0
    WHERE (to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17 08:39:22.305135405 -09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))
    GROUP BY t0.col_t0_2_float32, t0.col_t0_1_decimal128, t0.col_t0_3_date32
    HAVING (false)
    UNION ALL
    SELECT t0.col_t0_2_float32, t0.col_t0_1_decimal128, t0.col_t0_3_date32
    FROM t0
    WHERE (to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17 08:39:22.305135405 -09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))
    GROUP BY t0.col_t0_2_float32, t0.col_t0_1_decimal128, t0.col_t0_3_date32
    HAVING NOT (false)
    UNION ALL
    SELECT t0.col_t0_2_float32, t0.col_t0_1_decimal128, t0.col_t0_3_date32
    FROM t0
    WHERE (to_char(CAST('2052-04-28' AS DATE), '=B  2v') !~* to_char(INTERVAL '1 MONS -11 DAYS -0.658344865 SECS', to_char(CAST('2056-06-17 08:39:22.305135405 -09:00' AS TIMESTAMP), '9L4l6.-bG6dPLWk-7 ~9azH0^V;7q0S#|%@?MyX"')))
    GROUP BY t0.col_t0_2_float32, t0.col_t0_1_decimal128, t0.col_t0_3_date32
    HAVING (false) IS NULL

    === round=2 query=1 oracle=TlpHavingOracle query_seed=311304 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t0.col_t0_4_interval_month_day_nano, t0.col_t0_1_float64, t0.col_t0_2_time64_nanosecond
    FROM t0
    WHERE false
    GROUP BY t0.col_t0_4_interval_month_day_nano, t0.col_t0_1_float64, t0.col_t0_2_time64_nanosecond

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t0.col_t0_4_interval_month_day_nano, t0.col_t0_1_float64, t0.col_t0_2_time64_nanosecond
    FROM t0
    WHERE false
    GROUP BY t0.col_t0_4_interval_month_day_nano, t0.col_t0_1_float64, t0.col_t0_2_time64_nanosecond
    HAVING (false)
    UNION ALL
    SELECT t0.col_t0_4_interval_month_day_nano, t0.col_t0_1_float64, t0.col_t0_2_time64_nanosecond
    FROM t0
    WHERE false
    GROUP BY t0.col_t0_4_interval_month_day_nano, t0.col_t0_1_float64, t0.col_t0_2_time64_nanosecond
    HAVING NOT (false)
    UNION ALL
    SELECT t0.col_t0_4_interval_month_day_nano, t0.col_t0_1_float64, t0.col_t0_2_time64_nanosecond
    FROM t0
    WHERE false
    GROUP BY t0.col_t0_4_interval_month_day_nano, t0.col_t0_1_float64, t0.col_t0_2_time64_nanosecond
    HAVING (false) IS NULL

    === round=2 query=2 oracle=TlpHavingOracle query_seed=311305 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t0.col_t0_4_interval_month_day_nano, t0.col_t0_3_boolean
    FROM t0
    WHERE true
    GROUP BY t0.col_t0_4_interval_month_day_nano, t0.col_t0_3_boolean

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t0.col_t0_4_interval_month_day_nano, t0.col_t0_3_boolean
    FROM t0
    WHERE true
    GROUP BY t0.col_t0_4_interval_month_day_nano, t0.col_t0_3_boolean
    HAVING (t0.col_t0_3_boolean)
    UNION ALL
    SELECT t0.col_t0_4_interval_month_day_nano, t0.col_t0_3_boolean
    FROM t0
    WHERE true
    GROUP BY t0.col_t0_4_interval_month_day_nano, t0.col_t0_3_boolean
    HAVING NOT (t0.col_t0_3_boolean)
    UNION ALL
    SELECT t0.col_t0_4_interval_month_day_nano, t0.col_t0_3_boolean
    FROM t0
    WHERE true
    GROUP BY t0.col_t0_4_interval_month_day_nano, t0.col_t0_3_boolean
    HAVING (t0.col_t0_3_boolean) IS NULL

    === round=2 query=3 oracle=TlpHavingOracle query_seed=311306 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t2.col_t2_1_float32
    FROM t2
    WHERE false
    GROUP BY t2.col_t2_1_float32

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t2.col_t2_1_float32
    FROM t2
    WHERE false
    GROUP BY t2.col_t2_1_float32
    HAVING (false)
    UNION ALL
    SELECT t2.col_t2_1_float32
    FROM t2
    WHERE false
    GROUP BY t2.col_t2_1_float32
    HAVING NOT (false)
    UNION ALL
    SELECT t2.col_t2_1_float32
    FROM t2
    WHERE false
    GROUP BY t2.col_t2_1_float32
    HAVING (false) IS NULL

    === round=2 query=4 oracle=TlpHavingOracle query_seed=311307 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t1.col_t1_3_decimal128
    FROM t1
    GROUP BY t1.col_t1_3_decimal128

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t1.col_t1_3_decimal128
    FROM t1
    GROUP BY t1.col_t1_3_decimal128
    HAVING (true)
    UNION ALL
    SELECT t1.col_t1_3_decimal128
    FROM t1
    GROUP BY t1.col_t1_3_decimal128
    HAVING NOT (true)
    UNION ALL
    SELECT t1.col_t1_3_decimal128
    FROM t1
    GROUP BY t1.col_t1_3_decimal128
    HAVING (true) IS NULL

    === round=2 query=5 oracle=TlpHavingOracle query_seed=311308 ===
    --- statement=1 context=TLP-HAVING all groups ---
    SELECT t2.col_t2_1_float32
    FROM t2
    GROUP BY t2.col_t2_1_float32

    --- statement=2 context=TLP-HAVING p UNION ALL NOT p UNION ALL p IS NULL ---
    SELECT t2.col_t2_1_float32
    FROM t2
    GROUP BY t2.col_t2_1_float32
    HAVING (('ro' ~ to_char((INTERVAL '4 MONS -11 DAYS -0.961066195 SECS' + INTERVAL '10 MONS -17 DAYS -0.349920790 SECS'), to_char(NULL, '0,YK#Mw'))))
    UNION ALL
    SELECT t2.col_t2_1_float32
    FROM t2
    GROUP BY t2.col_t2_1_float32
    HAVING NOT (('ro' ~ to_char((INTERVAL '4 MONS -11 DAYS -0.961066195 SECS' + INTERVAL '10 MONS -17 DAYS -0.349920790 SECS'), to_char(NULL, '0,YK#Mw'))))
    UNION ALL
    SELECT t2.col_t2_1_float32
    FROM t2
    GROUP BY t2.col_t2_1_float32
    HAVING (('ro' ~ to_char((INTERVAL '4 MONS -11 DAYS -0.961066195 SECS' + INTERVAL '10 MONS -17 DAYS -0.349920790 SECS'), to_char(NULL, '0,YK#Mw')))) IS NULL
    "#);
    insta::assert_snapshot!(run_output.stats_summary, @r#"
    ============================================================
    🎯 DataFusion Fuzzer - Final Statistics
    ============================================================
    📊 Execution Summary:
      • Rounds Completed: 2
      • Queries Executed: 18
      • Query Success Rate: 77.78%
    "#);

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
