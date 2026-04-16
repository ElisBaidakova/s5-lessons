INSERT INTO public_test.testing_result (test_date_time, test_name, test_result)
VALUES (
    current_timestamp,
    'test_01',
    (SELECT EXISTS (
        SELECT 1
        FROM public_test.dm_settlement_report_actual a
        FULL JOIN public_test.dm_settlement_report_expected e 
          ON a.id = e.id
        WHERE a.id IS NULL OR e.id IS NULL
    ))
)
RETURNING test_date_time, test_name, test_result;