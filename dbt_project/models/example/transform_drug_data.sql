SELECT
    safetyreportid,
    receivedate,
    patient_age,
    drug_indication,
    outcome
FROM
    {{ ref('dbt_drug_data') }}
WHERE
    patient_age > 18
    AND drug_indication IS NOT NULL
