create or replace table int_evidence_ai as
-- Keep a valid first response, otherwise require the one retry response.
select
  first_attempt.project_id,
  first_attempt.file_id,
  case
    when validation.canonical_response <> ''
      then first_attempt.raw_response
    when retry.raw_response is not null
      then retry.raw_response
    else error('AI semantic retry did not return one response per failed row')
  end as raw_response
from int_evidence_ai_attempt_1 as first_attempt
inner join int_evidence_ai_attempt_1_validation_udf as validation
  using (project_id, file_id, role)
left join int_evidence_ai_attempt_2 as retry
  using (project_id, file_id, role);
