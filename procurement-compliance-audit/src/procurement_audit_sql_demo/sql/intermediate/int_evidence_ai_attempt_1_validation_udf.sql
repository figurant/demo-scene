create or replace table int_evidence_ai_attempt_1_validation_udf as
-- An empty canonical response marks exactly the rows needing one semantic retry.
select
  project_id,
  file_id,
  role,
  image_bytes,
  raw_response,
  try_validate_audit_fact_for_role_json(
    cast(raw_response as varchar),
    cast(role as varchar)
  ) as canonical_response
from int_evidence_ai_attempt_1;
