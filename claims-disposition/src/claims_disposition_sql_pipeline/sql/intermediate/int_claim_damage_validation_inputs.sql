create or replace view int_claim_damage_validation_inputs as
-- Bind each model response to its trusted claim, file identity, and content hash.
select
  model_inputs.claim_id,
  model_inputs.file_id,
  model_inputs.file_order,
  model_inputs.photo_sha256,
  model_inputs.photo_quality_json,
  responses.raw_damage_response
from int_claim_photo_ai_inputs as model_inputs
left join int_claim_photo_ai as responses
  on model_inputs.claim_id = responses.claim_id
 and model_inputs.file_id = responses.file_id
 and model_inputs.photo_sha256 = responses.photo_sha256;
