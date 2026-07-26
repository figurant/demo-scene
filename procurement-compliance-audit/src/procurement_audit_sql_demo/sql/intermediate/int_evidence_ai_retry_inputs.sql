create or replace view int_evidence_ai_retry_inputs as
-- Retry only first-attempt responses that violated the JSON or role contract.
select
  inputs.project_id,
  inputs.file_id,
  inputs.role,
  validation.image_bytes,
  inputs.prompt_text
    || '
上一次输出未通过合同校验。重新读取同一图片，只返回一个对象；必须包含 document_type、expert_id、supplier_name、recommended、participated、recused、evidence_quote、confidence 全部八个键，不要省略最后的 confidence。evidence_quote 必须逐字引用图片短句，confidence 必须根据证据清晰度实际填写，不得返回占位值。
' as prompt_text
from int_evidence_ai_inputs as inputs
inner join int_evidence_ai_attempt_1_validation_udf as validation
  using (project_id, file_id, role)
where validation.canonical_response = '';
