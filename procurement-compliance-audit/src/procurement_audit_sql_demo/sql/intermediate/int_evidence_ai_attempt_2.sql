create or replace table int_evidence_ai_attempt_2 as
-- Reuse the first attempt's exact BLOB for the contract-reinforced retry.
with retry_rows as materialized (
  select
    project_id,
    file_id,
    role,
    prompt_text,
    image_bytes
  from int_evidence_ai_retry_inputs
)

select
  project_id,
  file_id,
  role,
  ai_prompt(
    cast(prompt_text as varchar),
    cast(image_bytes as blob),
    struct_pack(
      provider := __AI_PROVIDER_SQL__,
      model := __AI_MODEL_SQL__,
      base_url := __AI_BASE_URL_SQL__,
      timeout := __AI_TIMEOUT_SQL__,
      concurrency := __AI_CONCURRENCY_SQL__,
      max_api_concurrency := __AI_CONCURRENCY_SQL__,
      temperature := __AI_TEMPERATURE_SQL__,
      max_tokens := __AI_MAX_TOKENS_SQL__,
      on_error := 'raise',
      system_message := __AI_SYSTEM_MESSAGE_SQL__
    )
  ) as raw_response
from retry_rows
where image_bytes is not null;
