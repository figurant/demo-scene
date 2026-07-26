create or replace table int_evidence_ai_attempt_1 as
-- Load every qualified image and make the first multimodal SQL call.
with loaded as materialized (
  select
    project_id,
    file_id,
    role,
    prompt_text,
    minio_object_bytes(
      cast(bucket as varchar),
      cast(object_key as varchar)
    ) as image_bytes
  from int_evidence_ai_inputs
)

select
  project_id,
  file_id,
  role,
  image_bytes,
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
from loaded
where image_bytes is not null;
