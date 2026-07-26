create or replace table int_claim_photo_ai as
-- Read each verified image as row data, then invoke multimodal AI in SQL.
with loaded as materialized (
  select
    claim_id,
    file_id,
    file_order,
    photo_sha256,
    photo_quality_json,
    prompt_text,
    verified_minio_object_bytes(
      cast(bucket as varchar),
      cast(object_key as varchar),
      cast(photo_sha256 as varchar)
    ) as image_bytes
  from int_claim_photo_ai_inputs
)

select
  claim_id,
  file_id,
  file_order,
  photo_sha256,
  photo_quality_json,
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
  ) as raw_damage_response
from loaded
where image_bytes is not null;
