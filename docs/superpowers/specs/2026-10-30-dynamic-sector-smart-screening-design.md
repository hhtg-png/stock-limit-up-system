# Natural-language dynamic sector screening

## User intent

The user clarified that the existing field picker is too rigid: they expect to describe stock-selection ideas in natural language. They previously explicitly opted out of brainstorming and requested direct implementation. An optional real example has been requested asynchronously; implementation continues on the agreed workflow without another approval gate.

## Behavior

- Make intelligent screening the primary create experience, with manual conditions still available. A user describes an idea, sees the interpreted conditions, assumptions and real market preview, and then saves the definition as an automatically updated dynamic sector.
- Reuse the configured DeepSeek provider for interpretation only. Actual matches come from deterministic execution against real quotes and verified database facts. No model-supplied stock list, arbitrary Python or SQL is executable.
- Newly interpreted groups default to dynamic membership; explicitly requested permanent tracking uses retain. Refinements preserve the current membership mode unless the user changes it. Old manual defaults are unchanged, and both modes retain frozen historical batches.
- Support the existing fields, nested AND/OR combinations, numeric/time comparisons, optional ranking and top-N, and recent completed-trading-day limit-up/touched-limit-up counts and maximum board height. Historical windows are 1–60 completed trading sessions and exclude the current intraday session, with that meaning explicitly displayed.
- Preserve exact user descriptions and interpretation summaries with each version. Refinement can use the current definition as context. Unknown, unsupported or ambiguous conditions return explicit questions or unsupported clauses; no omitted clause or default replacement should be presented as successfully interpreted.
- Existing groups, flat conditions, frozen batches and pre-creation historical indices remain compatible. Changing executable rules or ranking starts a new group version under the existing archival behavior.
- First release does not claim general financial statement, news sentiment, MA, MACD, historical volume or arbitrary technical-pattern screening. Such requests must identify the missing capability instead of silently approximating it with present-day fields.

## Contracts

- Rule leaves retain `{field, op, value, value2?}`. Historical leaves additionally require `days` (integer 1–60). Compound nodes are exactly `{all: [nodes...]}` or `{any: [nodes...]}`. Maximum 30 leaves, depth 4. Top-level `match_mode` stays compatible.
- Historical fields: `recent_limit_up_count`, `recent_touched_limit_up_count`, `recent_max_continuous_days`. Each is numeric; facts are calculated for the requested number of completed exchange sessions. Missing or unverified sessions yield unknown, never an invented zero.
- Optional group `strategy` contains `prompt`, `summary`, `assumptions`, and optional `ranking: {field, direction: asc|desc, limit?: 1..500, days?: 1..60}`. Ranking fields must be validated sortable numeric/time fields; historical ranking requires explicit `days`, other fields forbid it. Preserve this JSON in group/version/batch definitions; add a nullable database column through existing migration helpers.
- `POST /api/v1/dynamic-groups/interpret`: request `{prompt: string (1..2000), current_definition?: DynamicGroupDefinition}`; response `{status: ready|needs_clarification|unsupported|error, message: string, definition: DynamicGroupDefinition|null, assumptions: string[], questions: string[], unsupported: string[], model: string|null}`. Only `ready` has an executable, locally validated definition.
- Interpretation never writes groups or selection data. Create/update/preview keep the existing API and gain optional strategy. UI must not save stale or unparsed smart input.

## Data and correctness

- Batch historical queries by shared session window and share results across groups; no full-market per-stock network history scan in a quote tick.
- Validate precise session completeness using the existing market-review daily metric/fact contract. Historical source repair remains the responsibility of existing archive jobs.
- Preserve three-valued unknown behavior. Ranking must expose partial coverage and avoid treating unknown candidates as definite nonmatches or falsely evicting them.
- Fix the confirmed numeric threshold bug: open 9.45 / previous close 9 must satisfy 5% equality and >=5%, and fail <5%. Do not blur genuinely different financial values.
- Official DeepSeek request references: https://api-docs.deepseek.com/guides/json_mode/ and https://api-docs.deepseek.com/api/create-chat-completion/ . Use bounded JSON output, an explicit JSON instruction, local validation and current settings per request. Public errors must never include credentials or raw provider response bodies.

## Mobile and validation

- Keep the existing responsive drawer; smart input, understanding cards, questions and preview fit a 390px viewport. Preserve drafts on failed interpretation and ignore out-of-order responses.
- Verify supported complex descriptions, unsupported technical criteria, ambiguity, malformed model output, provider errors, nested unknown logic, historical missing dates, ranking ties/unknowns, old flat APIs and real source execution.
- Deployment requires fresh full backend tests and frontend tests/build, GitHub main push, managed deployment, public endpoints/assets and real configured-provider interpretation smoke without creating production fixtures.
