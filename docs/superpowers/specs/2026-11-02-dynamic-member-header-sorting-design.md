# Dynamic member header sorting

The user requests sorting for dynamic-screening columns directly in the table headers. Apply it to the dynamic-sector current members and archived batch members. Sorting changes the displayed order only; it does not edit the strategy, select different stocks, or rewrite historical batches.

## Behavior

- Each comparable displayed value has an accessible ascending/descending header control and a visible direction. Clearing the control restores the existing strategy ranking and stock-code fallback.
- Compound board/time columns expose each value independently. The operation column is not sortable. No global sorting toolbar is added.
- Server sorting runs before pagination for the entire searched member list. Numeric values sort numerically; zero and negative values remain valid; null or empty values come last in either direction; stock code breaks ties consistently.
- Current and historical views keep independent sorting state. Search, pagination, and polling retain the relevant order; changing the order starts on page one. Older requests cannot overwrite the new order.
- Mobile cards consume the same ordered server result. The existing responsive layout remains usable.

## API

Both `GET /{group_id}/current` and `GET /{group_id}/selections/{selection_id}` accept optional `sort_by` and `sort_order=asc|desc` (default `asc`). Omitting `sort_by` preserves existing ordering. Invalid keys or directions return 422.

Allowed fields: `stock_name`, `stock_code`, `current_price`, `change_pct`, `today_open_change_pct`, `yesterday_open_change_pct`, `turnover_rate`, `amount_yi`, `continuous_limit_up_days`, `yesterday_continuous_days`, `yesterday_first_limit_up_time`, `yesterday_final_seal_time`, `selected_at`.

Price follows the displayed `current_price ?? price`. Entry time follows `selected_at || first_selected_at`; batch creation time is a separate fact. Counts and summary statistics retain their existing scope.
