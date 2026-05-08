select 'mart_customer' as model_name
from (select 1)
where not exists (
    select 1
    from {{ ref('mart_customer') }}
    where position_date = current_date('Asia/Jakarta')
)