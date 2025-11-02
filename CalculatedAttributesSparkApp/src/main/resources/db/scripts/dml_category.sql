INSERT INTO prime.category (
    id,
    category_name,
    category_code,
    created_at,
    updated_at
)
VALUES
(
    10001,
    'mesh',
    'm0121',
    NOW(),
    NOW()
),
(
    10002,
    'mobile',
    'm021',
    NOW(),
    NOW()
);


INSERT INTO prime.item_category (
    id,
    item_id,
    category_id,
    default_category,
    updated_at
)
VALUES
(
    10001,
    10001,
    10001,
    true,
    NOW()
),
(
    10002,
    10002,
    10002,
    true,
    NOW()
);
