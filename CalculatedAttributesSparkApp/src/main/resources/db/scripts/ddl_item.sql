CREATE TYPE prime.item_status AS ENUM ('ACTIVE', 'INACTIVE', 'OBSOLETE');

CREATE TYPE prime.item_enriched_indicator AS ENUM ('ENHANCED', 'ENRICHED', 'NOT_ENHANCED');

CREATE TABLE prime.item (
    id BIGINT NOT NULL,
    part_number VARCHAR(100) NOT NULL,
    brand_name VARCHAR(20) NOT NULL,
    mfr_part_number VARCHAR(100) DEFAULT NULL,
    my_part_number VARCHAR(100) DEFAULT NULL,
    upc VARCHAR(100) DEFAULT NULL,
    country_of_origin VARCHAR(32) DEFAULT NULL,
    country_of_sale VARCHAR(32) DEFAULT NULL,
    gtin VARCHAR(20) DEFAULT NULL,
    short_description TEXT DEFAULT NULL,
    long_description TEXT DEFAULT NULL,
    feature_bullets TEXT DEFAULT NULL,
    invoice_description TEXT DEFAULT NULL,
    print_description TEXT DEFAULT NULL,
    pack_description TEXT DEFAULT NULL,
    qty_available INT DEFAULT NULL,
    min_order_qty INT DEFAULT NULL,
    package_length DECIMAL(10,2) DEFAULT NULL,
    package_length_uom VARCHAR(50) DEFAULT NULL,
    package_height DECIMAL(10,2) DEFAULT NULL,
    package_height_uom VARCHAR(50) DEFAULT NULL,
    package_weight DECIMAL(10,2) DEFAULT NULL,
    package_weight_uom VARCHAR(50) DEFAULT NULL,
    package_width DECIMAL(10,2) DEFAULT NULL,
    package_width_uom VARCHAR(50) DEFAULT NULL,
    volume DECIMAL(10,2) DEFAULT NULL,
    volume_uom VARCHAR(25) DEFAULT NULL,
    standard_approval TEXT DEFAULT NULL,
    status prime.item_status NOT NULL DEFAULT 'INACTIVE',
    warranty TEXT DEFAULT NULL,
    enriched_indicator prime.item_enriched_indicator NOT NULL DEFAULT 'NOT_ENHANCED',
    manufacturer_status prime.item_status NOT NULL DEFAULT 'INACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_item PRIMARY KEY (id),
    CONSTRAINT uq_item_part_number UNIQUE (part_number)
);

CREATE UNIQUE INDEX idx_item_part_number
    ON prime.item (part_number);

CREATE INDEX idx_item_brand_name
    ON prime.item (brand_name);

CREATE INDEX idx_item_my_part_number
    ON prime.item (my_part_number);

CREATE INDEX idx_item_updated_at
    ON prime.item (updated_at);
