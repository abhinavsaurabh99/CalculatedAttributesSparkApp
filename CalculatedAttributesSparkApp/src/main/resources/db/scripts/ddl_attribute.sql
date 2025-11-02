CREATE TYPE prime.data_type AS ENUM ('INT', 'DECIMAL', 'CHAR');

CREATE TABLE prime.attribute (
    id BIGINT NOT NULL,
    attribute_name VARCHAR(100) NOT NULL,
    attribute_code VARCHAR(100) NOT NULL,
    attribute_abbreviation VARCHAR(20),
    data_type prime.data_type NOT NULL DEFAULT 'CHAR',
    is_multivalue BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT pk_attribute PRIMARY KEY (id),
    CONSTRAINT uq_attribute_attribute_name UNIQUE (attribute_name),
    CONSTRAINT uq_attribute_attribute_code UNIQUE (attribute_code)
);

CREATE UNIQUE INDEX idx_attribute_attribute_code
    ON prime.attribute (attribute_code);

CREATE UNIQUE INDEX idx_attribute_attribute_name
    ON prime.attribute (attribute_name);


CREATE TABLE prime.item_attribute_value (
    id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    attribute_id BIGINT NOT NULL,
    attribute_value VARCHAR(1536),
    attribute_uom VARCHAR(50),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT pk_item_attribute_value PRIMARY KEY (id),
    CONSTRAINT uq_item_attribute_value_item_id_attribute_id UNIQUE (item_id, attribute_id),
    CONSTRAINT fk_item_attribute_value_item FOREIGN KEY (item_id)
        REFERENCES prime.item (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_item_attribute_value_attribute FOREIGN KEY (attribute_id)
        REFERENCES prime.attribute (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE UNIQUE INDEX idx_item_attribute_value_item_id_attribute_id
    ON prime.item_attribute_value (item_id, attribute_id);

CREATE INDEX idx_item_attribute_value_item_id
    ON prime.item_attribute_value (item_id);

CREATE INDEX idx_item_attribute_value_attribute_id
    ON prime.item_attribute_value (attribute_id);

CREATE INDEX idx_item_attribute_value_updated_at
    ON prime.item_attribute_value (updated_at);
