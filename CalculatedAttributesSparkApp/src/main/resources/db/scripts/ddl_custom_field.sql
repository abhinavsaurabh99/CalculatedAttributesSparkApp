CREATE TABLE prime.custom_field (
    id BIGINT NOT NULL,
    field_name VARCHAR(150) NOT NULL,
    field_display_name VARCHAR(150) DEFAULT NULL,
    data_type prime.data_type NOT NULL DEFAULT 'CHAR',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT pk_custom_field PRIMARY KEY (id),
    CONSTRAINT uq_custom_field_field_name UNIQUE (field_name),
    CONSTRAINT uq_custom_field_field_display_name UNIQUE (field_display_name)
);

CREATE UNIQUE INDEX idx_custom_field_field_name
    ON prime.custom_field (field_name);

CREATE UNIQUE INDEX idx_custom_field_field_display_name
    ON prime.custom_field (field_display_name);


CREATE TABLE prime.item_custom_field_value (
    id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    custom_field_id BIGINT NOT NULL,
    custom_field_value VARCHAR(2000) DEFAULT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_item_custom_field_value PRIMARY KEY (id),
    CONSTRAINT uq_item_custom_field_value_item_id_custom_field_id UNIQUE (item_id, custom_field_id),
    CONSTRAINT fk_item_custom_field_value_item FOREIGN KEY (item_id)
        REFERENCES prime.item (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_item_custom_field_value_custom_field FOREIGN KEY (custom_field_id)
        REFERENCES prime.custom_field (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE UNIQUE INDEX idx_item_custom_field_value_item_id_custom_field_id
    ON prime.item_custom_field_value (item_id, custom_field_id);

CREATE INDEX idx_item_custom_field_value_item_id
    ON prime.item_custom_field_value (item_id);

CREATE INDEX idx_item_custom_field_value_custom_field_id
    ON prime.item_custom_field_value (custom_field_id);

CREATE INDEX idx_item_custom_field_value_updated_at
    ON prime.item_custom_field_value (updated_at);
