CREATE TABLE prime.category (
    id BIGINT NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    category_code VARCHAR(25) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT pk_category PRIMARY KEY (id),
    CONSTRAINT uq_category_category_code UNIQUE (category_code),
    CONSTRAINT uq_category_category_name UNIQUE (category_name)
);

CREATE UNIQUE INDEX idx_category_category_code
    ON prime.category (category_code);

CREATE UNIQUE INDEX idx_category_category_name
    ON prime.category (category_name);



CREATE TABLE prime.item_category (
    id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    category_id BIGINT NOT NULL,
    default_category BOOLEAN NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT pk_item_category PRIMARY KEY (id),
    CONSTRAINT uq_item_category_item_id_category_id UNIQUE (item_id, category_id),
    CONSTRAINT fk_item_category_item FOREIGN KEY (item_id)
        REFERENCES prime.item (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_item_category_category FOREIGN KEY (category_id)
        REFERENCES prime.category (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE UNIQUE INDEX idx_item_category_item_id_category_id
    ON prime.item_category (item_id, category_id);

CREATE INDEX idx_item_category_item_id
    ON prime.item_category (item_id);

CREATE INDEX idx_item_category_category_id
    ON prime.item_category (category_id);

CREATE INDEX idx_item_category_updated_at
    ON prime.item_category (updated_at);

CREATE UNIQUE INDEX idx_item_category_item_id_default_category_true
    ON prime.item_category (item_id)
    WHERE default_category = TRUE;
