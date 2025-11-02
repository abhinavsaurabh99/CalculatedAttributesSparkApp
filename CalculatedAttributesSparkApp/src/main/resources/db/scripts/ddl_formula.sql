CREATE TABLE prime.lu_field_name (
    id BIGINT NOT NULL,
    field_name VARCHAR(100) NOT NULL,
	CONSTRAINT pk_lu_field_name PRIMARY KEY (id),
    CONSTRAINT uq_lu_field_name_field_name UNIQUE (field_name)
);

CREATE UNIQUE INDEX idx_lu_field_name_field_name
    ON prime.lu_field_name (field_name);


CREATE TABLE prime.category_field (
    id BIGINT NOT NULL,
    category_id BIGINT NOT NULL,
    formula_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT pk_category_field PRIMARY KEY (id),
    CONSTRAINT uq_category_field_category_id UNIQUE (category_id),
    CONSTRAINT fk_category_field_category FOREIGN KEY (category_id)
        REFERENCES prime.category (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE UNIQUE INDEX idx_category_field_category_id
    ON prime.category_field (category_id);


CREATE TABLE prime.formula (
    id BIGINT NOT NULL,
    category_field_id BIGINT NOT NULL,
    field_id BIGINT NOT NULL,
    formula_definition VARCHAR(1000) NOT NULL,
    encoded_formula VARCHAR(1000),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT pk_formula PRIMARY KEY (id),
    CONSTRAINT uq_formula_category_field_id_field_id UNIQUE (category_field_id, field_id),
    CONSTRAINT fk_formula_category_field FOREIGN KEY (category_field_id)
        REFERENCES prime.category_field (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_formula_lu_field_name FOREIGN KEY (field_id)
        REFERENCES prime.lu_field_name (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE UNIQUE INDEX idx_formula_category_field_id_field_id
    ON prime.formula (category_field_id, field_id);
