CREATE TABLE %VERTICES% (
  id SERIAL NOT NULL PRIMARY KEY
);

CREATE TABLE %EDGES% (
  id SERIAL NOT NULL PRIMARY KEY,
  vertex_out INT NOT NULL,
  vertex_in INT NOT NULL,
  label CHARACTER VARYING (255) NOT NULL,
  CONSTRAINT fk_vertex_out FOREIGN KEY (vertex_out) REFERENCES %VERTICES% (id)
    ON DELETE CASCADE,
  CONSTRAINT fk_vertex_in FOREIGN KEY (vertex_in) REFERENCES %VERTICES% (id)
    ON DELETE CASCADE
);

CREATE INDEX idx_edge_labels ON %EDGES% (label);

CREATE TABLE %VERTEX_PROPERTIES% (
  vertex_id INT NOT NULL,
  name CHARACTER VARYING(255) NOT NULL,
  string_value TEXT,
  numeric_value NUMERIC,
  value_type SMALLINT NOT NULL,
  CONSTRAINT fk_vertex FOREIGN KEY (vertex_id) REFERENCES %VERTICES% (id)
    ON DELETE CASCADE,
  UNIQUE (vertex_id, name)
);

CREATE INDEX idx_%VERTEX_PROPERTIES% ON %VERTEX_PROPERTIES% (name);
-- H2 doesn't support index on the TEXT column
-- CREATE INDEX idx_%VERTEX_PROPERTIES%_2 ON %VERTEX_PROPERTIES% (name, string_value);
-- CREATE INDEX idx_%VERTEX_PROPERTIES%_3 ON %VERTEX_PROPERTIES% (name, numeric_value);

CREATE TABLE %EDGE_PROPERTIES% (
  edge_id INT NOT NULL,
  name CHARACTER VARYING(255) NOT NULL,
  string_value TEXT,
  numeric_value NUMERIC,
  value_type SMALLINT NOT NULL,
  CONSTRAINT fk_edge FOREIGN KEY (edge_id) REFERENCES %EDGES% (id)
    ON DELETE CASCADE,
  UNIQUE (edge_id, name)
);

CREATE INDEX idx_%EDGE_PROPERTIES% ON %EDGE_PROPERTIES% (name);
-- H2 doesn't support index on the TEXT column
-- CREATE INDEX idx_%EDGE_PROPERTIES%_2 ON %EDGE_PROPERTIES% (name, string_value);
-- CREATE INDEX idx_%EDGE_PROPERTIES%_3 ON %EDGE_PROPERTIES% (name, numeric_value);
