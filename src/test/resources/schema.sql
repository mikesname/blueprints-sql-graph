

DROP TABLE IF EXISTS edge_properties;
DROP TABLE IF EXISTS vertex_properties;
DROP TABLE IF EXISTS edges;
DROP TABLE IF EXISTS vertices;

CREATE TABLE vertices (
  id SERIAL NOT NULL PRIMARY KEY
);

CREATE TABLE edges (
  id SERIAL NOT NULL PRIMARY KEY,
  vertex_out INT NOT NULL,
  vertex_in INT NOT NULL,
  label CHARACTER VARYING (255) NOT NULL,
  CONSTRAINT fk_vertex_out FOREIGN KEY (vertex_out) REFERENCES vertices (id)
    ON DELETE CASCADE ,
  CONSTRAINT fk_vertex_in FOREIGN KEY (vertex_in) REFERENCES vertices (id)
    ON DELETE CASCADE
);

CREATE INDEX idx_edge_labels ON edges (label);

CREATE TABLE vertex_properties(
  id SERIAL NOT NULL PRIMARY KEY,
  vertex_id INT NOT NULL,
  key CHARACTER VARYING(255) NOT NULL,
  value bytea NOT NULL,
  CONSTRAINT fk_vertex FOREIGN KEY (vertex_id) REFERENCES vertices (id)
    ON DELETE CASCADE
);

CREATE INDEX idx_vertex_properties ON vertex_properties (vertex_id, key, value);

CREATE TABLE edge_properties(
  id SERIAL NOT NULL PRIMARY KEY,
  edge_id INT NOT NULL,
  key CHARACTER VARYING(255) NOT NULL,
  value bytea NOT NULL,
  CONSTRAINT fk_edge FOREIGN KEY (edge_id) REFERENCES edges (id)
    ON DELETE CASCADE
);

CREATE INDEX idx_edge_properties ON edge_properties (edge_id, key, value);
