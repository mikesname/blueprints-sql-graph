

DROP TABLE IF EXISTS edge_properties;
DROP TABLE IF EXISTS vertex_properties;
DROP TABLE IF EXISTS edges;
DROP TABLE IF EXISTS vertices;

CREATE TABLE vertices (
  id INT(10) NOT NULL AUTO_INCREMENT PRIMARY KEY
);

CREATE TABLE edges (
  id INT(10) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  vertex_out INT(10) NOT NULL,
  vertex_in INT(10) NOT NULL,
  label VARCHAR(255) NOT NULL,
  CONSTRAINT FOREIGN KEY (vertex_out) REFERENCES vertices (id) ON DELETE CASCADE,
  CONSTRAINT FOREIGN KEY (vertex_in) REFERENCES vertices (id) ON DELETE CASCADE
);


CREATE TABLE vertex_properties(
  id INT(10) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  vertex_id INT(10) NOT NULL,
  name VARCHAR(255) NOT NULL,
  value varchar(4096),
  UNIQUE KEY(vertex_id, name),
  CONSTRAINT FOREIGN KEY (vertex_id) REFERENCES vertices (id) ON DELETE CASCADE
);


CREATE TABLE edge_properties(
  edge_id INT(10) NOT NULL,
  name VARCHAR(255) NOT NULL,
  value varchar(4096),
  UNIQUE KEY(edge_id, name),
  CONSTRAINT FOREIGN KEY (edge_id) REFERENCES edges (id) ON DELETE CASCADE
);

