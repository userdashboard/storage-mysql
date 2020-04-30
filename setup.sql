CREATE TABLE IF NOT EXISTS lists (
  id BIGINT NOT NULL AUTO_INCREMENT,
  path VARCHAR(255),
  objectid VARCHAR(255),
  PRIMARY KEY (id),
  INDEX(objectid),
  INDEX(path)
);

CREATE TABLE IF NOT EXISTS objects (
  path VARCHAR(255),
  contents BLOB,
  PRIMARY KEY (path)
);