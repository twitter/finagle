CREATE TEMPORARY TABLE IF NOT EXISTS swimming_record (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  event varchar(30) DEFAULT NULL,
  time float DEFAULT NULL,
  name varchar(40) DEFAULT NULL,
  nationality varchar(20) DEFAULT NULL,
  date date DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;