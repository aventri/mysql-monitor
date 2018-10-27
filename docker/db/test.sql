GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'monitor'@'%';

USE monitor_test;

CREATE TABLE monitor_test_data (
  `id` INTEGER AUTO_INCREMENT PRIMARY KEY,
  `text` VARCHAR(255),
  `number` INT,
  `datetime` DATETIME
);

INSERT INTO monitor_test_data (`text`, `number`, `datetime`) VALUES ("Some text", "123", "2018-01-01 12:30:00");
INSERT INTO monitor_test_data (`text`, `number`, `datetime`) VALUES ("More text", "456", "2018-02-01 17:00:00");

UPDATE monitor_test_data SET `text` = "Better text" WHERE `text` = "More text";
