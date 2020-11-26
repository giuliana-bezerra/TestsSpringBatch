CREATE TABLE `customer` (
  `name` varchar(255) NOT NULL,
  `age` integer,
  `city` varchar(255) DEFAULT '',
  `state` varchar(255) DEFAULT '',
  `address` varchar(255) DEFAULT '',
  `cell_phone` varchar(255) DEFAULT '',
  `email` varchar(255) DEFAULT '',
  `work_phone` varchar(255) DEFAULT '',
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `account` (
  `id` varchar(255) NOT NULL,
  `customer` varchar(255) DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;