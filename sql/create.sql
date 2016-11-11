----
-- tasks
----
CREATE TABLE `tasks` (
	`id` INT(10) unsigned NOT NULL AUTO_INCREMENT,
	`name` varchar(255) NOT NULL,
	`args` BLOB,
	`status` varchar(255) NOT NULL DEFAULT 'todo',
    `last_error` varchar(2048) NOT NULL DEFAULT '',
    `retry` int(10) DEFAULT '5',
	`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	INDEX name_idx (`name`),
	INDEX status_idx (`status`),
	INDEX created_at_idx (`created_at`),
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
