CREATE TABLE IF NOT EXISTS dds.dm_users (
	id serial NOT null primary key,
	user_id varchar NOT NULL,
	user_name  varchar NOT NULL,
	user_login varchar NOT NULL
);