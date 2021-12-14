CREATE TABLE authors (
	author_id SERIAL,
	author_name varchar(40) NOT NULL,
	PRIMARY KEY (author_id)
);

CREATE TABLE periods (
	period_id SERIAL,
	period_name VARCHAR(50) NOT NULL,
	PRIMARY KEY (period_id)
);

CREATE TABLE genres (
	genre_id SERIAL,
	genre_name VARCHAR(25) NOT NULL,
	PRIMARY KEY (genre_id)
);

CREATE TABLE poems (
	poem_name VARCHAR(100) UNIQUE,
	body VARCHAR(1000) NOT NULL,
	author_id INT NOT NULL,
	period_id INT NOT NULL,
	genre_id INT NOT NULL,
	PRIMARY KEY (poem_name),
	CONSTRAINT FK_author
		FOREIGN KEY (author_id)
			REFERENCES authors (author_id) ON DELETE CASCADE,
	CONSTRAINT FK_period
		FOREIGN KEY (period_id)
	  		REFERENCES periods (period_id) ON DELETE CASCADE,
	CONSTRAINT FK_genre
		FOREIGN KEY (genre_id)
	  		REFERENCES genres (genre_id) ON DELETE CASCADE
);
