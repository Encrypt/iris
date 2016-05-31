-- Datasets corresponding to the PCAP files added
CREATE TABLE datasets (
	id		SERIAL			PRIMARY KEY,
	name	VARCHAR(255)	NOT NULL,
	md5sum	CHAR(32)		NOT NULL UNIQUE,
	added	TIMESTAMP		NOT NULL
);

-- Protocols identified by libprotoident
CREATE TABLE protocols (
	id		SERIAL		PRIMARY KEY,
	name	VARCHAR(50)	NOT NULL UNIQUE
);

-- Topics of the categories (using DMOZ classification)
CREATE TABLE topics (
	id		SERIAL		PRIMARY KEY,
	name	VARCHAR(50)	NOT NULL UNIQUE
);

-- Subtopics of the categories (using DMOZ classification)
CREATE TABLE subtopics (
	id		SERIAL		PRIMARY KEY,
	name	VARCHAR(50)	NOT NULL UNIQUE
);

-- Categories of the websites
CREATE TABLE categories (
	id			SERIAL	PRIMARY KEY,
	topic		INT		NOT NULL REFERENCES topics(id),
	subtopic	INT		NOT NULL REFERENCES subtopics(id)
);

-- URLs of the websites
CREATE TABLE urls (
	id		SERIAL			PRIMARY KEY,
	value	VARCHAR(255)	NOT NULL UNIQUE
);

-- DMOZ database
CREATE TABLE dmoz (
	id				SERIAL	PRIMARY KEY,
	url				INT		NOT NULL UNIQUE REFERENCES urls(id),
	category		INT		NOT NULL REFERENCES categories(id)
);

-- Websites visited by the users
CREATE TABLE websites (
	id				SERIAL	PRIMARY KEY,
	url				INT		NOT NULL REFERENCES urls(id),
	category		INT		REFERENCES categories(id),
	hand_classified	BOOLEAN
);

-- Flows identified by libprotoident
CREATE TABLE flows (
	id				BIGSERIAL	PRIMARY KEY,
	dataset			INT			NOT NULL REFERENCES datasets(id),
	website			INT			REFERENCES websites(id),
	protocol		INT			NOT NULL,
	transport		SMALLINT	NOT NULL,
	timestamp		TIMESTAMP	NOT NULL,
	endpoint_a		INET		NOT NULL,
	endpoint_b		INET		NOT NULL,
	port_a			INT			NOT NULL,
	port_b			INT			NOT NULL,
	payload_size_ab	BIGINT		NOT NULL,
	payload_size_ba	BIGINT		NOT NULL
);

-- Index needed for the "complex" joins
CREATE INDEX ON flows (endpoint_a, timestamp);
