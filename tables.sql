-- Datasets corresponding to the PCAP files added
CREATE TABLE datasets (
	id 		SERIAL 			PRIMARY KEY	NOT NULL,
	name	VARCHAR(255)	NOT NULL,
	md5sum	CHAR(32)		NOT NULL UNIQUE,
	added	TIMESTAMP		NOT NULL
);

-- Protocols identified by libprotoident
CREATE TABLE protocols (
	id 		SERIAL 		PRIMARY KEY	NOT NULL,
	name	VARCHAR(50)	NOT NULL UNIQUE
);

-- Websites visited by the users
CREATE TABLE websites (
	id 			SERIAL			PRIMARY KEY	NOT NULL,
	url			VARCHAR(255)	NOT NULL UNIQUE,
	category	VARCHAR(20)
);

-- Flows identified by libprotoident
CREATE TABLE flows (
	id 				BIGSERIAL	PRIMARY KEY,
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
