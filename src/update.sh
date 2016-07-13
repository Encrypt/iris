# File: update.sh
# Content: Functions to update the DMOZ, ads & cdn databases of IRIS

# Updates DMOZ categories & database
update_dmoz() {

	# Local variables
	local rdf rdf_nb
	
	info 'Update of the dmoz table...' '0'
	info ' ├─ Cleaning previous downloads...' '0'
	
	# Removes previous RDFs if there is any
	for rdf in ${DMOZ_RDF[@]}
	do
		[[ -e "/tmp/${rdf}.rdf.u8.gz" ]] && rm /tmp/${rdf}.rdf.u8.gz
		[[ -e "/tmp/${rdf}.rdf.u8" ]] && rm /tmp/${rdf}.rdf.u8
	done
	
	# Creates the temporary tables for the update
	exec_sql <<- 'SQL'
	CREATE TEMPORARY TABLE dmoz_tmp (
		id			SERIAL			PRIMARY KEY,
		topic		VARCHAR(50)		NOT NULL,
		subtopic	VARCHAR(50)		NOT NULL,
		url			VARCHAR(255)	NOT NULL
	);
	SQL
	
	info ' ├─ Downloading the DMOZ RDF dumps...' '10'
	
	# Downloads the RDF databases
	rdf_nb=1
	for rdf in ${DMOZ_RDF[@]}
	do
		
		# Downloads the database and ungzips it
		wget -qO /tmp/${rdf}.rdf.u8.gz http://rdf.dmoz.org/rdf/${rdf}.rdf.u8.gz \
			&& gunzip /tmp/${rdf}.rdf.u8.gz \
			|| { error 'rdf_download' "http://rdf.dmoz.org/rdf/${rdf}.rdf.u8.gz" ; return $? ; }
		
		info " ├─ Downloaded the ${rdf} RDF dump..." "$((${rdf_nb} * 30 / ${#DMOZ_RDF[@]} + 10))"
		
		# Increments the dataset number
		rdf_nb=$((${rdf_nb} + 1))
		
	done
	
	# Inserts the DMOZ data in the database
	exec_sql <<- 'SQL'
	PREPARE dmoz_plan (VARCHAR(50), VARCHAR(50), VARCHAR(255)) AS
	INSERT INTO dmoz_tmp (topic, subtopic, url)
	VALUES ($1, $2, $3);
	SQL
	exec_sql 'BEGIN;'
	
	rdf_nb=1
	for rdf in ${DMOZ_RDF[@]}
	do
		
		info " ├─ Adding the ${rdf} RDF entries to the database..." "$((${rdf_nb} * 30 / ${#DMOZ_RDF[@]} + 40))"
		
		# Adds the dataset
		{
			sed -n 's/  <Topic r:id="\(Top\/\)\?\([^/]*\)\/\([^/]*\)">/\2 \3/p;s/    <link r:resource="https*:\/\/\([^/"]*\)\/".*/\1/p' /tmp/${rdf}.rdf.u8 \
				| awk '{if(NF == 2){if($1 == "Top"){next} ; gsub("\47", "_") ; gsub(",", "") ; topic=$1 ; subtopic=$2} else {printf "EXECUTE dmoz_plan (\47%s\47, \47%s\47, \47%s\47);\n", tolower(topic), tolower(subtopic), $0}}'
		} >&${db[1]}
		
		# Increments the dataset number
		rdf_nb=$((${rdf_nb} + 1))
		
	done
	exec_sql 'COMMIT;'
	
	info ' ├─ Filling the topics and subtopics tables...' '80'
	
	# Fills the "topics" and "suptopics" tables
	exec_sql 'INSERT INTO topics (name) SELECT DISTINCT d.topic FROM dmoz_tmp d LEFT JOIN topics t ON d.topic = t.name WHERE t.name IS NULL;'
	exec_sql 'INSERT INTO subtopics (name) SELECT DISTINCT d.subtopic FROM dmoz_tmp d LEFT JOIN subtopics s ON d.subtopic = s.name WHERE s.name IS NULL;'
	
	# Fills the "categories" table
	exec_sql <<- 'SQL'
	INSERT INTO categories (topic, subtopic)
	SELECT q.topic, q.subtopic
	FROM (
		SELECT DISTINCT t.id AS topic, s.id AS subtopic
		FROM dmoz_tmp d
		JOIN topics t ON d.topic = t.name
		JOIN subtopics s ON d.subtopic = s.name
	) q
	LEFT JOIN categories c ON q.topic = c.topic
		AND q.subtopic = c.subtopic
	WHERE c.id IS NULL;
	SQL
	
	info ' ├─ Filling the urls table...' '90'
	
	# Fills the "urls" table
	exec_sql 'INSERT INTO urls (value) SELECT DISTINCT d.url FROM dmoz_tmp d LEFT JOIN urls u ON d.url = u.value WHERE u.value IS NULL;'
	
	# Fills the "dmoz" table
	exec_sql <<- 'SQL'
	INSERT INTO dmoz (url, category)
	SELECT u.id, q.category_id
	FROM dmoz_tmp t
	JOIN urls u ON t.url = u.value
	JOIN (
		SELECT c.id as category_id, t.id AS topic_id, t.name AS topic_name, s.id AS subtopic_id, s.name AS subtopic_name
		FROM categories c
		JOIN topics t ON c.topic = t.id
		JOIN subtopics s ON c.subtopic = s.id
		) q
	ON t.topic = q.topic_name
		AND t.subtopic = q.subtopic_name
	LEFT JOIN dmoz d ON d.url = u.id
		AND d.category = q.category_id
	WHERE d.id IS NULL;
	SQL
	
	# Removes all the downloaded RDFs
	for rdf in ${DMOZ_RDF[@]}
	do
		[[ -e "/tmp/${rdf}.rdf.u8.gz" ]] && rm /tmp/${rdf}.rdf.u8.gz
		[[ -e "/tmp/${rdf}.rdf.u8" ]] && rm /tmp/${rdf}.rdf.u8
	done
	
	info ' └─ Update successful!' '100'
	
	return 0
}

# Updates the ads database
update_ads() {

	info 'Update of the ads table...' '0'
	info ' ├─ Cleaning previous downloads...' '0'

	# Removes previous download if there is any
	[[ -e "/tmp/ad_servers.txt" ]] && rm /tmp/ad_servers.txt
	
	# Creates the temporary tables for the update
	exec_sql <<- 'SQL'
	CREATE TEMPORARY TABLE ads_tmp (
		id			SERIAL			PRIMARY KEY,
		url			VARCHAR(255)	NOT NULL
	);
	SQL
	
	info ' ├─ Downloading the ad_servers file...' '10'
	
	# Downloads the ads file provided by http://hosts-file.net
	wget -qO /tmp/ad_servers.txt http://hosts-file.net/ad_servers.txt \
		|| { error 'ads_download' "http://hosts-file.net/ad_servers.txt" ; return $? ; }
		
	info ' ├─ Insertion of the ad urls in the database...' '50'
	
	# Inserts the ads data in the database
	exec_sql <<- 'SQL'
	PREPARE ads_plan (VARCHAR(255)) AS
	INSERT INTO ads_tmp (url)
	VALUES ($1);
	SQL
	exec_sql 'BEGIN;'
	awk '{if(NF == 2 && $1 == "127.0.0.1"){sub(/\015/,"") ; printf "EXECUTE ads_plan (\47%s\47);\n", $2}}' /tmp/ad_servers.txt >&${db[1]}
	exec_sql 'COMMIT;'
	
	info ' ├─ Filling the urls table...' '80'
	
	# Fills the "urls" table
	exec_sql 'INSERT INTO urls (value) SELECT DISTINCT a.url FROM ads_tmp a LEFT JOIN urls u ON a.url = u.value WHERE u.value IS NULL;'
	
	# Fills the "ads" table
	exec_sql <<- 'SQL'
	INSERT INTO ads (url, category)
	SELECT q.url, q.category FROM (
		SELECT * FROM (
			SELECT u.id as url
			FROM ads_tmp t
			JOIN urls u ON u.value = t.url
		) a
		CROSS JOIN (
			SELECT id as category
			FROM categories
			WHERE topic = (SELECT id FROM topics WHERE name = 'ads')
			AND subtopic IS NULL
		) b
	) q
	LEFT JOIN ads a
	ON a.url = q.url
	WHERE a.url IS NULL;
	SQL
	
	# Removes all the downloaded ads file
	rm /tmp/ad_servers.txt
	
	info ' └─ Update successful!' '100'
	
	return 0
}

# Updates the CDNs database
update_cdns() {
	
	# Local variables
	local domain_list=$1

	info 'Update of the cdns table...' '0'

	# Creates the temporary tables for the update
	exec_sql <<- 'SQL'
	CREATE TEMPORARY TABLE cdns_tmp (
		id			SERIAL			PRIMARY KEY,
		domain		VARCHAR(100)	NOT NULL UNIQUE
	);
	SQL

	info ' ├─ Insertion of the domains in the database...' '10'

	# Fills that table
	exec_sql <<- 'SQL'
	PREPARE cdns_plan (VARCHAR(100)) AS
	INSERT INTO cdns_tmp (domain)
	VALUES ($1);
	SQL
	exec_sql 'BEGIN;'
	awk '{printf "EXECUTE cdns_plan (\47%s\47);\n", $0}' $domain_list >&${db[1]}
	exec_sql 'COMMIT;'

	info ' ├─ Filling the cdns table...' '70'

	# Fills the "cdns" table
	exec_sql 'INSERT INTO cdns (domain) SELECT DISTINCT t.domain FROM cdns_tmp t LEFT JOIN cdns c ON t.domain = c.domain WHERE c.domain IS NULL;'
	
	info ' └─ Update successful!' '100'
	
	return 0
}
