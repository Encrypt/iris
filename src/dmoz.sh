# File: dmoz.sh
# Content: Function to update the DMOZ database of IRIS

# Updates DMOZ categories & database
update_dmoz() {

	# Local variables
	local rdf
	
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
	
	# Downloads the RDF databases
	for rdf in ${DMOZ_RDF[@]}
	do
		
		# Information
		echo -n "Now downloading the dataset ${rdf}.rdf.u8.gz... "
		
		# Downloads the database and ungzips it
		wget -qO /tmp/${rdf}.rdf.u8.gz http://rdf.dmoz.org/rdf/${rdf}.rdf.u8.gz \
			&& gunzip /tmp/${rdf}.rdf.u8.gz \
			|| { error 'rdf_download' "http://rdf.dmoz.org/rdf/${rdf}.rdf.u8.gz" ; return $? ; }
		
		# Information
		echo 'done!'
		
	done
	
	# Inserts the DMOZ data in the database
	exec_sql <<- 'SQL'
	PREPARE dmoz_plan (VARCHAR(50), VARCHAR(50), VARCHAR(255)) AS
	INSERT INTO dmoz_tmp (topic, subtopic, url)
	VALUES ($1, $2, $3);
	SQL
	exec_sql 'BEGIN;'
	for rdf in ${DMOZ_RDF[@]}
	do
		# Information
		echo -n "Now adding the dataset ${rdf}.rdf.u8 to the database... "
		
		# Adds the dataset
		{
			sed -n 's/  <Topic r:id="\(Top\/\)\?\([^/]*\)\/\([^/]*\)">/\2 \3/p;s/    <link r:resource="https*:\/\/\([^/"]*\)\/".*/\1/p' /tmp/${rdf}.rdf.u8 \
				| awk '{if(NF == 2){if($1 == "Top"){next} ; gsub("\47", "_") ; gsub(",", "") ; topic=$1 ; subtopic=$2} else {printf "EXECUTE dmoz_plan (\47%s\47, \47%s\47, \47%s\47);\n", tolower(topic), tolower(subtopic), $0}}'
		} >&${db[1]}
		
		# Information
		echo 'done!'
		
	done
	exec_sql 'COMMIT;'
	
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
	
	return 0
}
