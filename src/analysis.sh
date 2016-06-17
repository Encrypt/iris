# File: analysis.sh
# Content: Functions to analyse PCAP files

# Adds the dataset in the database
fill_dataset() {

	# Local variables
	local pcap_path=$1
	local pcap_md5 pcap_name timestamp
	
	# Gets information about the dataset
	pcap_md5=$(head -c 100M ${pcap_path} | md5sum | cut -c1-32)
	pcap_name=$(basename ${pcap_path})
	timestamp=$(date '+%F %T')
	
	# Inserts the new dataset in the database if has not already been added
	echo "INSERT INTO datasets VALUES (DEFAULT, '${pcap_name}', '${pcap_md5}', '${timestamp}'); \echo EOF" >&${db[1]}
	while read line
	do
		[[ "$line" == 'ERROR'* ]] && { echo '\q' >&${db[1]} ; error 'already_processed' "${pcap_name}" ; return $? ; }
		[[ "$line" == 'EOF' ]] && break
	done <&${db[0]}
	
	return 0
}

# Fills the flows
fill_flows() {
	
	# Local variables
	local pcap_path=$1
	local tmpfiles line dataset_id
	local protocols protocol ids
	local lpi_pids
	
	# Creates 3 temporary files for lpi_arff and lpi_protoident and both merged
	for i in {0..2}
	do
		tmpfiles+=($(mktemp))
	done
	
	# Information
	echo -n 'Retrieving the dataset ID from the database... '
	
	# Gets the ID corresponding to the previous insert statement
	echo 'SELECT id FROM datasets ORDER BY added DESC LIMIT 1; \echo EOF' >&${db[1]}
	while read line
	do
		[[ "$line" != 'EOF' ]] && dataset_id="$line" || break
	done <&${db[0]}
	
	# Information
	echo -ne 'done!\nAnalysis of the flows of the PCAP in progress... '
	
	# Processes the PCAP file with lpi_protoident
	# Extracts: protocol, endpoint_a, endpoint_b, port_a, port_b, transport
	{
		lpi_protoident "pcap:${pcap_path}" 2>/dev/null \
			| awk '{print $1, $2, $3, $4, $5, $6}' \
			> ${tmpfiles[0]}
	} &
	lpi_pids+=($!)
	
	# Does the same with lpi_arff to get more info about the flows
	# Extracts: timestamp, duration, payload_size_ab, payload_size_ba, packets_nb_ab, packets_nb_ba
	{
		lpi_arff "pcap:${pcap_path}" 2>/dev/null \
			| awk -F ',' '{if(NF == 24) {print $24, $23, $4, $6, $5, $7}}' \
			> ${tmpfiles[1]}
	} &
	lpi_pids+=($!)
	
	# Waits for the analysis tools to finish
	wait ${lpi_pids[@]}
	
	# Merge both analyses
	paste -d ' ' "${tmpfiles[0]}" "${tmpfiles[1]}" > ${tmpfiles[2]}
		
	# Information
	echo -ne 'done!\nInsertion of the new protocols in the database... '
	
	# Adds the protocols not already existing in the database
	exec_sql 'CREATE TEMPORARY TABLE protocols_tmp (name VARCHAR(50) PRIMARY KEY);'
	exec_sql 'PREPARE protocols_plan (VARCHAR(50)) AS INSERT INTO protocols_tmp (name) VALUES ($1);'
	exec_sql 'BEGIN;'
	awk '!exists[$1]++ {printf "EXECUTE protocols_plan (\47%s\47);\n", tolower($1)}' ${tmpfiles[2]} >&${db[1]}
	exec_sql 'COMMIT;'
	exec_sql 'INSERT INTO protocols (name) SELECT t.name FROM protocols_tmp t LEFT JOIN protocols p ON t.name = p.name WHERE p.name IS NULL;'
	
	# Information
	echo -ne 'done!\nInsertion of the flows in the database (this may take some time)... '
	
	# Insert in the database
	exec_sql <<- 'SQL'
	PREPARE flows_plan (INT, VARCHAR(50), SMALLINT, DOUBLE PRECISION, BIGINT, INET, INET, INT, INT, BIGINT, BIGINT, INT, INT) AS
	INSERT INTO flows (dataset, protocol, transport, timestamp, duration, endpoint_a, endpoint_b, port_a, port_b, payload_size_ab, payload_size_ba, packets_nb_ab, packets_nb_ba)
	VALUES ($1, (SELECT id FROM protocols WHERE name = $2), $3, to_timestamp($4), $5, $6, $7, $8, $9, $10, $11, $12, $13);
	SQL
	exec_sql 'BEGIN;'
	awk -v dataset_id=${dataset_id} '{printf "EXECUTE flows_plan (%s, \47%s\47, %s, %s, %s, \47%s\47, \47%s\47, %s, %s, %s, %s, %s, %s);\n", dataset_id, tolower($1), $6, $7, $8, $2, $3, $4, $5, $9, $10, $11, $12}' ${tmpfiles[2]} >&${db[1]}
	exec_sql 'COMMIT;'
	
	# Information
	echo 'done!'
	
	# Deletes the temporary files
	rm ${tmpfiles[@]}
	
	return 0
}

# Fills the table "website"
fill_websites() {
	
	# Local variables
	local pcap_path=$1
	local pcap_name tmpfile
	local filters extra_filter
	
	# Creates a temporary file
	tmpfile=$(mktemp)
	
	# Gets the basename of the PCAP
	pcap_name=$(basename ${pcap_path})
	
	# Creates a temporary database for the websites found in the PCAP
	exec_sql <<- 'SQL'
	CREATE TEMPORARY TABLE websites_analysis (
		id 			SERIAL			PRIMARY KEY,
		timestamp	TIMESTAMP		NOT NULL,
		endpoint_a	INET			NOT NULL,
		endpoint_b	INET			NOT NULL,
		url			VARCHAR(255)	NOT NULL
	);
	SQL
	
	# Information
	echo -n 'Analysis of the websites visited... '
	
	# Inserts in the websites in the temporary database
	exec_sql <<- 'SQL'
	PREPARE websites_plan (DOUBLE PRECISION, INET, INET, VARCHAR(255)) AS
	INSERT INTO websites_analysis (timestamp, endpoint_a, endpoint_b, url)
	VALUES (to_timestamp($1), $2, $3, $4);
	SQL
	exec_sql 'BEGIN;'
	{
		/usr/sbin/tcpdump -r ${pcap_path} -w - 'udp port 53' 2>/dev/null \
			| tshark -T fields -e frame.time_epoch -e dns.a -e ip.dst -e dns.qry.name -Y 'dns.flags.response eq 1' -r - \
			| awk '{if(NF != 4){next} ; $1 = substr($1, 1, 17) ; ip_nb = split($2, ip, ",") ; for(i = 1 ; i < ip_nb ; i++) {printf "EXECUTE websites_plan (%s, \47%s\47, \47%s\47, \47%s\47);\n", $1, ip[i], $3, $4}}'
	} >&${db[1]}
	exec_sql 'COMMIT;'
	
	# Creates a view to insert the necessary rows
	exec_sql <<- SQL
	CREATE TEMPORARY VIEW websites_view AS
	SELECT DISTINCT ON (f.id) f.id AS flow_id, u.id AS url_id, a.url as url_value, (f.timestamp - a.timestamp) AS tstamp_diff
	FROM flows f
	JOIN websites_analysis a ON f.endpoint_a = a.endpoint_a AND f.endpoint_b = a.endpoint_b
	LEFT JOIN urls u ON a.url = u.value
	WHERE f.dataset = (SELECT id FROM datasets WHERE name = '${pcap_name}' ORDER BY added DESC LIMIT 1)
		AND f.protocol IN (SELECT id FROM protocols WHERE name = 'http' OR name = 'https')
		AND f.timestamp BETWEEN a.timestamp AND a.timestamp + interval '15 minutes'
	ORDER BY f.id, tstamp_diff ASC;
	SQL
	
	# Inserts the URLs of the websites which do not yet exist in "websites"
	exec_sql 'INSERT INTO urls (value) SELECT DISTINCT url_value FROM websites_view WHERE url_id IS NULL;'
	
	# Inserts the identified websites in the database
	exec_sql 'INSERT INTO websites (url) SELECT DISTINCT v.url_id FROM websites_view v LEFT JOIN websites w ON v.url_id = w.url WHERE w.url IS NULL AND v.url_id IS NOT NULL;'
	
	# Update the flows with the websites added
	exec_sql 'UPDATE flows f SET website = w.id FROM websites_view v JOIN websites w ON w.url = v.url_id WHERE f.id = v.flow_id;'
	
	# Automatically try to set a category to the new websites
	filters+=("AND c.topic NOT IN (SELECT id FROM topics WHERE name = 'world' OR name = 'regional')")
	filters+=("AND c.topic != (SELECT id FROM topics WHERE name = 'world')")
	filters+=(" ")
	
	for extra_filter in "${filters[@]}"
	do
		exec_sql <<- SQL
		UPDATE websites w
		SET category = s.category, hand_classified = FALSE
		FROM (
			SELECT DISTINCT ON (w.id) w.id, d.category
			FROM websites w JOIN dmoz d ON w.url = d.url
			JOIN categories c ON d.category = c.id
			WHERE hand_classified IS NULL ${extra_filter}
		) s
		WHERE s.id = w.id;
		SQL
	done
	
	# Information
	echo 'done!'
	
	# Deletes the temporary file
	rm $tmpfile
	
	return 0
}
