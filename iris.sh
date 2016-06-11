#!/bin/bash

# Iris - A script achieving user profiling based on network activity
#
# Copyright (C) 2016 Yann Privé
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program. If not, see http://www.gnu.org/licenses/.

# Gets the execution parameters
readonly PROGNAME=$(basename $0)
readonly ARGS=("$@")
readonly ARGS_NB=$#

# Global parameters of the script
readonly PSQL_USER='<username>'
readonly PSQL_DATABASE='<database>'
readonly DMOZ_RDF=(ad-content kt-content content)

# Main function
main() {
	
	# Local variables
	local pcap_path
	local tmpfile
	
	# Checks that there are at least 1 argument
	[[ ${ARGS_NB} -lt 1 ]] && { error 'argument_missing' ; return $? ; }
	
	# Gets the command given
	case ${ARGS[0]} in
		
		# If the command implies the database...
		analyse|dmoz)
		
			# ... open a connection
			coproc db { psql -Atnq -U ${PSQL_USER} -d ${PSQL_DATABASE} 2>&1 ; }
			;;&
		
		# Analyses a dataset
		analyse)
			
			# Gets the PCAP path
			pcap_path=${ARGS[1]}
			
			# Tests if the file exists on the disk
			[[ -e "${pcap_path}" ]] || { error 'file_doesnt_exist' "${pcap_path}" ; return $? ; }
			
			# Processes the dataset
			fill_dataset "$pcap_path"
			
			# Creates a temporary file to store the results of libprotoident / ngrep
			tmpfile=$(mktemp)
			
			# Fills the flows table
			fill_flows "$pcap_path" "$tmpfile"
			
			# And finally the websites
			fill_websites "$pcap_path" "$tmpfile"
			
			# Deletes the temporary file
			rm $tmpfile
			;;&
		
		# Updates the DMOZ database
		dmoz)
			
			[[ "${ARGS[1]}" == 'update' ]] \
				&& update_dmoz \
				|| { error 'dmoz_option' "${ARGS[1]}" ; return $? ; }
			;;&
			
		# Closes the database connection
		analyse|dmoz)
			
			echo '\q' >&${db[1]}
			;;
		
		# Displays the help
		help)
			help
			;;
		
		# Unknown argument
		*)
			error 'unknown_argument' "${ARGS[0]}"
			return $?
			;;
		
	esac
	
	return $?
}

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
		[[ "$line" == 'ERROR'* ]] && { echo '\q' ; error 'already_processed' "${pcap_name}" ; return $? ; }
		[[ "$line" == 'EOF' ]] && break
	done <&${db[0]}
	
	return 0
}

# Fills the flows
fill_flows() {
	
	# Local variables
	local pcap_path=$1 tmpfile=$2
	local line dataset_id
	local protocols protocol ids

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
	
	# Processes the PCAP file
	lpi_protoident "pcap:${pcap_path}" > $tmpfile 2>/dev/null
	
	# Information
	echo -ne 'done!\nInsertion of the new protocols in the database... '
	
	# Adds the protocols not already existing in the database
	exec_sql 'CREATE TEMPORARY TABLE protocols_tmp (name VARCHAR(50) PRIMARY KEY);'
	exec_sql 'PREPARE protocols_plan (VARCHAR(50)) AS INSERT INTO protocols_tmp (name) VALUES ($1);'
	exec_sql 'BEGIN;'
	awk '!exists[$1]++ {printf "EXECUTE protocols_plan (\47%s\47);\n", tolower($1)}' $tmpfile >&${db[1]}
	exec_sql 'COMMIT;'
	exec_sql 'INSERT INTO protocols (name) SELECT t.name FROM protocols_tmp t LEFT JOIN protocols p ON t.name = p.name WHERE p.name IS NULL;'
	
	# Information
	echo -ne 'done!\nInsertion of the flows in the database (this may take some time)... '
	
	# Insert in the database
	exec_sql <<- 'SQL'
	PREPARE flows_plan (INT, VARCHAR(50), SMALLINT, DOUBLE PRECISION, INET, INET, INT, INT, BIGINT, BIGINT) AS
	INSERT INTO flows (dataset, protocol, transport, timestamp, endpoint_a, endpoint_b, port_a, port_b, payload_size_ab, payload_size_ba)
	VALUES ($1, (SELECT id FROM protocols WHERE name = $2), $3, to_timestamp($4), $5, $6, $7, $8, $9, $10);
	SQL
	exec_sql 'BEGIN;'
	awk -v dataset_id=${dataset_id} '{printf "EXECUTE flows_plan (%s, \47%s\47, %s, %s, \47%s\47, \47%s\47, %s, %s, %s, %s);\n", dataset_id, tolower($1), $6, $7, $2, $3, $4, $5, $8, $9}' $tmpfile >&${db[1]}
	exec_sql 'COMMIT;'
	
	# Information
	echo 'done!'
	
	return 0
}

# Fills the table "website"
fill_websites() {
	
	# Local variables
	local pcap_path=$1 tmpfile=$2
	local pcap_name
	local filters extra_filter
	
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
			| awk '{if(NF != 4){next} ; $1 = substr($1, 1, 14) ; ip_nb = split($2, ip, ",") ; for(i = 1 ; i < ip_nb ; i++) {printf "EXECUTE websites_plan (%s, \47%s\47, \47%s\47, \47%s\47);\n", $1, ip[i], $3, $4}}'
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
	
	return 0
}

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

# Executes SQL
exec_sql() {

	# Local variables
	local argument=$1
	local line
	
	# If an argument is given, simply echoes it
	if [[ -n "$argument" ]]
	then
		echo "$argument" >&${db[1]}
	
	# Else, it's a here-document, reads it
	else
		while read line
		do
			echo "$line" >&${db[1]}
		done
	fi
	
	# Sends an "EOF" string to be returned once it finishes
	echo '\echo EOF' >&${db[1]}
	
	# Reads the output of the server
	while read line
	do
		case $line in
			EOF)
				break
				;;
			ERROR:*|DETAIL:*)
				error 'database_error' "$line"
				return $?
				;;
			*)
				echo "INFO: Unexpected database output: $line"
				;;
		esac
	done <&${db[0]}
	
	return 0
}


# Error handling
error() {

	local err=$1

	# Displays the error
	echo -n 'ERROR: ' >&2
	case $err in
		argument_missing)
			echo "${PROGNAME} expects at least one argument. Run \"${PROGNAME} help\" for further help." >&2
			;;
		unknown_argument)
			echo "Unknown argument $2. Run \"${PROGNAME} help\" for further help." >&2
			;;
		dmoz_option)
			echo "Unexpected DMOZ option: $2."
			;;
		already_processed)
			echo "The dataset $2 has already been processed. Exiting..." >&2
			;;
		database_error)
			echo "The database returned the following error during a statement execution: \"$2\"" >&2
			;;
		file_doesnt_exist)
			echo "The file $2 doesn't not exist on the disk. Exiting..." >&2
			;;
		rdf_download)
			echo "Error when trying to download the RDF: $2." >&2
			;;
		*)
			echo "Unrecognized error: $err" >&2
			;;
	esac

	return 1
}

# Help about the script
help() {

	cat <<- EOF
	Usage: ${PROGNAME} [ analyse <pcap_path> | dmoz update ]
	
	Operations:
	  analyse <pcap_path>: Analyses the PCAP given as argument.
	  dmoz update: Updates the DMOZ database used for classification.
	  help: Displays this help.
	
	To analyse a PCAP, you will probably run: ./${PROGNAME} /home/me/capture.pcap
	Please make sure that the configuration (of the database) is correct before running ${PROGNAME}.
	EOF
	
	return 0
}

# Launches the main function
main

# Exits with the correct exit code
exit $?
