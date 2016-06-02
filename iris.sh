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
	local pcap_path=${ARGS[0]}
	local tmpfile
	
	# Tests if the file exists on the disk
	[[ -e "${pcap_path}" ]] || { error 'file_doesnt_exist' "${pcap_path}" ; return $? ; }
		
	# Opens a connection to the database
	coproc db { psql -Atnq -U ${PSQL_USER} -d ${PSQL_DATABASE} 2>&1 ; }
	
	# Processes the dataset
	fill_dataset "$pcap_path" <&${db[0]} >&${db[1]}
	
	# Creates a temporary file to store the results of libprotoident / ngrep
	tmpfile=$(mktemp)
	
	# Use another file descriptor for information
	exec 3>&1
	
	# Fills the flows table
	fill_flows "$pcap_path" "$tmpfile" <&${db[0]} >&${db[1]}
	
	# And finally the websites
	fill_websites "$pcap_path" "$tmpfile" <&${db[0]} >&${db[1]}
	
	# Updates the DMOZ database
#	update_dmoz <&${db[0]} > test.sql
	
	# Closes the database connection
	echo '\q' >&${db[1]}
	
	# Deletes the temporary file
	rm $tmpfile
	
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
	echo "INSERT INTO datasets VALUES (DEFAULT, '${pcap_name}', '${pcap_md5}', '${timestamp}'); \echo EOF"
	while read line
	do
		[[ "$line" == 'ERROR'* ]] && { echo '\q' ; error 'already_processed' "${pcap_name}" ; return $? ; }
		[[ "$line" == 'EOF' ]] && break
	done
	
	return 0
}

# Fills the flows
fill_flows() {
	
	# Local variables
	local pcap_path=$1 tmpfile=$2
	local line dataset_id
	local protocols protocol ids

	# Information
	echo -n 'Retrieving the dataset ID from the database... ' >&3
	
	# Gets the ID corresponding to the previous insert statement
	echo 'SELECT id FROM datasets ORDER BY added DESC LIMIT 1; \echo EOF'
	while read line
	do
		[[ "$line" != 'EOF' ]] && dataset_id="$line" || break
	done
	
	# Information
	echo -ne 'done!\nAnalysis of the flows of the PCAP in progress... ' >&3
	
	# Processes the PCAP file
	lpi_protoident "pcap:${pcap_path}" > $tmpfile 2>/dev/null
	
	# Information
	echo -ne 'done!\nInsertion of the new protocols in the database... ' >&3
	
	# Adds the protocols not already existing in the database
	echo 'CREATE TEMPORARY TABLE protocols_tmp (name VARCHAR(50) PRIMARY KEY);'
	echo 'BEGIN;'
	awk '!exists[$1]++ {print "INSERT INTO protocols_tmp VALUES (\47" tolower($1) "\47);"}' $tmpfile
	echo 'COMMIT;'
	echo 'INSERT INTO protocols (name) SELECT t.name FROM protocols_tmp t LEFT JOIN protocols p ON t.name = p.name WHERE p.name IS NULL;'
	
	# Information
	echo -ne 'done!\nInsertion of the flows in the database (this may take some time)... ' >&3
	
	# Insert in the database
	echo 'BEGIN;'
	awk -v dataset_id=${dataset_id} '{print "INSERT INTO flows VALUES (DEFAULT, " dataset_id ", NULL, (SELECT id FROM protocols WHERE name = \47" tolower($1) "\47), " $6 ", to_timestamp(" $7 "), \47" $2 "\47, \47" $3 "\47, " $4 ", " $5 ", " $8 ", " $9 ");"}' $tmpfile
	echo 'COMMIT;'
	
	# Information
	echo 'done!' >&3
	
	return 0
}

# Fills the table "website"
fill_websites() {
	
	# Local variables
	local pcap_path=$1 tmpfile=$2
	local pcap_name
	
	# Gets the basename of the PCAP
	pcap_name=$(basename ${pcap_path})
	
	# Creates a temporary database for the websites found in the PCAP
	cat <<- SQL
	CREATE TEMPORARY TABLE websites_analysis (
		id 			SERIAL			PRIMARY KEY,
		timestamp	TIMESTAMP		NOT NULL,
		endpoint_a	INET			NOT NULL,
		endpoint_b	INET			NOT NULL,
		url			VARCHAR(255)	NOT NULL
	);
	SQL
	
	# Information
	echo -n 'Analysis of the websites visited... ' >&3
	
	# Inserts in the websites in the temporary database
	echo 'BEGIN;'
	/usr/sbin/tcpdump -r ${pcap_path} -w - 'udp port 53' 2>/dev/null \
		| tshark -T fields -e frame.time_epoch -e dns.a -e ip.dst -e dns.qry.name -Y 'dns.flags.response eq 1' -r - \
		| awk '{if(NF != 4){next} ; $1 = substr($1, 1, 14) ; ip_nb = split($2, ip, ",") ; for(i = 1 ; i < ip_nb ; i++) {print "INSERT INTO websites_analysis VALUES (DEFAULT, to_timestamp(" $1 "), \47" ip[i] "\47, \47" $3 "\47, \47" $4 "\47);"}}'
	echo 'COMMIT;'
	
	# Creates a view to insert the necessary rows
	cat <<- SQL
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
	echo 'INSERT INTO urls (value) SELECT DISTINCT url_value FROM websites_view WHERE url_id IS NULL;'
	
	# Inserts the identified websites in the database
	# TODO
	
	# Updates the flows
	# TODO: To review
	# echo 'UPDATE flows SET website = v.website_id FROM websites_view v WHERE flows.id = v.flow_id;'
	
	# Information
	echo 'done!' >&3
	
	return 0
}

# Updates DMOZ categories & database
update_dmoz() {

	# Local variables
	local rdf
	
	# Creates the temporary tables for the update
	cat <<- SQL
	CREATE TEMPORARY TABLE dmoz_tmp (
		id			SERIAL			PRIMARY KEY,
		topic		VARCHAR(50)		NOT NULL,
		subtopic	VARCHAR(50)		NOT NULL,
		url			VARCHAR(255)	NOT NULL
	);
	SQL
	
	# Downloads the RDF databases
#	for rdf in ${DMOZ_RDF[@]}
#	do
#		wget -qO /tmp/${rdf}.rdf.u8.gz http://rdf.dmoz.org/rdf/${rdf}.rdf.u8.gz \
#			&& gunzip /tmp/${rdf}.rdf.u8.gz \
#			|| { error 'rdf_download' "http://rdf.dmoz.org/rdf/${rdf}.rdf.u8.gz" ; }
#	done
	
	# Inserts the DMOZ data in the database
	echo 'BEGIN;'
	for rdf in ${DMOZ_RDF[@]}
	do
		# Information
		echo -n "Now adding the dataset ${rdf}... " >&3
		
		# Adds the dataset
		sed -n 's/  <Topic r:id="\(Top\/\)\?\([^/]*\)\/\([^/]*\)">/\2 \3/p;s/    <link r:resource="https*:\/\/\([^/"]*\)\/".*/\1/p' /tmp/${rdf}.rdf.u8 \
			| awk '{if(NF == 2){if($1 == "Top"){next} ; gsub("\47", "_") ; gsub(",", "") ; topic=$1 ; subtopic=$2} else {print "INSERT INTO dmoz_tmp VALUES (DEFAULT, \47" tolower(topic) "\47, \47" tolower(subtopic) "\47, \47" $0 "\47);"}}'
		
		# Information
		echo 'done!' >&3
		
	done
	echo 'COMMIT;'
	
	# Fills the "topics" and "suptopics" tables
	echo 'INSERT INTO topics (name) SELECT DISTINCT d.topic FROM dmoz_tmp d LEFT JOIN topics t ON d.topic = t.name WHERE t.name IS NULL;'
	echo 'INSERT INTO subtopics (name) SELECT DISTINCT d.subtopic FROM dmoz_tmp d LEFT JOIN subtopics s ON d.subtopic = s.name WHERE s.name IS NULL;'
	
	# Fills the "categories" table
	cat <<- SQL
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
	echo 'INSERT INTO urls (value) SELECT DISTINCT d.url FROM dmoz_tmp d LEFT JOIN urls u ON d.url = u.value WHERE u.value IS NULL;'
	
	# Fills the "dmoz" table
	cat <<- SQL
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
	
	# Remove all the downloaded RDFs
#	rm /tmp/*.rdf.u8
	
	return 0

}
# Error handling
error() {

	local err=$1

	# Displays the error
	echo -n 'ERROR: ' >&2
	case $err in
		already_processed)
			echo "The dataset $2 has already been processed. Exiting..." >&2
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

# Launches the main function
main

# Exits with the correct exit code
exit $?
