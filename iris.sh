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
	fill_dataset "$pcap_path"
	
	# Creates a temporary file to store the results of libprotoident / ngrep
	tmpfile=$(mktemp)
	
	# Fills the flows table
	fill_flows "$pcap_path" "$tmpfile"
	
	# And finally the websites
	fill_websites "$pcap_path" "$tmpfile"
	
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
	echo 'CREATE TEMPORARY TABLE protocols_tmp (name VARCHAR(50) PRIMARY KEY);' >&${db[1]}
	echo 'BEGIN;' >&${db[1]}
	cut -d ' ' -f 1 $tmpfile | sort | uniq | awk '{print "INSERT INTO protocols_tmp VALUES (\47" tolower($1) "\47);"}' >&${db[1]}
	echo 'COMMIT;' >&${db[1]}
	echo 'INSERT INTO protocols (name) SELECT t.name FROM protocols_tmp t LEFT JOIN protocols p ON t.name = p.name WHERE p.name IS NULL;' >&${db[1]}
	
	# Information
	echo -ne 'done!\nInsertion of the flows in the database (this may take some time)... '
	
	# Insert in the database
	echo 'BEGIN;' >&${db[1]}
	awk -v dataset_id=${dataset_id} '{print "INSERT INTO flows VALUES (DEFAULT, " dataset_id ", NULL, (SELECT id FROM protocols WHERE name = \47" tolower($1) "\47), " $6 ", to_timestamp(" $7 "), \47" $2 "\47, \47" $3 "\47, " $4 ", " $5 ", " $8 ", " $9 ");"}' $tmpfile >&${db[1]}
	echo 'COMMIT;' >&${db[1]}
	
	# Information
	echo 'done!'
	
	return 0
}

# Fills the table "website"
fill_websites() {
	
	# Local variables
	local pcap_path=$1 tmpfile=$2
	
	# Information
	echo -n 'Analysis of the websites visited... '

	# Processes the entries containing GETs / POSTs
	ngrep -tqI "${pcap_path}" -W single -P '#' '^(GET|POST)' | awk '/Host: [a-zA-Z0-9]/{sub(/\[.*#Host:/,"") ; gsub("/", "-", $2) ; $3 = substr($3, 1, 12) ; sub("#.*", "", $7) ; split($6, end_a, ":") ; split($4, end_b, ":") ; print $2, $3, end_a[1], end_a[2], end_b[1], end_b[2], $7}' > $tmpfile
	
	# Information
	echo -ne 'done!\nInsertion of the websites in the database... '
	
	# Creates a temporary database for the websites found in the PCAP
	cat <<- SQL >&${db[1]}
	CREATE TEMPORARY TABLE websites_tmp (
		id 			SERIAL			PRIMARY KEY,
		timestamp	TIMESTAMP		NOT NULL,
		endpoint_a	INET			NOT NULL,
		endpoint_b	INET			NOT NULL,
		port_a		INT				NOT NULL,
		port_b		INT				NOT NULL,
		url			VARCHAR(255)	NOT NULL
	);
	SQL
	
	# Insert in the database
	echo 'BEGIN;' >&${db[1]}
	awk '{print "INSERT INTO websites_tmp VALUES (DEFAULT, \47"$1, $2 "\47, \47" $3 "\47, \47" $5 "\47, \47" $4 "\47, \47" $6 "\47, \47" $7 "\47);"}' $tmpfile >&${db[1]}
	echo 'COMMIT;' >&${db[1]}
	
	# Insert in the table "websites" the URLs of "websites_tmp" which do not exist
	echo 'INSERT INTO websites (url) SELECT DISTINCT t.url FROM websites_tmp t LEFT JOIN websites w ON t.url = w.url WHERE w.url IS NULL;' >&${db[1]}
	
	# Update the flows
	cat <<- SQL >&${db[1]}
	UPDATE flows
	SET website = q.website_id
	FROM (
		SELECT DISTINCT s.id as flow_id, w.id as website_id FROM websites_tmp t
		JOIN websites w ON w.url = t.url,
		LATERAL (
			SELECT f.id
			FROM flows f
			WHERE f.dataset = (SELECT id FROM datasets where name = '${pcap_name}')
				AND f.protocol = (SELECT id FROM protocols where name = 'http')
				AND f.timestamp >= t.timestamp - interval '1 minute'
				AND f.timestamp < t.timestamp + interval '1 minute'
				AND f.endpoint_a = t.endpoint_a
				AND f.endpoint_b = t.endpoint_b
				AND f.port_a = t.port_a
				AND f.port_b = t.port_b
			ORDER BY f.timestamp
			ASC LIMIT 1) s
		) q
	WHERE flows.id = q.flow_id;
	SQL
	
	# Information
	echo 'done!'
	
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
