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
	fill_dataset "$pcap_path" <&${db[0]} >&${db[1]}
	
	# Creates a temporary file to store the results of libprotoident / ngrep
	tmpfile=$(mktemp)
	
	# Use another file descriptor for information
	exec 3>&1
	
	# Fills the flows table
	fill_flows "$pcap_path" "$tmpfile" <&${db[0]} >&${db[1]}
	
	# And finally the websites
	fill_websites "$pcap_path" "$tmpfile" <&${db[0]} >&${db[1]}
	
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
	CREATE TEMPORARY TABLE websites_tmp (
		id 			SERIAL			PRIMARY KEY,
		timestamp	TIMESTAMP		NOT NULL,
		endpoint_a	INET			NOT NULL,
		url			VARCHAR(255)	NOT NULL
	);
	SQL
	
	# Information
	echo -n 'Analysis of the websites visited... ' >&3
	
	# Inserts in the websites in the temporary database
	echo 'BEGIN;'
	/usr/sbin/tcpdump -r ${pcap_path} -w - 'udp port 53' 2>/dev/null \
		| tshark -T fields -e frame.time_epoch -e dns.a -e dns.qry.name -Y 'dns.flags.response eq 1' -r - \
		| awk '{$1 = substr($1, 1, 14) ; ip_nb = split($2, ip, ",") ; for(i = 1 ; i < ip_nb ; i++) {print "INSERT INTO websites_tmp VALUES (DEFAULT, to_timestamp(" $1 "), \47" ip[i] "\47, \47" $3 "\47);"}}'
	echo 'COMMIT;'
	
	# Inserts in the table "websites" the URLs of "websites_tmp" which do not exist
	echo 'INSERT INTO websites (url) SELECT DISTINCT t.url FROM websites_tmp t LEFT JOIN websites w ON t.url = w.url WHERE w.url IS NULL;'
	
	# Updates the flows
	cat <<- SQL
	UPDATE flows
	SET website = s.website_id
	FROM (
		SELECT DISTINCT ON (f.id) f.id AS flow_id, w.id AS website_id, (f.timestamp - t.timestamp) AS tstamp_diff
		FROM flows f
		JOIN websites_tmp t ON f.endpoint_a = t.endpoint_a
		JOIN websites w ON t.url = w.url
		WHERE f.dataset = (SELECT id FROM datasets WHERE name = '${pcap_name}' ORDER BY added DESC LIMIT 1)
			AND f.protocol IN (SELECT id FROM protocols WHERE name = 'http' OR name = 'https')
			AND f.timestamp BETWEEN t.timestamp AND t.timestamp + interval '15 minutes'
		ORDER BY f.id, tstamp_diff ASC
		) s
	WHERE flows.id = s.flow_id;
	SQL
	
	# Information
	echo 'done!' >&3
	
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
