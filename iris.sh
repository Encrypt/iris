#!/bin/bash

# IRIS - A script achieving user profiling based on network activity
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
	
	# Processes the dataset
	process_dataset "$pcap_path"
	
	return $?
}

# Processes a new dataset
process_dataset() {
	
	# Local variables
	local pcap_path=$1
	local pcap_name pcap_md5 timestamp
	local protocols protocol ids tmpfile
	
	# Connects to the database
	coproc db { psql -Atnq -U ${PSQL_USER} -d ${PSQL_DATABASE} 2>&1 ; }
	
	# Gets information about the dataset
	pcap_md5=$(head -c 100M ${pcap_path} | md5sum | cut -c1-32)
	pcap_name=$(basename ${pcap_path})
	timestamp=$(date '+%F %T')
	
	# Inserts the new dataset in the database if has not already been added
	echo "INSERT INTO datasets VALUES (DEFAULT, '${pcap_name}', '${pcap_md5}', '${timestamp}'); \echo EOF" >&${db[1]}
	while read line
	do
		[[ "$line" == 'ERROR'* ]] && { echo '\q' >&${db[1]} ; return $(error 'already_processed' "${pcap_name}") ; }
		[[ "$line" == 'EOF' ]] && break
	done <&${db[0]}
	
	# Gets the ID corresponding to the previous insert statement
	echo 'SELECT id FROM datasets ORDER BY added DESC LIMIT 1; \echo EOF' >&${db[1]}
	while read line
	do
		[[ "$line" != 'EOF' ]] && dataset_id="$line" || break
	done <&${db[0]}
	
	# Gets the protocols already in the database
	protocols=','
	echo 'SELECT name FROM protocols; \echo EOF' >&${db[1]}
	while read line
	do
		[[ "$line" != 'EOF' ]] && protocols+="${line}," || break
	done <&${db[0]}
	
	# Processes the PCAP file
	tmpfile=$(mktemp)
	lpi_protoident "pcap:${pcap_path}" > $tmpfile
	
	# Adds the protocols not already in the database
	for protocol in $(cut -f 1 -d ' ' $tmpfile | sort | uniq)
	do
	
		if [[ "$protocols" != *",${protocol,,},"* ]]
		then
		
			# Adds the appropriate row
			echo "INSERT INTO protocols VALUES (DEFAULT, '${protocol,,}');" >&${db[1]}
			
			# Adds it in the protocols variable
			protocols+="${protocol,,},"
		
		fi
	done
	
	echo 'BEGIN;' >&${db[1]}
	awk -v dataset_id=${dataset_id} '{print "INSERT INTO flows VALUES (DEFAULT, " dataset_id ", NULL, (SELECT id FROM protocols WHERE name = \47" tolower($1) "\47), " $6 ", to_timestamp(" $7 "), \47" $2 "\47, \47" $3 "\47, " $4 ", " $5 ", " $8 ", " $9 ");"}' $tmpfile >&${db[1]}
	echo 'COMMIT;' >&${db[1]}
	
	# Closes the database connection
	echo '\q' >&${db[1]}
	
	# Deletes the temporary file
	rm $tmpfile
	
	# Launches the website analysis (TODO)
	# fill_website &
	
	return 0
}

# Fills the table "website"
fill_website() {
	
	# To be done...
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
