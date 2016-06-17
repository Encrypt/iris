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
	local proj_file tmpfile
	
	# Sources the files of the project
	for proj_file in $(find ./src/ -name "*.sh")
	do
		source "$proj_file"
	done
	
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
			fill_dataset "$pcap_path" || return $?
			
			# Fills the flows table
			fill_flows "$pcap_path" || return $?
			
			# And finally the websites
			fill_websites "$pcap_path" || return $?
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
	
	return 0
}

# Launches the main function
main

# Exits with the correct exit code
exit $?
