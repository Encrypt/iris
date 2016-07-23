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
	local pcap_path proj_file
	
	# Sources the files of the project
	for proj_file in $(find ./src/ -name "*.sh")
	do
		source "$proj_file"
	done
	
	# If there is no argument, use the UI
	if [[ ${ARGS_NB} -eq 0 ]]
	then
		menu_main \
			&& return 0 \
			|| return $?
	
	# Else, gets the command given
	else
	
		case ${ARGS[0]} in
		
			# If the command implies the database...
			analyse|update|stats|classify|reclassify)
		
				# ... open a connection
				coproc db { psql -Atnq -U ${PSQL_USER} -d ${PSQL_DATABASE} 2>&1 ; }
				;;&
		
			# Analyses a dataset
			analyse)
			
				# Tests if the file exists on the disk
				[[ -f "${ARGS[1]}" ]] || { error 'file_doesnt_exist' "${ARGS[1]}" ; return $? ; }
			
				# Processes the dataset
				fill_dataset "${ARGS[1]}" || return $?
			
				# Fills the flows table
				fill_flows "${ARGS[1]}" || return $?
			
				# Fills the websites
				fill_websites "${ARGS[1]}" || return $?
			
				# Tries to classify the new websites
				classify_websites || return $?
				;;&
		
			# Updates the classifier given as argument
			update)
		
				case ${ARGS[1]} in
			
					# Updates the DMOZ table
					dmoz)
						update_dmoz || return $?
						;;
				
					# Updates the ads table
					ads)
						update_ads || return $?
						;;
				
					# Updates the cdns table with the file given as argument
					cdns)
						update_cdns "${ARGS[2]}" || return $?
						;;
			
					# Unknown option
					*)
						error 'unknown_argument' "${ARGS[1]}"
						return $?
						;;
			
				esac
				;;&
			
			# Stats on an IP address
			stats)
				
				# Checks if the IP is a correct IPv4 one
				[[ "${ARGS[1]}" =~ [0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3} ]] \
					|| { error 'ip_doesnt_exist' "${ARGS[1]}" ; return $? ; }
				
				# Processes the stats
				process_stats "${ARGS[1]}" || return $?
				;;&
			
			# Classifies the websites
			classify)
				classify_websites || return $?
				;;&
		
			# Reclassifies the websites
			reclassify)
				reset_classification || return $?
				classify_websites || return $?
				;;&
		
			# Closes the database connection
			analyse|update|stats|classify|reclassify)
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
	fi
	
	return 0
}

# Launches the main function
main

# Exits with the correct exit code
exit $?
