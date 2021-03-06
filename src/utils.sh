# File: utils.sh
# Content: Functions for SQL execution, error handling and help

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
		
			# If we read "EOF", then that is the end of the results
			EOF)
				break
				;;
			
			# If it is an error, displays the error
			ERROR:*|DETAIL:*)
				error 'database_error' "$line"
				return $?
				;;
			
			# If it is something else, echoes the result
			*)
				echo "$line"
				;;
		esac
	done <&${db[0]}
	
	return 0
}

# Information formating
info() {
	
	# Local variables
	local message=$1
	local progress=$2
	local line
	
	# If at least one argument is given, process it
	if [[ $# -gt 0 ]]
	then
	
		# The message exists and UI is not used, echoes it
		[[ -n "$message" && ${ARGS_NB} -gt 0 ]] \
			&& echo "INFO: ${message}"
	
		# If the progress exists and the UI is used, echoes it
		[[ -n "$progress" && ${ARGS_NB} -eq 0 ]] \
			&& echo "$progress"
	
	# Else if the UI is unused and there is no argument, read the here-document
	elif [[ $# -eq 0 && ${ARGS_NB} -gt 0 ]]
	then
		while read line
		do
			echo "$line"
		done
	fi
	
	return 1
}

# Error handling
error() {

	local err=$1

	# Displays the error
	echo -n 'ERROR: ' >&2
	case $err in
		unknown_argument)
			echo "Unknown argument $2. Run \"${PROGNAME} help\" for further help." >&2
			;;
		already_processed)
			echo "The dataset $2 has already been processed." >&2
			;;
		database_error)
			echo "The database returned the following error during a statement execution: \"$2\"" >&2
			;;
		ip_doesnt_exist)
			echo "The IP $2 is either wrong or doesn't exist in the database." >&2
			;;
		file_doesnt_exist)
			echo "The file $2 doesn't not exist on the disk." >&2
			;;
		rdf_download)
			echo "Could not download the RDF: $2." >&2
			;;
		ads_download)
			echo "Could not download the ads dataset at address $2." >&2
			;;
		no_entry)
			echo -n "The table \"$2\" doesn't contain any entry. " >&2
			echo 'Please update that table before running the classification again.' >&2
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
	Usage: ${PROGNAME} <operation> <arguments>
	
	Operations and arguments:
	  analyse <pcap_path>: Analyses the PCAP given as argument.
	  update [ dmoz | ads | cnds <file> ]: Updates the database given as argument.
	  stats <ip_address>: Processes the statistics of the IP given as argument.
	  classify: Uses the classification databases to classify the unclassified websites.
	  reclassify: Does the same as classify, but resets the classification first.
	  help: Displays this help.
	
	With no operation given, a whiptail UI will appear.
	
	Hints:
	  To analyse a PCAP, you will probably run: ./${PROGNAME} analyse /home/me/capture.pcap
	  Please make sure that the database configuration is correct before running ${PROGNAME}.
	EOF
	
	return 0
}
