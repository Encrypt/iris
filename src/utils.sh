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