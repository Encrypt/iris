# File: ui.sh
# Content: User interface, called when there is no argument

# Main menu: displays all the options
menu_main() {
	
	# Local variables
	local option
	local exit_menu=0
	
	# Loops on the main menu
	while [[ "$option" != 'exit' ]]
	do
	
		# Displays the menu
		option=$(whiptail --notags --nocancel --title 'IRIS -- Main Menu' --menu '\nSelect an option to go further.' 14 78 5 \
			'analyse' 'Analyse a new PCAP' \
			'classify' 'Classify the website entries' \
			'update' 'Update a classifier' \
			'help' 'Display the help' \
			'exit' 'Exit IRIS' \
		3>&1 1>&2 2>&3)
	
		# Goes to the corresponding menu
		case "$option" in
			analyse)
				menu_analyse || return $?
				;;
			classify)
				menu_classify || return $?
				;;
			help)
				menu_help || return $?
				;;			
			update)
				menu_update || return $?
				;;
		esac
		
	done
	
	return 0
}

# Analyses a PCAP
menu_analyse() {

	# Local variables
	local pcap_file
	
	# Sets the first path as the current directory
	pcap_file="$(pwd)"
	
	# While the path is incorrect, displays the input box
	while [[ ! -f "${pcap_file}" ]]
	do
	
		# Displays the input box
		pcap_file=$(whiptail --title 'IRIS -- Analyse' --inputbox '\nPlease enter the path of the PCAP to analyse...' 9 78 "$pcap_file" 3>&1 1>&2 2>&3)
		
		# If the pcap_file path is empty, "Cancel" was selected
		[[ -z "${pcap_file}" ]] \
			&& return 0
		
		# Checks if the file exists
		[[ ! -f "${pcap_file}" ]] \
			&& whiptail --title 'IRIS -- Analyse' --msgbox "ERROR: The file ${pcap_file} doesn't exist." 8 78
	done
	
	# Use a gauge to indicate progress
	{
	
		# Opens the database connection
		coproc db { psql -Atnq -U ${PSQL_USER} -d ${PSQL_DATABASE} 2>&1 ; }
		
		# Processes the file
		fill_dataset "${pcap_file}" || return $?

		# Fills the flows table
		fill_flows "${pcap_file}" || return $?

		# Fills the websites
		fill_websites "${pcap_file}" || return $?

		# Tries to classify the new websites
		classify_websites || return $?
		
		# Closes the database connection
		echo '\q' >&${db[1]}
	
	} \
		| awk '{if(lag < $0){sum = sum - lag + $0} ; lag = $0 ; print int(sum/6) ; fflush()}' \
		| whiptail --gauge 'Analysis in progress...' 6 50 0
	
	return 0
}

# Classifies the websites
menu_classify() {

	# Local variables
	local option gauge_max

	# Displays the menu
	option=$(whiptail --notags --title 'IRIS -- Classify' --menu '\nWhat do ou want to do?' 13 78 4 \
		'classify' 'Classify the unclassified websites' \
		'reclassify' 'Reclassify all the websites' \
	3>&1 1>&2 2>&3)
	
	# Sets the max value
	[[ "$option" == 'classify' ]] \
		&& gauge_max=200 \
		|| gauge_max=300
	
	# Use a gauge to indicate progress
	{
		
		# Does the corresponding action
		case "$option" in
			
			# Opens the database connection
			classify|reclassify)
				coproc db { psql -Atnq -U ${PSQL_USER} -d ${PSQL_DATABASE} 2>&1 ; }
				;;&
			
			# Classifies
			classify)
				classify_websites || return $?
				;;&
			reclassify)
				reset_classification || return $?
				classify_websites || return $?
				;;&
			
			# Closes the database connection
			classify|reclassify)
				echo '\q' >&${db[1]}
				;;&
		esac
	
	} \
		| awk -v gauge_max=${gauge_max} '{if(lag < $0){sum = sum - lag + $0} ; lag = $0 ; print int(sum/gauge_max*100) ; fflush()}' \
		| whiptail --gauge 'Classification in progress...' 6 50 0
	
	return 0
}

# Updates a classifier
menu_update() {

	# Local variables
	local options option step
	local cdn_file
	
	# Displays the menu
	options=$(whiptail --notags --separate-output --title 'IRIS -- Update' --checklist '\nChoose the classifier(s) that you wish to update.' 11 78 3 \
		'ads' 'ADs  -- Using the preconfigured lists' OFF \
		'dmoz' 'DMOZ -- Using the official RDFs dumps' OFF \
		'cdns' 'CDNs -- Using a domain list file' OFF \
	3>&1 1>&2 2>&3)
	
	# Update the chosen datasets
	step=1
	for option in "${options[@]}"
	do
	
		# Updates the chosen table
		case "$option" in
			ads)
				{
					coproc db { psql -Atnq -U ${PSQL_USER} -d ${PSQL_DATABASE} 2>&1 ; }
					update_ads || return $?
					echo '\q' >&${db[1]}
				} | whiptail --gauge "Step ${step}/${#options[@]}: Update of the ads table..." 6 50 0
				;;&
			
			dmoz)
				{
					coproc db { psql -Atnq -U ${PSQL_USER} -d ${PSQL_DATABASE} 2>&1 ; }
					update_dmoz || return $?
					echo '\q' >&${db[1]}
				} | whiptail --gauge "Step ${step}/${#options[@]}: Update of the dmoz table..." 6 50 0
				;;&
			
			cdns)
				
				# Sets the first path as the current directory
				cdn_file="$(pwd)"
	
				# While the path is incorrect, displays the input box
				while [[ ! -f "${cdn_file}" ]]
				do
	
					# Displays the input box
					cdn_file=$(whiptail --title 'IRIS -- Update' --inputbox '\nPlease enter the path of the CDN file to use...' 9 78 "$cdn_file" 3>&1 1>&2 2>&3)
		
					# If the cdn_file path is empty, "Cancel" was selected
					[[ -z "${cdn_file}" ]] \
						&& return 0
		
					# Checks if the file exists
					[[ ! -f "${cdn_file}" ]] \
						&& whiptail --title 'IRIS -- Update' --msgbox "ERROR: The file ${pcap_file} doesn't exist." 8 78
				done
				
				{
					coproc db { psql -Atnq -U ${PSQL_USER} -d ${PSQL_DATABASE} 2>&1 ; }
					update_cdns || return $?
					echo '\q' >&${db[1]}
				} | whiptail --gauge "Step ${step}/${#options[@]}: Update of the cdns table..." 6 50 0
				;;&
			
			# Increment the step for the next operation
			*)
				step=$((${step} + 1))
				;;
		
		esac
	done
	
	return 0
}

# Displays the help
menu_help() {
	
	# Local variable
	local help_page
	
	# Reads the help
	while read line
	do
		help_page+="${line}\n"
	done < <(help)
	
	# Dialog
	whiptail --scrolltext --title 'IRIS -- Help' --msgbox "${help_page}" 15 78
	
	return 0
}