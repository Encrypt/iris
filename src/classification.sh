# File: classification.sh
# Content: Functions dealing with the HTTP(S) flows classification

# Resets the classification
reset_classification() {

	info 'Resetting of the websites classification...' '0'

	exec_sql <<- 'SQL'
	UPDATE websites
	SET category = NULL, hand_classified = NULL
	WHERE hand_classified IS NOT NULL
		OR category IS NOT NULL;
	SQL
	
	info ' └─ Successfully reset the website categories!' '100'
	
	return 0
}

# Classifies the [new] websites (note that the order is important)
classify_websites() {
	
	classify_dmoz || return $?
	classify_ads || return $?
	classify_cdns || return $?
	
	return 0
}

# Classifies the website using the DMOZ categories
classify_dmoz() {

	# Local variables
	local filters extra_filter
	local db_entries

	info 'Classification of the websites using the DMOZ table...' '0'
	info ' ├─ Checking if the table exists and is not empty...' '0'
	
	# Check if the table exists and there are entries in it
	db_entries=$(exec_sql 'SELECT count(*) FROM dmoz;')
	[[ $db_entries -gt 0 ]] \
		|| { error 'no_entry' 'DMOZ' ; return $? ; }
	
	info ' ├─ Classification in progress...' '20'
	
	# Automatically try to set a category to the new websites using DMOZ
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
			WHERE w.hand_classified IS NULL
				OR w.category IS NULL
			${extra_filter}
		) s
		WHERE s.id = w.id;
		SQL
	done
	
	info ' └─ Classification successful!' '100'
	
	return 0
}

# Classifies the ads
classify_ads() {
	
	# Local variables
	local db_entries
	local ads_regexp='(%[-_.])?ad(s|vertising|server|content)?[0-9]*([-_.]%)?'
	
	info 'Classification of the websites using the ads table...' '0'
	info ' ├─ Checking if the table exists and is not empty...' '0'
	
	# Check if the table exists and there are entries in it
	db_entries=$(exec_sql 'SELECT count(*) FROM ads;')
	[[ $db_entries -gt 0 ]] \
		|| { error 'no_entry' 'ads' ; return $? ; }
	
	info ' ├─ Classification in progress...' '20'
	
	# Use the ads table to classify the websites
	exec_sql <<- 'SQL'
	UPDATE websites w
	SET category = s.category, hand_classified = FALSE
	FROM (
		SELECT w.id, a.category
		FROM websites w
		JOIN ads a ON w.url = a.url
		JOIN categories c ON a.category = c.id
		WHERE w.hand_classified IS NULL
			OR w.category IS NULL
	) s
	WHERE s.id = w.id;
	SQL
	
	info '' '60'
	
	# Use the URL, with the regexp
	exec_sql <<- SQL
	UPDATE websites
	SET category = (
		SELECT id FROM categories
		WHERE topic = (
			SELECT id FROM topics
			WHERE name = 'ads')),
		hand_classified = FALSE
	WHERE id IN (
		SELECT w.id
		FROM websites w
		JOIN urls u ON w.url = u.id
		WHERE u.value SIMILAR TO '${ads_regexp}'
			AND (w.hand_classified IS NULL
				OR w.category IS NULL)
	);
	SQL
	
	info ' └─ Classification successful!' '100'
	
	return 0
}

# Classifies the CDNs
classify_cdns() {

	# Local variables
	local db_entries
	local cdn_regexp='(%[-_.])?cdns?[0-9]*([-_.]%)?'

	info 'Classification of the websites using the cdns table...' '0'
	info ' ├─ Checking if the table exists and is not empty...' '0'
	
	# Check if the table exists and there are entries in it
	db_entries=$(exec_sql 'SELECT count(*) FROM cdns;')
	[[ $db_entries -gt 0 ]] \
		|| { error 'no_entry' 'cdns' ; return $? ; }
	
	info ' ├─ Classification in progress...' '20'
	
	# Use the cdns table to classify the websites
	exec_sql <<- 'SQL'
	UPDATE websites
	SET category = (
		SELECT id FROM categories
		WHERE topic = (
			SELECT id FROM topics
			WHERE name = 'cdn')),
		hand_classified = FALSE
	WHERE id IN (
		SELECT w.id
		FROM websites w
		JOIN urls u ON w.url = u.id
		INNER JOIN (SELECT domain FROM cdns) c
			ON u.value LIKE '%' || c.domain || '%'
		WHERE w.hand_classified IS NULL
			OR w.category IS NULL
	);
	SQL
	
	info '' '60'
	
	# Use the URL, with the regexp
	exec_sql <<- SQL
	UPDATE websites
	SET category = (
		SELECT id FROM categories
		WHERE topic = (
			SELECT id FROM topics
			WHERE name = 'cdn')),
		hand_classified = FALSE
	WHERE id IN (
		SELECT w.id
		FROM websites w
		JOIN urls u ON w.url = u.id
		WHERE u.value SIMILAR TO '${cdn_regexp}'
			AND (w.hand_classified IS NULL
				OR w.category IS NULL)
	);
	SQL
	
	info ' └─ Classification successful!' '100'
	
	return 0
}
