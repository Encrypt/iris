# File: statistics.sh
# Content: Functions to do statistics on the data

# Checks that the IP is correct
check_ip() {

	# Local variables
	local ip=$1
	local value
	
	info "Checking if the IP $ip is correct..." '0'
	
	# Checks against a regexp
	[[ "$ip" =~ [0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3} ]] \
		|| { error 'ip_doesnt_exist' "$ip" ; return $? ; }
	
	# Checks the value between the dots (0 <= value <= 255)
	for value in $(echo "$ip" | sed 's/\./\n/g')
	do
		[[ $value -ge 0 && $value -le 255 ]] \
			|| { error 'ip_doesnt_exist' "$ip" ; return $? ; }
	done
	
	info ' └─ IP correct!' '100'
	
	return 0
}

# Cleans the database "stats" table ans "stats" folder
prepare_stats_clean() {
	
	# Local variables
	local iris_folder="${BASH_SOURCE[0]%/src/*}"
	
	info 'Cleaning the stats table...' '0'
	
	# Cleans the stats table
	exec_sql 'DELETE FROM stats;'
	
	info ' ├─ Deleting the old stats...' '50'
	
	# Cleans the stats folder
	[[ -e "${iris_folder}/stats" ]] \
		&& rm -rf "${iris_folder}/stats"
	mkdir "${iris_folder}/stats"
	
	info ' └─ Cleaning sucessful!' '100'
	
	return 0
}

# Generates the stats report for a given user with R
generate_user_report() {

	# Local variables
	local ip=$1
	local iris_folder="${BASH_SOURCE[0]%/src/*}"
	
	info "Generation of the report for user $ip..." '0'
	
	# Calls R
	R --slave --no-restore --no-save --file="${iris_folder}/src/user_stats.r" --args "$ip" &> /dev/null
	
	# Moves the PDF to the stats folder
	mv 'iris_stats.pdf' "${iris_folder}/stats/$(echo $ip | sed 's/\./-/g').pdf"
	
	info ' └─ Report created!' '100'
	
	return 0
}

# Generates the stats report for the network with R
generate_global_report() {
	
	# Local variables
	local iris_folder="${BASH_SOURCE[0]%/src/*}"
	
	info 'Generation of the global report...' '0'
	
	# Ideas: network usage, user clustering
	R --slave --no-restore --no-save --file="${iris_folder}/src/global_stats.r" &> /dev/null
	
	# Moves the PDF to the stats folder
	mv 'iris_stats.pdf' "${iris_folder}/stats/overview.pdf"
	
	info ' └─ Report created!' '100'
	
	return 0
}

# Extracts stats from the database
fill_stats_table() {

	# Local variables
	local arg_ip=$1
	local ip_exists
	
	info "Checking if the IP $ip exists in the database..." '0'
	
	# Checks if there is at least a row with the given IP
	ip_exists=$(exec_sql "SELECT exists(SELECT 1 FROM flows WHERE endpoint_a = '${arg_ip}' OR endpoint_b = '${arg_ip}');")
	
	if [[ "${ip_exists}" == 'f' ]]
	then
		echo '\q' >&${db[1]}
		error 'ip_doesnt_exist' "${arg_ip}"
		return $?
	fi
	
	info ' ├─ Getting the days of activity...' '10'
	
	# Adds the days of activity
	exec_sql <<- SQL
	INSERT INTO stats (ip, day)
	SELECT '${arg_ip}'::INET AS ip,
		generate_series(min(bgn_tstp)::DATE, max(end_tstp)::DATE, '1 day'::INTERVAL)::DATE AS day
	FROM (
		SELECT timestamp AS bgn_tstp,
			(timestamp + (duration / 1000000.0) * INTERVAL '1 second') AS end_tstp
		FROM flows
		WHERE (endpoint_b = '${arg_ip}' OR endpoint_a = '${arg_ip}')
	) q;
	SQL
	
	info ' ├─ Getting the number of incoming flows...' '19'
	
	# Number of flows in
	exec_sql <<- SQL
	UPDATE stats s
	SET flows_in = q.flows_in
	FROM (
		SELECT s.day,
			coalesce(s1.flows_in, 0) AS flows_in
		FROM stats s
		LEFT JOIN (
			SELECT timestamp::DATE AS day,
				count(*) AS flows_in
			FROM flows f
			WHERE f.endpoint_b = '${arg_ip}'
			GROUP BY day
		) s1
		ON s.day = s1.day
		WHERE ip = '${arg_ip}'
	) q
	WHERE s.ip = '${arg_ip}'
		AND s.day = q.day;
	SQL
	
	info ' ├─ Getting the number of outgoing flows...' '28'
	
	# Number of flows out
	exec_sql <<- SQL
	UPDATE stats s
	SET flows_out = q.flows_out
	FROM (
		SELECT s.day,
			coalesce(s1.flows_out, 0) AS flows_out
		FROM stats s
		LEFT JOIN (
			SELECT timestamp::DATE AS day,
				count(*) AS flows_out
			FROM flows f
			WHERE f.endpoint_a = '${arg_ip}'
			GROUP BY day
		) s1
		ON s.day = s1.day
		WHERE ip = '${arg_ip}'
	) q
	WHERE s.ip = '${arg_ip}'
		AND s.day = q.day;
	SQL
	
	info ' ├─ Getting the number of flows per 30 minutes...' '37'
	
	# Number of flows per 30 minutes (in and out)
	exec_sql <<- SQL
	UPDATE stats s
	SET flows_thirty_mins = q.flows_thirty_mins
	FROM (
		SELECT q2.day AS day,
			array_agg(q2.flows_number) AS flows_thirty_mins
		FROM (
			SELECT s2.time_unit::DATE AS day,
				coalesce(q1.flows_number, 0) AS flows_number
			FROM (
				SELECT generate_series(start_day, end_day + INTERVAL '23 hours 30 minutes', '30 minutes'::INTERVAL) AS time_unit
				FROM (
					SELECT min(day) AS start_day, 
						max(day) AS end_day
					FROM stats
					WHERE ip = '${arg_ip}'
				) s1
			) s2
			LEFT JOIN (
				SELECT date_trunc('hour', timestamp) + INTERVAL '30 minutes' * floor(date_part('minute', timestamp) / 30.0) AS round_timestamp,
					count(*) AS flows_number
				FROM flows
				WHERE endpoint_a = '${arg_ip}'
					OR endpoint_b = '${arg_ip}'
				GROUP BY round_timestamp
			) q1
			ON s2.time_unit = q1.round_timestamp
			ORDER BY s2.time_unit
		) q2
		GROUP BY q2.day
	) q 
	WHERE s.ip = '${arg_ip}'
		AND s.day = q.day;
	SQL
	
	info ' ├─ Getting the number unique websites visited...' '46'
	
	# Number of unique websites visited (which are not CDNs or ads)
	exec_sql <<- SQL
	UPDATE stats s
	SET websites_visited = q.websites_visited
	FROM (
		SELECT timestamp::DATE AS day,
			coalesce(count(DISTINCT w.id), 0) AS websites_visited
		FROM flows f
		JOIN websites w ON f.website = w.id
		WHERE f.protocol IN (
				SELECT id FROM protocols
				WHERE name IN ('http', 'https')
			)
			AND w.category NOT IN (
				SELECT id FROM categories
				WHERE topic IN (
					SELECT id FROM topics
					WHERE name IN ('ads', 'cdn')
				)
			)
			AND f.endpoint_b = '${arg_ip}'
		GROUP BY day
	) q
	WHERE s.ip = '${arg_ip}'
		AND s.day = q.day;
	SQL
	
	info ' ├─ Getting the number of packets / payload size...' '55'
	
	# Number of packets / payload size
	exec_sql <<- SQL
	UPDATE stats s
	SET packets_in = q.packets_in,
		packets_out = q.packets_out,
		payload_size_in = q.payload_size_in,
		payload_size_out = q.payload_size_out
	FROM (
		SELECT day,
			sum(packets_in) AS packets_in,
			sum(packets_out) AS packets_out,
			sum(payload_size_in) AS payload_size_in,
			sum(payload_size_out) AS payload_size_out
		FROM (
			SELECT timestamp::DATE AS day,
				payload_size_ba AS payload_size_in,
				payload_size_ab AS payload_size_out,
				packets_nb_ba AS packets_in,
				packets_nb_ab AS packets_out
			FROM flows
			WHERE endpoint_a = '${arg_ip}'
			UNION ALL SELECT timestamp::DATE AS day,
				payload_size_ab AS payload_size_in,
				payload_size_ba AS payload_size_out,
				packets_nb_ab AS packets_in,
				packets_nb_ba AS packets_out
			FROM flows
			WHERE endpoint_b = '${arg_ip}'
		) q1
		GROUP BY day
	) q
	WHERE s.ip = '${arg_ip}'
		AND s.day = q.day;
	SQL
	
	info ' ├─ Getting the browsing probabilities...' '64'
	
	# Browsing probabilities
	exec_sql <<- SQL
	UPDATE stats s
	SET browsing_proba_bb = q.p_bb,
		browsing_proba_bi = q.p_bi,
		browsing_proba_ib = q.p_ib,
		browsing_proba_ii = q.p_ii
	FROM (
		SELECT day,
			sum_bool_bb/(sum_bool_bb + sum_bool_bi)::FLOAT AS p_bb,
			sum_bool_bi/(sum_bool_bb + sum_bool_bi)::FLOAT AS p_bi,
			sum_bool_ib/(sum_bool_ii + sum_bool_ib)::FLOAT AS p_ib,
			sum_bool_ii/(sum_bool_ii + sum_bool_ib)::FLOAT AS p_ii
		FROM (
			SELECT day,
				sum(CASE WHEN bool_bb THEN 1 ELSE 0 END) AS sum_bool_bb,
				sum(CASE WHEN bool_bi THEN 1 ELSE 0 END) AS sum_bool_bi,
				sum(CASE WHEN bool_ib THEN 1 ELSE 0 END) AS sum_bool_ib,
				sum(CASE WHEN bool_ii THEN 1 ELSE 0 END) AS sum_bool_ii
			FROM (
				SELECT
					time::DATE AS day,
					lag_browsing AND browsing AS bool_bb,
					lag_browsing AND NOT browsing AS bool_bi,
					NOT lag_browsing AND browsing AS bool_ib,
					NOT lag_browsing AND NOT browsing AS bool_ii
				FROM (
					SELECT s2.time_unit AS time, coalesce(q6.browsing, FALSE) AS browsing, coalesce(lag(q6.browsing) OVER (ORDER BY s2.time_unit), FALSE) AS lag_browsing
					FROM (
						SELECT generate_series(start_day, end_day + INTERVAL '23 hours 59 minutes', '1 minute'::INTERVAL) AS time_unit
						FROM (
							SELECT min(day) AS start_day, 
								max(day) AS end_day
							FROM stats
							WHERE ip = '${arg_ip}'
						) s1
					) s2
					LEFT JOIN (
						SELECT generate_series(session_start, session_end, '1 minute'::INTERVAL) AS session_unit, TRUE as browsing
						FROM (
							SELECT min(q4.start_time) AS session_start, max(q4.end_time) AS session_end
							FROM (
								SELECT q3.start_time, q3.end_time, max(q3.new_start) OVER (ORDER BY q3.start_time, q3.end_time) AS left_edge
								FROM (
									SELECT q2.start_time, q2.end_time,
										CASE WHEN q2.start_time <= max(q2.lag_end_time + INTERVAL '1 minute') OVER (ORDER BY q2.start_time, q2.end_time)
										THEN NULL
										ELSE q2.start_time
										END AS new_start
									FROM (
										SELECT start_time, end_time, lag(end_time) OVER (ORDER BY start_time, end_time) AS lag_end_time
										FROM (
											SELECT date_trunc('minute', timestamp) AS start_time,
												date_trunc('minute', timestamp + (duration / 1000000.0) * INTERVAL '1 second') AS end_time
											FROM flows
											WHERE endpoint_b = '${arg_ip}'
												AND protocol IN (
													SELECT id FROM protocols
													WHERE name IN ('http', 'https')
												)
											GROUP BY start_time, end_time
										) q1
									) q2
								) q3
							) q4
							GROUP BY q4.left_edge
						) q5
					) q6
					ON s2.time_unit = q6.session_unit
				) q7
			) q8
			GROUP BY q8.day
		) q9
	) q
	WHERE s.ip = '${arg_ip}'
		AND s.day = q.day;
	SQL
	
	info ' ├─ Getting the number of sessions & session time...' '73'
	
	# Browsing sessions & session time
	exec_sql <<- SQL
	UPDATE stats s
	SET browsing_sessions = q.browsing_sessions,
		browsing_sess_time = q.browsing_sess_time
	FROM (
		SELECT q6.day,
			count(*) AS browsing_sessions,
			sum(q6.duration) AS browsing_sess_time
		FROM (
			SELECT s2.day, q5.timestamp_start, q5.timestamp_end, CASE
				WHEN q5.day_start = q5.day_end
					THEN q5.timestamp_end - q5.timestamp_start
				ELSE CASE
					WHEN q5.day_start = s2.day
						THEN q5.day_start + INTERVAL '24 hours' - q5.timestamp_start
					WHEN q5.day_end = s2.day
						THEN q5.timestamp_end - s2.day
					ELSE
						INTERVAL '24 hours'
					END
				END AS duration
			FROM (
				SELECT generate_series(day_start, day_end, '1 day'::INTERVAL)::DATE AS day
				FROM (
					SELECT min(day) AS day_start, 
						max(day) AS day_end
					FROM stats
					WHERE ip = '${arg_ip}'
				) s1
			) s2
			LEFT JOIN (
				SELECT min(q4.start_time) AS timestamp_start,
					max(q4.end_time) + INTERVAL '5 minutes' AS timestamp_end,
					min(q4.start_time)::DATE AS day_start,
					(max(q4.end_time) + INTERVAL '5 minutes')::DATE AS day_end
				FROM (
					SELECT q3.start_time, q3.end_time, max(q3.new_start) OVER (ORDER BY q3.start_time, q3.end_time) AS left_edge
					FROM (
						SELECT q2.start_time, q2.end_time,
							CASE WHEN q2.start_time <= max(q2.lag_end_time) OVER (ORDER BY q2.start_time, q2.end_time) + INTERVAL '5 minutes'
							THEN NULL
							ELSE q2.start_time
							END AS new_start
						FROM (
							SELECT start_time, end_time, lag(end_time) OVER (ORDER BY start_time, end_time) AS lag_end_time
							FROM (
								SELECT timestamp AS start_time,
									timestamp + (duration / 1000000.0) * INTERVAL '1 second' AS end_time
								FROM flows
								WHERE (endpoint_a = '${arg_ip}' OR endpoint_b = '${arg_ip}')
									AND protocol IN (
										SELECT id FROM protocols
										WHERE name IN ('http', 'https')
									)
							) q1
						) q2
					) q3
				) q4
				GROUP BY left_edge
			) q5
			ON s2.day BETWEEN q5.day_start AND q5.day_end
		) q6
		GROUP BY q6.day
	) q
	WHERE s.ip = '${arg_ip}'
		AND s.day = q.day;
	SQL
	
	info ' ├─ Getting the network activity time...' '82'
	
	# Network activity time
	exec_sql <<- SQL
	UPDATE stats s
	SET network_time = q.network_time
	FROM (
		SELECT q6.day,
			sum(q6.duration) AS network_time
		FROM (
			SELECT s1.day, CASE
				WHEN q5.day_start = q5.day_end
					THEN q5.timestamp_end - q5.timestamp_start
				ELSE CASE
					WHEN q5.day_start = s1.day
						THEN q5.day_start + INTERVAL '24 hours' - q5.timestamp_start
					WHEN q5.day_end = s1.day
						THEN q5.timestamp_end - s1.day
					ELSE
						INTERVAL '24 hours'
					END
				END AS duration
			FROM (
				SELECT day
				FROM stats
				WHERE ip = '${arg_ip}'
			) s1
			LEFT JOIN (
				SELECT min(q4.start_time) AS timestamp_start,
					max(q4.end_time) AS timestamp_end,
					min(q4.start_time)::DATE AS day_start,
					max(q4.end_time)::DATE AS day_end
				FROM (
					SELECT q3.start_time, q3.end_time, max(q3.new_start) OVER (ORDER BY q3.start_time, q3.end_time) AS left_edge
					FROM (
						SELECT q2.start_time, q2.end_time,
							CASE WHEN q2.start_time < max(q2.lag_end_time) OVER (ORDER BY q2.start_time, q2.end_time)
							THEN NULL
							ELSE q2.start_time
							END AS new_start
						FROM (
							SELECT start_time, end_time, lag(end_time) OVER (ORDER BY start_time, end_time) AS lag_end_time
							FROM (
								SELECT timestamp AS start_time,
									timestamp + (duration / 1000000.0) * INTERVAL '1 second' AS end_time
								FROM flows
								WHERE endpoint_a = '${arg_ip}'
									OR endpoint_b = '${arg_ip}'
								GROUP BY start_time, end_time
							) q1
						) q2
					) q3
				) q4
				GROUP BY q4.left_edge
			) q5
			ON s1.day BETWEEN q5.day_start AND q5.day_end
		) q6
		GROUP BY q6.day
	) q
	WHERE s.ip = '${arg_ip}'
		AND s.day = q.day;
	SQL
	
	info ' ├─ Getting the network usage percentages...' '91'
	
	# Network usage percentages
	exec_sql <<- SQL
	UPDATE stats s
	SET percent_flows = q.percent_flows,
		percent_packets = q.percent_packets,
		percent_payload = q.percent_payload
	FROM (
		SELECT day,
			(user_flows / total_flows * 100) AS percent_flows,
			(user_packets / total_packets * 100) AS percent_packets,
			(user_payload / total_payload * 100) AS percent_payload
		FROM (
			SELECT s.day,
				q1.total_flows::FLOAT,
				(s.flows_in + s.flows_out)::FLOAT AS user_flows,
				q1.total_packets::FLOAT,
				(s.packets_in + s.packets_out)::FLOAT AS user_packets,
				q1.total_payload::FLOAT,
				(s.payload_size_in + s.payload_size_out)::FLOAT AS user_payload
			FROM stats s
			JOIN (
				SELECT timestamp::DATE AS day,
					count(*) AS total_flows,
					sum(payload_size_ab + payload_size_ba) AS total_payload,
					sum(packets_nb_ab + packets_nb_ba) AS total_packets
				FROM flows
				GROUP BY day
			) q1
			ON s.day = q1.day
			WHERE s.ip = '${arg_ip}'
		) q2
	) q
	WHERE s.ip = '${arg_ip}'
		AND s.day = q.day;
	SQL
	
	info ' └─ Stats successful!' '100'
	
	return 0
}
