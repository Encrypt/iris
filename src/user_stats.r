# File: user_stats.r
# Produces different statistics about users

# Gets the IP from which to create the stats
ip <- commandArgs(trailingOnly = TRUE)

# Connects to the PostgreSQL database
library(RPostgreSQL)
db_drv <- dbDriver("PostgreSQL")
db_con <- dbConnect(db_drv, dbname = "<database>")

# Saves the results in a pdf
pdf(file = "iris_stats.pdf", paper = "a4r", pointsize = 10, width = 29.7, height = 21)



# # # # # # # # # # # # # #
# Title page (cheat code) #
# # # # # # # # # # # # # #

plot(0:10, type = "n", xaxt = "n", yaxt = "n", bty = "n", xlab = "", ylab = "")
text(6, 6.5, cex = 3, "IRIS -- Report")
text(6, 5, cex = 2.5, sprintf("Statistics for the IP %s", ip))



# # # # # # # # # # # # # # # # # # #
# Plots pie charts of network usage #
# # # # # # # # # # # # # # # # # # #

# Query to gets the stats
db_res <- dbSendQuery(db_con, sprintf("SELECT p.name AS protocol, q.flows, q.payload, q.packets FROM (SELECT protocol, count(*)::FLOAT AS flows, sum(coalesce(payload_size_ab, 0) + coalesce(payload_size_ba, 0))::FLOAT AS payload, sum(coalesce(packets_nb_ab, 0) + coalesce(packets_nb_ba, 0))::FLOAT AS packets FROM flows WHERE endpoint_a = '%s' OR endpoint_b = '%s' GROUP BY protocol) q JOIN protocols p ON q.protocol = p.id;", ip, ip))
data <- fetch(db_res, -1)

# Creates a matrix for each type of data
arr_flows <- data[,c('protocol', 'flows')]
arr_payload <- data[,c('protocol', 'payload')]

# Deletes the rows having a value too low
stats_flows <- arr_flows[(arr_flows$flows / sum(arr_flows$flows) * 100) > 1,]
stats_payload <- arr_payload[(arr_payload$payload / sum(arr_payload$payload) * 100) > 1,]

# Adds the "other" section
stats_flows <- rbind(stats_flows, list('other', sum(arr_flows$flows) - sum(stats_flows$flows)))
stats_payload <- rbind(stats_payload, list('other', sum(arr_payload$payload) - sum(stats_payload$payload)))

# Plots the pie charts
layout(matrix(c(1, 2), 1, 2))
pie(stats_flows$flows, labels = stats_flows$protocol, main = "Distribution of the flows")
pie(stats_payload$payload, labels = stats_payload$protocol, main = "Distribution of the payload")



# # # # # # # # # # # # # # # # # #
# Plots the flows per 30 minutes  #
# # # # # # # # # # # # # # # # # #

# Query to gets the stats
db_res <- dbSendQuery(db_con, "SELECT ip, flows_thirty_mins FROM stats;")
data <- fetch(db_res, -1)

# Splits the flows into multiple columns
flows_thirty_mins <- strsplit(gsub("[{}]", "", data$flows_thirty_mins), ",")
flows_matrix <- matrix(as.numeric(unlist(flows_thirty_mins)), ncol = 48, byrow = TRUE)

# Binds the columns to the IPs and add labels
flows <- data.frame(data$ip, flows_matrix)
names(flows) <- c("ip", sprintf("%02d:%02d", rep(seq(0, 23), each = 2), rep(c(0, 30), times = 24)))

# Extracts the user flows and global flows
flows_user <- flows[flows$ip == ip, -which(names(flows) == "ip")]
flows_global <- flows[, -which(names(flows) == "ip")]

# Plots the bar graph
layout(matrix(c(1, 2), 2, 2), heights = c(5, 3))
barplot(unlist(colMeans(flows_user)), main = "Average number of flows per 30 minutes", xlab = "Time", ylab = "Flows") 

mean_comp <- unlist((colMeans(flows_user) - colMeans(flows_global)) / colMeans(flows_global) * 100)
mean_comp[is.nan(mean_comp)] <- 0

barplot(mean_comp, col = ifelse(mean_comp > 0, "darkgreen", "darkred"), main = "Comparison to the mean", xlab = "Time", ylab = "% to the mean")



# # # # # # # # # # # # # # # # #
# Top 5 of the websites visited #
# # # # # # # # # # # # # # # # #

# Query to gets the stats
db_res <- dbSendQuery(db_con, sprintf("SELECT count(*) AS hits, u.value AS url FROM flows f JOIN websites w ON f.website = w.id JOIN urls u ON w.url = u.id WHERE (f.endpoint_a = '%s' OR f.endpoint_b = '%s') AND (w.category NOT IN (SELECT c.id FROM categories c JOIN topics t ON c.topic = t.id WHERE t.name = 'ads' or t.name = 'cdn') OR w.category IS NULL) GROUP BY u.value ORDER BY hits DESC LIMIT 5;", ip, ip))
data <- fetch(db_res, -1)

# Plots the result
margin <- max(nchar(data$url))*0.11
layout(matrix(c(2, 3, 0, 2, 3, 1), 3, 2), widths = c(margin, 29.7 - margin))
barplot(data$hits, horiz = TRUE, names.arg = data$url, main = "Top 5 websites visited by the user", xlab = "Number of hits", las = 1)



# # # # # # # # # # # # # # # # # # # #
# Flows in and out compared to others #
# # # # # # # # # # # # # # # # # # # #

# Query to gets the stats
db_res <- dbSendQuery(db_con, "SELECT ip, sum(flows_in) AS flows_in, sum(flows_out) AS flows_out FROM stats GROUP BY ip;")
data <- fetch(db_res, -1)

# Plots the results
boxplot(data$flows_in[data$flows_in != 0], horizontal = TRUE, log = "x", outcex = 1.5, main = "Number of incoming flows produced by the user, compared to the others", xlab = "Number of incoming flows (log axis)")
stripchart(data[data$ip == ip, which(names(data) == "flows_in")], add = TRUE, cex = 1.5, col = "red", pch = 16)

boxplot(data$flows_out[data$flows_out != 0], horizontal = TRUE, log = "x", outcex = 1.5, main = "Number of outgoing flows produced by the user, compared to the others", xlab = "Number of outgoing flows (log axis)")
stripchart(data[data$ip == ip, which(names(data) == "flows_out")], add = TRUE, cex = 1.5, col = "red", pch = 16)



# # # # # # # # # # #
# Browsing sessions #
# # # # # # # # # # #

# Query to gets the stats
db_res <- dbSendQuery(db_con, "SELECT ip, avg(browsing_sessions)::INT AS browsing_sessions, avg(browsing_sess_time)::INT AS browsing_sess_time, avg(mean_sess_time)::INT AS mean_sess_time FROM (SELECT ip, browsing_sessions, extract(EPOCH FROM browsing_sess_time) AS browsing_sess_time, extract(EPOCH FROM (browsing_sess_time / browsing_sessions)) AS mean_sess_time FROM stats) q GROUP BY ip;")
data <- fetch(db_res, -1)

# Gets the different values for the user and group
browsing_user <- data[data$ip == ip, -which(names(data) == "ip")]
browsing_global <- data[, -which(names(data) == "ip")]

# Plots the 3 graphs
layout(matrix(c(1, 2, 3), 3, 1))

boxplot(browsing_global$browsing_sessions, horizontal = TRUE, outcex = 1.5, main = "Average number of browsing sessions the user, compared to the others, per day", xlab = "Average number of browsing sessions (seconds)")
stripchart(browsing_user[, which(names(browsing_user) == "browsing_sessions")], add = TRUE, cex = 1.5, col = "red", pch = 16)

boxplot(browsing_global$browsing_sess_time, horizontal = TRUE, outcex = 1.5, main = "Average time spent by the user browsing, compared to the others", xlab = "Average time spent by the user browsing (seconds)")
stripchart(browsing_user[, which(names(browsing_user) == "browsing_sess_time")], add = TRUE, cex = 1.5, col = "red", pch = 16)

boxplot(browsing_global$mean_sess_time, horizontal = TRUE, log = "x", outcex = 1.5, main = "Average duration of a browsing session of the user, compared to the others", xlab = "Average duration of a browsing session (seconds)")
stripchart(browsing_user[, which(names(browsing_user) == "mean_sess_time")], add = TRUE, cex = 1.5, col = "red", pch = 16)



# # # # # # # # # # # # # # # # # #
# Browsing activity markov model  #
# # # # # # # # # # # # # # # # # #

# igraph is necessary to plot a graph
library(igraph)

# Query to gets the stats
db_res <- dbSendQuery(db_con, sprintf("SELECT round(browsing_proba_bb::NUMERIC, 4), round(browsing_proba_bi::NUMERIC, 4), round(browsing_proba_ib::NUMERIC, 4), round(browsing_proba_ii::NUMERIC, 4) FROM stats WHERE ip = '%s';", ip))
data <- fetch(db_res, -1)

# Sets up the markov model
markov_states <- c('Browsing','Browsing', 'Browsing','Not browsing', 'Not browsing','Browsing', 'Not browsing','Not browsing')
markov_graph <- graph(markov_states, dir = TRUE)

# Plots it
layout(matrix(c(1), 1, 1))
plot(markov_graph, edge.label = matrix(data, ncol = 4), edge.label.cex = 1.3, edge.curved = TRUE, edge.color = "black", vertex.size = 40, vertex.color = "grey75", main = "Markov model of the browsing sessions of the user")



# Disconnects from the database
dbDisconnect(db_con)

# Print the PDF
dev.off()
