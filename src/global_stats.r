# File: user_stats.r
# Produces different statistics about the network

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
text(6, 5, cex = 2.5, "Overview of the network")



# # # # # # # # # # # # # # # # # # #
# Plots pie charts of network usage #
# # # # # # # # # # # # # # # # # # #

# Query to gets the stats
db_res <- dbSendQuery(db_con, "SELECT p.name AS protocol, q.flows, q.payload, q.packets FROM (SELECT protocol, count(*)::FLOAT AS flows, sum(coalesce(payload_size_ab, 0) + coalesce(payload_size_ba, 0))::FLOAT AS payload, sum(coalesce(packets_nb_ab, 0) + coalesce(packets_nb_ba, 0))::FLOAT AS packets FROM flows GROUP BY protocol) q JOIN protocols p ON q.protocol = p.id;")
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

# matrixStats is necessary for the standard deviation
library(matrixStats)

# Query to gets the stats
db_res <- dbSendQuery(db_con, "SELECT flows_thirty_mins FROM stats;")
data <- fetch(db_res, -1)

# Splits the flows into multiple columns
flows_thirty_mins <- strsplit(gsub("[{}]", "", data$flows_thirty_mins), ",")
flows_matrix <- matrix(as.numeric(unlist(flows_thirty_mins)), ncol = 48, byrow = TRUE)

# Processes the standard deviation
flows_sd_matrix <- matrix(colSds(flows_matrix), ncol = 48)
flows_sd <- data.frame(flows_sd_matrix)

# Binds the columns to the IPs and add labels
flows <- data.frame(flows_matrix)
names(flows) <- c(sprintf("%02d:%02d", rep(seq(0, 23), each = 2), rep(c(0, 30), times = 24)))
names(flows_sd) <- c(sprintf("%02d:%02d", rep(seq(0, 23), each = 2), rep(c(0, 30), times = 24)))

# Plots the bar graph
layout(matrix(c(1, 2), 2, 1), heights = c(5, 4))
barplot(unlist(colMeans(flows)), main = "Average number of flows per 30 minutes", xlab = "Time", ylab = "Number of flows") 
barplot(unlist(colMeans(flows_sd)), main = "Standard deviation of the number of flows per 30 minutes", xlab = "Time", ylab = "Number of flows") 



# # # # # # # # # # # # # # #
# User clustering on flows  #
# # # # # # # # # # # # # # #

# Query to gets the stats
db_res <- dbSendQuery(db_con, "SELECT EXTRACT(EPOCH FROM avg(network_time))::INT AS network_time, round(avg(flows_in + flows_out), 1) AS flows, round(avg(packets_in + packets_out), 1) AS packets FROM stats GROUP BY ip;")
data <- fetch(db_res, -1)

# Apply the k-means algorithm
flows_kmeans <- kmeans(data, 5)

# Plots the graphs
layout(matrix(c(1), 1, 1))
plot(data[c("network_time", "flows", "packets")], log = "xy", col = flows_kmeans$cluster, labels = c("Network usage time (seconds)", "Total number of flows", "Total number of packets"), main = "Average distribution of the flows, per day")



# # # # # # # # # # # # # # # # # # # #
# User clustering on brosing sessions #
# # # # # # # # # # # # # # # # # # # #

# Query to gets the stats
db_res <- dbSendQuery(db_con, "SELECT round(avg(browsing_sessions), 1) AS browsing_sessions, EXTRACT(EPOCH FROM avg(browsing_sess_time))::INT AS browsing_sess_time, round(avg(coalesce(websites_visited, 0)), 1) AS websites_visited FROM stats GROUP BY ip;")
data <- fetch(db_res, -1)

# Apply the k-means algorithm
flows_kmeans <- kmeans(data, 5)

# Plots the graphs
plot(data[c("browsing_sessions", "browsing_sess_time", "websites_visited")], col = flows_kmeans$cluster, labels = c("Number of browsing sessions", "Duration of a session (seconds)", "Number of websites visited"), main = "Average distribution of the browsing information, per day")



# Disconnects from the database
dbDisconnect(db_con)

# Print the PDF
dev.off()
