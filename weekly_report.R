library(RJDBC)
library(dplyr)
library(jsonlite)
library(elastic)
library(stringr)


#A9 cluster
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-1.1.9.1009.jar", identifier.quote="`")
url <- "jdbc:redshift://a9dba-fin-rs2.db.amazon.com:8192/a9aws?user=maghuang&password=pw20160926NOW&tcpKeepAlive=true&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"

#AWS database
#url <- "jdbc:postgresql://54.85.28.62:8192/datamart?user=maghuang&password=Youcan88$&tcpKeepAlive=true"

dataPath <- "/Users/maghuang/aws/weekly_report"

weeknum <- function(dateStr=Sys.Date()) {
  
  t <- as.POSIXlt(dateStr) 
  t <- strftime(t,format="%W")
  return (as.numeric(t))
}

getLast7days <- function () {
  t <- c(1:7)
  d <- Sys.Date() -1
  for (i in 1:7) {
    t[i] <- format(d, "%Y-%m-%d")
    d <- d - 1
  }
  return (t)
}

getTestAccounts <- function(week) {
  if (missing(week)) {
    week = weeknum() -1
  }
  fname <- "test_accounts.csv"
  fpath <- file.path(dataPath, fname)
  t <- read.csv(fpath)
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  return (t)
}



getWeeklyCharges <- function(week = weeknum() - 1, year = as.integer(format(Sys.Date(), "%Y"))) {
  
  message("- Getting weekly charge data for week ", week)
  
  SQL <- paste("select account_id, region, billed_amount, is_internal_flag 
              from a9cs_metrics.es_weekly_charges
              where year = '2016' AND week='", week,"'", sep="")
  conn <- dbConnect(driver, url)
    
  message("- Connected to DB.")
    
    
  message("- Running query: weekly charge data...")
  t <- dbGetQuery(conn, SQL)
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  message("- Query succeed: weekly charge for week ", week, " retrieved.")
  dbDisconnect(conn)
  
  return(t)
}

getWeeklyMetering <- function(week = weeknum() - 1, year = as.integer(format(Sys.Date(), "%Y"))) {
  
  message("- Getting weekly metering data for week ", week)
  
  
  SQL <- paste("select account_id, sum(usage_value) as sum_usage_value, week, year, is_internal_flag, usage_resource, usage_type, region
                from a9cs_metrics.es_weekly_metering
                where year = '", year, "' and week = '", week,"' 
                group by account_id, week, year, usage_resource, usage_type, is_internal_flag, region;", sep = "")

  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  
  message("- Running query: weekly metering data...")
  t <- dbGetQuery(conn, SQL)
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  message("- Query succeed: weekly metering for week ", week, " retrieved.")
  dbDisconnect(conn)
  
  return(t)
}

getDefectedDomainStats <- function(week = weeknum() - 1, year = as.integer(format(Sys.Date(), "%Y"))) {
  
  message("- Getting weekly metering data for week ", week)
  
  
  SQL <- paste("SELECT year, week, account_id, count(distinct usage_resource) as countofusage_resource, region, is_internal_flag
               FROM a9cs_metrics.es_weekly_metering 
               Where week = '", week,"' and year = '", year,"'
               GROUP BY year, week, ACCOUNT_ID, Band, Region, is_internal_flag
               HAVING sum(usage_value)<>0", sep = "")
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  
  message("- Running query: weekly metering data...")
  t <- dbGetQuery(conn, SQL)
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  message("- Query succeed: weekly metering for week ", week, " retrieved.")
  dbDisconnect(conn)
  
  return(t)
}

### NOTE ###
# there are 2 ways to get cusomer #, 
# 1. unique account_id from weekly metering 
# 2. unique account_id from weekly charge
# These 2 dataset 
# - Suspended accounts are included inn metering data but not in charge data (534 accounts for wk 38)
# - accounts that only had credits applied that week but doesn't have any domains, only appears in charge data (52 accounts for wk 38)
# Currently we're using the account # from the weekly charge data

countCustomers <- function(week = weeknum()-1, year = as.integer(format(Sys.Date(), "%Y"))) {
  
  testAccount <- getTestAccounts()
  
  conn <- dbConnect(driver, url)
  
  # Get all free tier customer
  SQL <- paste("select * from a9cs_metrics.es_freetier
               where year = '", year,"'
               and week in ('", week,"', '", week-1,"')
               and is_internal_flag = 'N'", sep = "")
  
  message("Getting free tier customer data for week ", week - 1, ", ", week,".")
  free_accounts <- dbGetQuery(conn, SQL)
  free_accounts$account_id <- str_pad(as.character(free_accounts$account_id), 12, pad="0")
  free_accounts$week <- as.integer(free_accounts$week)
  message("- Query succeed: free tier customer data for week ", week - 1, ", ", week," retrieved.")
  
  
  # Get all new active customer
  SQL <- paste("select * from a9cs_metrics.es_new_active_customers
               where year = '", year,"'
               and week = '", week,"'
               and is_internal_flag = 'N'", sep = "")
  
  message("- Connected to DB.")
  message("Getting new active customer data for week ", week)
  new_active_account <- dbGetQuery(conn, SQL)
  new_active_account$account_id <- str_pad(as.character(new_active_account$account_id), 12, pad="0")
  message("- Query succeed: new active customer data for week ", week, " retrieved.")
  
  # Calculate the active customers, divided by free-tier and paying customers
  ft_current <- free_accounts[free_accounts$week == week, ]
  
  
  new_free_active <- intersect(new_active_account$account_id, ft_current$account_id)
  
  new_free_active <- setdiff(new_free_active, testAccount$account_id)
  new_free_active_count <- length(new_free_active)
  
  new_paying_active <- setdiff(new_active_account$account_id, ft_current$account_id)
  new_paying_active <- setdiff(new_paying_active, testAccount$account_id)
  new_paying_active_count <- length(new_paying_active)
  
  SQL <- paste("select * from a9cs_metrics.es_new_inactive_customers
               where year = '", year,"'
               and week = '", week,"'
               and is_internal_flag = 'N'", sep = "")
  
  
  message("Getting new inactive customer data for week ", week)
  new_inactive_account <- dbGetQuery(conn, SQL)
  
  new_inactive_account$account_id <- str_pad(as.character(new_inactive_account$account_id), 12, pad="0")
  message("- Query succeed: new inactive customer data for week ", week, " retrieved.")
  
  # Calculate the inactive customers, divided by free-tier and paying customers
  ft_last <- free_accounts[free_accounts$week == week - 1, ]
  new_free_inactive <- intersect(new_inactive_account$account_id, ft_last$account_id)
  new_free_inactive_count <- length(new_free_inactive)
  
  new_paying_inactive <- setdiff(new_inactive_account$account_id, new_free_inactive)
  new_paying_inactive_count <- length(new_paying_inactive)
  
  
  # Get converted to pay
  SQL <- paste("select * from a9cs_metrics.es_free_to_pay
               where year = '", year,"'
               and week = '", week,"'", sep = "")
  
  message("Getting converted-to-paying customer data for week ", week)
  convert_to_pay <- dbGetQuery(conn, SQL)
  convert_to_pay$account_id <- str_pad(as.character(convert_to_pay$account_id), 12, pad="0")
  message("- Query succeed: converted-to-paying customer data for week ", week, " retrieved.")
  
  # Get zero revenue customer data
  SQL <- paste("select account_id, sum(billed_amount) from a9cs_metrics.es_weekly_charges
               where year = '", year,"'
               and week = '", week,"'
               group by 1", sep = "")
  
  message("Getting zero revenue customer data for week ", week)
  zero_rev <- dbGetQuery(conn, SQL)
  zero_rev$account_id <- str_pad(as.character(zero_rev$account_id), 12, pad="0")
  convert_to_paying_count <- length(unique(convert_to_pay$account_id))
  message("- Query succeed: zero revenue customer data for week ", week, " retrieved.")
  dbDisconnect(conn)
  
  zero_rev_FT <- merge(ft_current, zero_rev, by=c("account_id"), all.x=TRUE)
  zero_rev_FT$sum <- round(zero_rev_FT$sum, 2)
  zero_rev_FT <- zero_rev_FT[zero_rev_FT$sum == 0,]
  zero_rev_FT <- zero_rev_FT[!is.na(zero_rev_FT$account_id),]
  zero_revenue_free_count = nrow(zero_rev_FT)
  
  # get total customer account
  
  weeklyCharge <- getWeeklyCharges(week)
  weeklyCharge <- weeklyCharge[weeklyCharge$is_internal_flag == "N",]
  
  active <- unique(weeklyCharge$account_id)
  active <- setdiff(active, testAccount$account_id)
  active_count <- length(active)
  
  # Get the weekly customer goal
  goalFile <- "es-goal-2016.csv"
  goalFilePath <- file.path(dataPath, goalFile)
  esGoal <- read.csv(goalFilePath)
  esGoal <- esGoal[esGoal$week == week,]
  
  x <- data.frame(year = year, 
                  week = week, 
                  new_free_active_count = new_free_active_count,
                  new_free_inactive_count = new_free_inactive_count,
                  convert_to_paying_count = convert_to_paying_count,
                  zero_revenue_free_count = zero_revenue_free_count,
                  new_paying_active_count = new_paying_active_count,
                  new_paying_inactive_count = new_paying_inactive_count,
                  active_paying_count = active_count - zero_revenue_free_count,
                  active_count = active_count,
                  new_active_count = new_free_active_count + new_paying_active_count,
                  new_inactive_count = new_free_inactive_count + new_paying_inactive_count,
                  paying_customer_goal = esGoal$paying_customer_goal, 
                  active_customer_goal = esGoal$active_customer_goal)
  
  return (x)
}

### NOTE ###
# there are 2 ways to get cusomer #, 
# 1. unique account_id from weekly metering 
# 2. unique account_id from weekly charge
# These 2 dataset 
# - Suspended accounts are included inn metering data but not in charge data (534 accounts for wk 38)
# - accounts that only had credits applied that week but doesn't have any domains, only appears in charge data (52 accounts for wk 38)
# Currently, when counting customer with 1 or more domains,
# we're using the account # from the weekly charge data, and exclude the "credit only" accounts, which has 0 domains

countDomains <- function(week = weeknum()-1, year = as.integer(format(Sys.Date(), "%Y"))) {
  
  meter <- getWeeklyMetering()
  charge_account <- getWeeklyCharges()$account_id
  testAccount <- getTestAccounts()
  
  meter <- subset(meter, !(account_id %in% testAccount$account_id))
  meter <- subset(meter, account_id %in% charge_account)

  
  
  dt_external <- subset(meter, is_internal_flag == "N")
  dt_internal <- subset(meter, is_internal_flag == "Y")
  
  external_domain_count <- length(unique(dt_external$usage_resource))
  internal_domain_count <- length(unique(dt_internal$usage_resource))
  
  ag_external <- aggregate(usage_resource ~ account_id, dt_external, length)
  one_domain_customer_count <- nrow(ag_external[ag_external$usage_resource == 1, ])
  multi_domain_customer_count <- nrow(ag_external[ag_external$usage_resource > 1, ])
  
  dt_external
  
  
  x <- data.frame(year = year, 
                  week = week, 
                  external_domain_count = external_domain_count,
                  one_domain_customer_count = one_domain_customer_count,
                  multi_domain_customer_count = multi_domain_customer_count,
                  internal_domain_count = internal_domain_count)
 
  return (x) 
}


countInstanceUsage <- function(week = weeknum()-1, year = as.integer(format(Sys.Date(), "%Y"))) {
  
  meter <- getWeeklyMetering()
  charge_account <- getWeeklyCharges()$account_id
  testAccount <- getTestAccounts()
  
  meter <- subset(meter, !(account_id %in% testAccount$account_id))
  meter <- subset(meter, account_id %in% charge_account)
  
  meter$region <- 
  
  dt_external <- subset(meter, is_internal_flag == "N")
  dt_internal <- subset(meter, is_internal_flag == "Y")
  
  
  external_domain_count <- length(unique(dt_external$usage_resource))
  internal_domain_count <- length(unique(dt_internal$usage_resource))
  
  ag_external <- aggregate(usage_resource ~ account_id, dt_external, length)
  one_domain_customer_count <- nrow(ag_external[ag_external$usage_resource == 1, ])
  multi_domain_customer_count <- nrow(ag_external[ag_external$usage_resource > 1, ])
  
  dt_external
  
  
  x <- data.frame(year = year, 
                  week = week, 
                  external_domain_count = external_domain_count,
                  one_domain_customer_count = one_domain_customer_count,
                  multi_domain_customer_count = multi_domain_customer_count,
                  internal_domain_count = internal_domain_count)
  
  return (x) 
}

calcWeeklyStats <- function(week = weeknum()-1, year = as.integer(format(Sys.Date(), "%Y"))) {

  fname <- "weekly_metrics.csv"
  fpath <- file.path(dataPath, fname)
  if(file.exists(fpath)) {
    t <- read.csv(fpath)
    x <- t[t$week == week, ]
    if (nrow(x) > 0) {
      return (x)
    }
  } else {
    t <- data.frame()
  }
  
  x <- countCustomers(week = week, year = year)
  x <- cbind(x, countDomains(week = week, year = year))
  
  t <- rbind(t, x)
  t <- t[order(t$year, t$week), ]
  
  write.csv(t, fpath, row.names = FALSE)
}
  


# Total # Active Domains
# WoW Change
# Distribution - # Customers with:
#   1 domain
# 2 domains+
#   
#   Weekly ASP
# Weekly ASP Goal
# 
# External Customer Revenue (All Regions)
# Overall Weekly Revenue
# WoW Change
# Trailing 13 week CWGR
# Weekly Revenue Goal
# Variance to Plan - Over / (Under)
# Revenue Breakdown:
#   ESInstance
# EBSStorage
# DataTransfer-Out-Bytes
# YTD Revenue
# YTD Revenue Goal
# Variance to Plan - Over / (Under)
# 4-Wk Annual Run Rate
# 4-Wk Annual Run Rate Goal
# Variance to Plan - Over / (Under)



# Daily analysis 
# * daily external, internal revenue
# * Total credit daily
# * Top customers
# * Top gainers, top losers
# * Count of total paying customer accounts
# * Count of 0 revenue accounts
# * Single Customer usage trending





