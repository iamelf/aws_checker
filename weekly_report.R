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

dataPath <- "/Users/maghuang/aws"

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


countFreeTierCustomer <- function(week) {
  if (missing(week)) {
    week = weeknum() -1
  }
  year = as.integer(format(Sys.Date(), "%Y"))
  
  fname <- "weekly_report.csv"
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
  
  testAccount <- getTestAccounts()
  
  conn <- dbConnect(driver, url)
  
  # Get all free tier customer
  SQL <- paste("select * from a9cs_metrics.es_freetier
               where year = '", year,"'
               and week in ('", week,"', '", week-1,"')
               and is_internal_flag = 'N'", sep = "")
  
  message("Getting free tier customer data for week ", week - 1, ", ", week,".")
  free_tier_accounts <- dbGetQuery(conn, SQL)
  free_tier_accounts$account_id <- str_pad(as.character(free_tier_accounts$account_id), 12, pad="0")
  free_tier_accounts$week <- as.integer(free_tier_accounts$week)
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
  
  ft_current <- free_tier_accounts[free_tier_accounts$week == week, ]
  new_free_active <- intersect(new_active_account$account_id, ft_current$account_id)
  new_free_active <- setdiff(new_free_active, testAccount$account_id)
  
  SQL <- paste("select * from a9cs_metrics.es_new_inactive_customers
               where year = '", year,"'
               and week = '", week,"'
               and is_internal_flag = 'N'", sep = "")
  
  
  message("Getting new inactive customer data for week ", week)
  new_inactive_account <- dbGetQuery(conn, SQL)
  
  new_inactive_account$account_id <- str_pad(as.character(new_inactive_account$account_id), 12, pad="0")
  message("- Query succeed: new inactive customer data for week ", week, " retrieved.")
  
  
  ft_last <- free_tier_accounts[free_tier_accounts$week == week - 1, ]
  new_free_inactive <- intersect(new_inactive_account$account_id, ft_last$account_id)
  
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
  message("- Query succeed: zero revenue customer data for week ", week, " retrieved.")
  
  zero_rev_FT <- merge(ft_current, zero_rev, by=c("account_id"), all.x=TRUE)
  zero_rev_FT$sum <- round(zero_rev_FT$sum, 2)
  zero_rev_FT <- zero_rev_FT[zero_rev_FT$sum == 0,]
  zero_rev_FT <- zero_rev_FT[!is.na(zero_rev_FT$account_id),]
  
  ### deadloop
  last_x <- countFreeTierCustomer(week-1)
  
  x <- data.frame(year = year, 
                  week = week, 
                  new_free_active_count = length(new_free_active),
                  new_free_inactive_count = length(new_free_inactive),
                  convert_to_paying_count = length(unique(convert_to_pay$account_id)),
                  zero_revenue_free_tier = nrow(zero_rev_FT),
                  zero_revenue_free_tier_wow = nrow(zero_rev_FT)/last_x$zero_revenue_free_tier_wow-1)
  
  t <- rbind(t, x)
  t <- t[order(t$year, t$week), ]
  
  write.csv(t, fpath, row.names = FALSE)
  
  dbDisconnect(conn)
  return (x)
  
}

# External Customers Summary (All Regions)
# 
# Free Tier Customers
# # New Active Customers (over last 7 days)
# # Converts to Paying
# # New Inactive Customers (over last 7 days)
# Active Free Tier Customers with 0 Revenue
# WoW Change
# 
# Paying Customers
# # New Active Customers (over last 7 days)
# # Converts from Free Tier
# # New Inactive Customers (over last 7 days)
# Active Paying Customers
# WoW Change
# Active Paying Customers Goal
# Net Additional Paying Customers
# 
# Weekly Paying Churn
# 
# TOTAL CUSTOMERS
# # New Active Customers (over last 7 days)
# # New Inactive Customers (over last 7 days)
# Active Customers
# WoW Change
# Active Customers Goal
# Net Additional Customers
# 
# Weekly Churn
# 
# DOMAINS
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





