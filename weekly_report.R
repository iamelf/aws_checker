library(RJDBC)
library(dplyr)
library(jsonlite)
library(elastic)
library(stringr)
library(reshape2)
library(XLConnect)

#A9 cluster
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-1.1.9.1009.jar", identifier.quote="`")
url <- "jdbc:redshift://a9dba-fin-rs2.db.amazon.com:8192/a9aws?user=maghuang&password=pw20160926NOW&tcpKeepAlive=true&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"

#AWS database
#url <- "jdbc:postgresql://54.85.28.62:8192/datamart?user=maghuang&password=Youcan88$&tcpKeepAlive=true"

dataPath <- "/Users/maghuang/aws/weekly_report"

# Output files
reportFilePath <- file.path(dataPath, "Elasticsearch-metrics-report.xlsx")
metricsFilePath <- file.path(dataPath, "weekly_metrics.csv")
creditsFilePath <- file.path(dataPath, "weekly_credits.csv")
customerFilePath <- file.path(dataPath, "top_customers.csv")
gainerFilePath <- file.path(dataPath, "gainer_loser.csv")

# Input files
priceFilePath <- file.path(dataPath, "prices.csv")

weeknum <- function(dateStr=Sys.Date()) {
  
  t <- as.POSIXlt(dateStr) 
  t <- strftime(t,format="%W")
  return (as.numeric(t))
}

translateRegion <- function (alias) {
  if (missing(alias)) {
    message("Please pass in a region alias!")
    return (-1)
  }
  regions <- data.frame(location = c("", "USW2","USW1", "EU", "APN1", "APS1", "APS2",	"SAE1", "EUC1", "APN2"),
                        airport = c("IAD","PDX","SFO","DUB", "NRT", "SIN", "SYD", "GRU", "FRA", "ICN"))
  
  z <- NULL
  for (i in alias) {
    x <- regions[regions$location == i, ]
    y <- as.character(x$airport)
    if (nrow(x) == 0) {
      x <- regions[regions$airport == i, ]
      if (nrow(x) == 0) {
        y <- "IAD"   # When couldn't find a region, set it to default as IAD
      } else {
        y <- as.character(x$location)
      }
    }
    z <- c(z, y)
  }
  return (z)
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

getGoal <- function(week, year) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  # Get the weekly customer goal
  goalFile <- paste("es-goal-", year, ".csv", sep="")
  goalFilePath <- file.path(dataPath, goalFile)
  esGoal <- read.csv(goalFilePath)
  esGoal <- esGoal[esGoal$week == week,]
  return (esGoal)
}


getCustomerData <- function () {
  
  dataPath <- file.path(dataPath, "data_buffer")
  
  if (!dir.exists(dataPath)) {
    dir.create(dataPath)
  }
  
  fname <- paste("CustomerData-wk", weeknum(),".csv", sep="")
  fpath <- file.path(dataPath, fname)
  
  if(file.exists(fpath)) {
    message("Reading customer data from file: ", fpath)
    t <- read.csv(fpath, sep=",")
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    t$payer_account_id <- str_pad(as.character(t$payer_account_id), 12, pad="0")
  } else {
    conn <- dbConnect(driver, url)
    message("- Connected to DB.")
    
    SQL <- "WITH account 
    AS (SELECT DISTINCT account_id, payer_account_id, is_internal_flag 
    FROM   dim_aws_accounts 
    WHERE  end_effective_date IS NULL
    AND    current_record_flag = 'Y')   -- tt33060829 dupe stop
    SELECT aa.account_id, 
    coalesce(bb.account_field_value, 'e-mail Domain:' 
    || aa.email_domain) AS company, 
    aa.clear_name name, 
    aa.clear_lower_email email, 
    account.is_internal_flag,
    account.payer_account_id
    FROM   t_customers aa 
    left outer join o_aws_account_field_values bb 
    ON ( aa.account_id = bb.account_id 
    AND bb.account_field_id = 2 ) 
    left outer join account 
    ON ( account.account_id = aa.account_id ) WHERE  aa.account_id IN (SELECT DISTINCT account_id 
    FROM   o_aws_subscriptions 
    -- WHERE  offering_id IN ( '26555', '113438', '128046' )
    WHERE  offering_id IN ('607996', '607997', '615324', '615325', '729920' )
    AND ( end_date IS NULL 
    OR end_date >= ( SYSDATE - 8 )));"
    
    message("- Running query: customer data...")
    t <- dbGetQuery(conn, SQL)
    
    t$account_id <- str_pad(t$account_id, 12, pad="0")
    message("- Query succeed: customer data retrieved.")
    dbDisconnect(conn)
    
    write.csv(t, fpath)
    message("Customer data saved to file: ", fpath)
  }
  
  return(t)
}


getWeeklyCharges <- function(week, year) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  message("- Getting weekly charge data for week ", week)
  
  dataPath <- file.path(dataPath, "data_buffer")
  
  if (!dir.exists(dataPath)) {
    dir.create(dataPath)
  }
  
  fname <- paste("WeeklyCharge_wk", week, ".csv", sep="")
  fpath <- file.path(dataPath, fname);
  
  if (file.exists(fpath)) {
    t <- read.csv(fpath, sep=",")
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  } else {
    SQL <- paste("select account_id, 
              is_internal_flag, 
                 billed_amount, 
                 usage_type, 
                 region, 
                 credit_id,
                 charge_item_desc
                 from a9cs_metrics.es_weekly_charges
                 where year = '2016' AND week='", week,"'", sep="")
    conn <- dbConnect(driver, url)
    
    message("- Connected to DB.")
    
    
    message("- Running query: weekly charge data...")
    t <- dbGetQuery(conn, SQL)
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    dbDisconnect(conn)
    write.csv(t, fpath)
  }
  
  message("Charge data for week ", week, " retrieved.")
  
  return(t)
}

getWeeklyMetering <- function(week, year) {
  
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  message("- Getting weekly metering data for week ", week)
  
  dataPath <- file.path(dataPath, "data_buffer")
  
  if (!dir.exists(dataPath)) {
    dir.create(dataPath)
  }
  
  fname <- paste("WeeklyMetering_wk", week, ".csv", sep="")
  fpath <- file.path(dataPath, fname);
  
  if (file.exists(fpath)) {
    t <- read.csv(fpath, sep=",")
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  } else {
    SQL <- paste("select account_id, sum(usage_value) as sum_usage_value, week, year, is_internal_flag, usage_resource, usage_type, region
                from a9cs_metrics.es_weekly_metering
                 where year = '", year, "' and week = '", week,"' 
                 and usage_type like '%ESInstance%'
                 group by account_id, week, year, usage_resource, usage_type, is_internal_flag, region;", sep = "")
    conn <- dbConnect(driver, url)
    
    message("- Connected to DB.")
    
    
    message("- Running query: weekly metering data...")
    t <- dbGetQuery(conn, SQL)
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
   
    dbDisconnect(conn)
    write.csv(t, fpath)
  }
  message("Metering data for week ", week, " retrieved.")
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

countCustomers <- function(week, year) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  testAccount <- getTestAccounts()
  x <- data_frame(week = week)
  
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
               and week = '", week,"'", sep = "")
  
  message("- Connected to DB.")
  message("Getting new active customer data for week ", week)
  new_active_account <- dbGetQuery(conn, SQL)
  new_active_account$account_id <- str_pad(as.character(new_active_account$account_id), 12, pad="0")
  message("- Query succeed: new active customer data for week ", week, " retrieved.")
  
  # Calculate the active customers, divided by free-tier and paying customers
  ft_current <- free_accounts[free_accounts$week == week, ]
  
  new_active_account_external <- new_active_account[new_active_account$is_internal_flag == "N",]
  
  new_free_active <- intersect(new_active_account_external$account_id, ft_current$account_id)
  new_free_active <- setdiff(new_free_active, testAccount$account_id)
  x$new_free_active_count <- length(new_free_active)
  
  new_paying_active <- setdiff(new_active_account_external$account_id, ft_current$account_id)
  new_paying_active <- setdiff(new_paying_active, testAccount$account_id)
  x$new_paying_active_count <- length(new_paying_active)
  
  x$new_active_count <- x$new_free_active_count + x$new_paying_active_count
  
  new_active_count_internal <- new_active_account[new_active_account$is_internal_flag == "Y",c("account_id")]
  x$new_active_count_internal <- length(setdiff(new_active_count_internal, testAccount$account_id))
  
  SQL <- paste("select * from a9cs_metrics.es_new_inactive_customers
               where year = '", year,"'
               and week = '", week,"'", sep = "")
  
  
  message("Getting new inactive customer data for week ", week)
  new_inactive_account <- dbGetQuery(conn, SQL)
  new_inactive_account$account_id <- str_pad(as.character(new_inactive_account$account_id), 12, pad="0")
  message("- Query succeed: new inactive customer data for week ", week, " retrieved.")
  
  # Calculate the inactive customers, divided by free-tier and paying customers
  
  new_inactive_account_external <- new_inactive_account[new_inactive_account$is_internal_flag == "N", ]
  ft_last <- free_accounts[free_accounts$week == week - 1, ]
  new_free_inactive <- intersect(new_inactive_account_external$account_id, ft_last$account_id)
  x$new_free_inactive_count <- length(new_free_inactive)
  
  new_paying_inactive <- setdiff(new_inactive_account_external$account_id, new_free_inactive)
  x$new_paying_inactive_count <- length(new_paying_inactive)
  x$new_inactive_count <- x$new_free_inactive_count + x$new_paying_inactive_count
  
  new_inactive_account_internal <- new_inactive_account[new_inactive_account$is_internal_flag == "Y", c("account_id")]
  
  x$new_inactive_count_internal <- length(setdiff(new_inactive_account_internal, testAccount$account_id))
  
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
  x$convert_to_paying_count <- length(unique(convert_to_pay$account_id))
  message("- Query succeed: zero revenue customer data for week ", week, " retrieved.")
  dbDisconnect(conn)
  
  zero_rev_FT <- merge(ft_current, zero_rev, by=c("account_id"), all.x=TRUE)
  zero_rev_FT$sum <- round(zero_rev_FT$sum, 2)
  zero_rev_FT <- zero_rev_FT[zero_rev_FT$sum == 0,]
  zero_rev_FT <- zero_rev_FT[!is.na(zero_rev_FT$account_id),]
  x$zero_revenue_free_count = nrow(zero_rev_FT)
  
  # get total external customer account
  
  weeklyCharge <- getWeeklyCharges(week, year)
  weeklyCharge_external <- weeklyCharge[weeklyCharge$is_internal_flag == "N",]
  
  active <- unique(weeklyCharge_external$account_id)
  active <- setdiff(active, testAccount$account_id)
  x$active_count <- length(active)
  x$active_paying_count <- x$active_count - x$zero_revenue_free_count
  
  # get total internal customer account
  
  weeklyCharge_internal <- weeklyCharge[weeklyCharge$is_internal_flag == "Y",]
  
  active <- unique(weeklyCharge_internal$account_id)
  active <- setdiff(active, testAccount$account_id)
  x$active_count_internal <- length(active)
  
  # Get the weekly customer goal
  esGoal <- getGoal(week, year)
  x$paying_customer_goal <- esGoal$paying_customer_goal
  x$active_customer_goal <- esGoal$active_customer_goal
  
  
  x$week <- NULL
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

countDomains <- function(week, year) {
  
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  x <- data.frame(week = week)
  
  meter <- getWeeklyMetering(week, year)[, c("account_id", "region", "usage_resource", "is_internal_flag")]
  charge_account <- getWeeklyCharges(week, year)$account_id
  testAccount <- getTestAccounts()
  
  meter <- subset(meter, !(account_id %in% testAccount$account_id))
  meter <- subset(meter, account_id %in% charge_account)
  
  dt_external <- subset(meter, is_internal_flag == "N")
  dt_internal <- subset(meter, is_internal_flag == "Y")
  
  x$external_domain_count <- length(unique(dt_external$usage_resource))
  x$internal_domain_count <- length(unique(dt_internal$usage_resource))
  
  dt_external <- dt_external[!duplicated(dt_external$usage_resource), ]
  ag_external <- aggregate(usage_resource ~ account_id, dt_external, length)
  x$one_domain_customer_count <- nrow(ag_external[ag_external$usage_resource == 1, ])
  x$multi_domain_customer_count <- nrow(ag_external[ag_external$usage_resource > 1, ])
  
  x$week <- NULL
  return (x) 
}


countInstanceUsage <- function(week = weeknum()-1, year = as.integer(format(Sys.Date(), "%Y"))) {
  
  meter <- getWeeklyMetering(week, year)
  charge_account <- getWeeklyCharges(week, year)$account_id
  testAccount <- getTestAccounts()

  # Clean up the usaage date 
  # by excluding all the suspended accounts (accounts that are not in charge data)
  meter <- subset(meter, !(account_id %in% testAccount$account_id))
  meter <- subset(meter, account_id %in% charge_account)
  
  meter <- transform(meter, usage=colsplit(meter$usage_type, ":", names=c("1","2")))
  meter$usage.1 <- NULL
  names(meter)[names(meter) == 'usage.2'] <- 'instance_type'
  
  dt_external <- subset(meter, is_internal_flag == "N")
  
  ag_external <- aggregate(sum_usage_value ~ instance_type, dt_external, sum)
  
  x <- data.frame(week = week)
  for (i in 1:nrow(ag_external)) { 
    vname <- paste(ag_external[i,]$instance_type, "_hours", sep = "")
    x[, vname] <- ag_external[i,]$sum_usage_value
  }
  x$week <- NULL
  
  return (x) 
}


countRevenue <- function(week, year, customerList) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  if (missing(customerList)) {
    message("Please pass in customer data")
    return (0)
  }
  
  charge_total <- getWeeklyCharges(week, year)
  testAccount <- getTestAccounts()
  
  charge_total <- subset(charge_total, !(account_id %in% testAccount$account_id))
  
  charge_external_gross <- charge_total[charge_total$is_internal_flag == "N", ]
  charge_external_gross <- charge_external_gross[is.na(charge_external_gross$credit_id) ,]
  
  charge_external_credit <- charge_total[!is.na(charge_total$credit_id), ]
  
  charge_internal <- charge_total[charge_total$is_internal_flag == "Y", ]
  
  x <- data_frame(week = week)
  
  x$revenue_external_gross <- sum(charge_external_gross$billed_amount)
  x$revenue_internal <- sum(charge_internal$billed_amount)
  x$credit_amount <- sum(charge_external_credit$billed_amount)
  
  esGoal <- getGoal(week, year)
  
  x$revenue_goal = esGoal$revenue_goal
  
  instance_usage <- filter(charge_external_gross, grepl("ESInstance", usage_type))
  transfer_usage <- filter(charge_external_gross, grepl("-Out", usage_type))
  storage_usage <- filter(charge_external_gross, grepl("Storage|PIOPS", usage_type))
  
  x$revenue_instance <- sum(instance_usage$billed_amount, na.rm = TRUE)
  x$revenue_transfer <- sum(transfer_usage$billed_amount, na.rm = TRUE)
  x$revenue_storage <- sum(storage_usage$billed_amount, na.rm = TRUE)
  
  instance_usage <- filter(charge_internal, grepl("ESInstance", usage_type))
  transfer_usage <- filter(charge_internal, grepl("-Out", usage_type))
  storage_usage <- filter(charge_internal, grepl("Storage|PIOPS", usage_type))
  
  x$revenue_instance_internal <- sum(instance_usage$billed_amount, na.rm = TRUE)
  x$revenue_transfer_internal <- sum(transfer_usage$billed_amount, na.rm = TRUE)
  x$revenue_storage_internal <- sum(storage_usage$billed_amount, na.rm = TRUE)
  
  # aggregate revenue by customers
  charge_external <- rbind(charge_external_credit, charge_external_gross)
  revenue_customer <- aggregate(billed_amount ~ account_id, charge_external, sum)
  
  # aggregate revenue by payer_account_id
  cust <- customerList[, c("account_id", "payer_account_id")]
  
  revenue_customer <- merge(revenue_customer, cust, by=c("account_id"), all.x = TRUE)
  revenue_customer <- within(revenue_customer, account_id <- ifelse(!is.na(payer_account_id),payer_account_id,account_id))
  revenue_customer <- aggregate(billed_amount ~ account_id, data = revenue_customer, sum)
  
  # calculate the pro-rate monthly spending
  revenue_customer$monthly_revenue <- revenue_customer$billed_amount * 4.345238
  
  customer_micro <- revenue_customer[revenue_customer$monthly_revenue < 100, ]
  x$customer_micro_count <- nrow(customer_micro)
  x$customer_micro_revenue <- sum(customer_micro$billed_amount, na.rm = TRUE)
  
  customer_S <- revenue_customer[revenue_customer$monthly_revenue >= 100 & revenue_customer$monthly_revenue < 1000 , ]
  x$customer_S_count <- nrow(customer_S)
  x$customer_S_revenue <- sum(customer_S$billed_amount, na.rm = TRUE)
  
  customer_M <- revenue_customer[revenue_customer$monthly_revenue >= 1000 & revenue_customer$monthly_revenue < 10000 , ]
  x$customer_M_count <- nrow(customer_M)
  x$customer_M_revenue <- sum(customer_M$billed_amount, na.rm = TRUE)
  
  customer_L <- revenue_customer[revenue_customer$monthly_revenue >= 10000 & revenue_customer$monthly_revenue < 100000 , ]
  x$customer_L_count <- nrow(customer_L)
  x$customer_L_revenue <- sum(customer_L$billed_amount, na.rm = TRUE)
  
  customer_XL <- revenue_customer[revenue_customer$monthly_revenue >= 100000 & revenue_customer$monthly_revenue < 1000000 , ]
  x$customer_XL_count <- nrow(customer_XL)
  x$customer_XL_revenue <- sum(customer_XL$billed_amount, na.rm = TRUE)
  
  # get top 5 customers and their revenue
  top_external_customers <- head(arrange(revenue_customer,desc(billed_amount)), n = 5)
  top_external_customers <- merge(top_external_customers, customerList[,c("account_id", "company")], by = c("account_id"), all.x = TRUE)
  top_external_customers <- arrange(top_external_customers,desc(billed_amount))
  
  x$top_external_customer_1 = top_external_customers$company[1]
  x$top_external_customer_1_revenue = top_external_customers$billed_amount[1]
  x$top_external_customer_2 = top_external_customers$company[2]
  x$top_external_customer_2_revenue = top_external_customers$billed_amount[2]
  x$top_external_customer_3 = top_external_customers$company[3]
  x$top_external_customer_3_revenue = top_external_customers$billed_amount[3]
  x$top_external_customer_4 = top_external_customers$company[4]
  x$top_external_customer_4_revenue = top_external_customers$billed_amount[4]
  x$top_external_customer_5 = top_external_customers$company[5]
  x$top_external_customer_5_revenue = top_external_customers$billed_amount[5]
  
  
  revenue_customer <- aggregate(billed_amount ~ account_id, charge_internal, sum)
  
  top_internal_customers <- head(arrange(revenue_customer,desc(billed_amount)), n = 5)
  top_internal_customers <- merge(top_internal_customers, customerList[,c("account_id", "company")], by = c("account_id"), all.x = TRUE)
  top_internal_customers <- arrange(top_internal_customers,desc(billed_amount))
  
  x$top_internal_customer_1 = top_internal_customers$account_id[1]
  x$top_internal_customer_1_revenue = top_internal_customers$billed_amount[1]
  x$top_internal_customer_2 = top_internal_customers$account_id[2]
  x$top_internal_customer_2_revenue = top_internal_customers$billed_amount[2]
  x$top_internal_customer_3 = top_internal_customers$account_id[3]
  x$top_internal_customer_3_revenue = top_internal_customers$billed_amount[3]
  x$top_internal_customer_4 = top_internal_customers$account_id[4]
  x$top_internal_customer_4_revenue = top_internal_customers$billed_amount[4]
  x$top_internal_customer_5 = top_internal_customers$account_id[5]
  x$top_internal_customer_5_revenue = top_internal_customers$billed_amount[5]
  
  x$week <- NULL
  return (x) 
}





countRegionCustomer <- function(week, year) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  meter <- getWeeklyMetering(week, year)
  charge_account <- getWeeklyCharges(week, year)$account_id
  testAccount <- getTestAccounts()
  
  #Clean up the usaage date by excluding all the suspended accounts (accounts that are not in charge data)
  meter <- subset(meter, !(account_id %in% testAccount$account_id))
  meter <- subset(meter, account_id %in% charge_account)
  
  meter <- meter[meter$is_internal_flag=="N", ]
  
  region_customers <- aggregate(account_id ~ region, meter, function(x) length(unique(x)))
  
  x <- data.frame(week = week)
  for (i in 1:nrow(region_customers)) { 
    vname <- paste(region_customers[i,]$region, "_customer_count", sep = "")
    x[, vname] <- region_customers[i,]$account_id
  }
  x$week <- NULL
  
  return (x)
}

countRegionRevenue <- function(week, year) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  charge <- getWeeklyCharges(week, year)
  testAccount <- getTestAccounts()
  
  #Clean up the usaage date by excluding all the suspended accounts (accounts that are not in charge data)
  charge <- subset(charge, !(account_id %in% testAccount$account_id))
  
  charge <- charge[charge$is_internal_flag=="N", ]
  
  region_revenue <- aggregate(billed_amount ~ region, charge, sum)
  
  x <- data.frame(week = week)
  for (i in 1:nrow(region_revenue)) { 
    vname <- paste(region_revenue[i,]$region, "_revenue", sep = "")
    x[, vname] <- region_revenue[i,]$billed_amount
  }
  x$week <- NULL
  
  return (x)
}


countInstanceUsage <- function(week, year) {
  
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  meter <- getWeeklyMetering(week, year)
  charge_account <- getWeeklyCharges(week, year)$account_id
  testAccount <- getTestAccounts()
  
  #Clean up the usaage date by excluding all the suspended accounts (accounts that are not in charge data)
  meter <- subset(meter, !(account_id %in% testAccount$account_id))
  meter <- subset(meter, account_id %in% charge_account)
  
  meter <- transform(meter, usage=colsplit(meter$usage_type, ":", names=c("1","2")))
  meter$usage.1 <- NULL
  names(meter)[names(meter) == 'usage.2'] <- 'instance_type'
  
  dt_external <- subset(meter, is_internal_flag == "N")
  
  ag_external <- aggregate(sum_usage_value ~ instance_type, dt_external, sum)
  
  x <- data.frame(week = week)
  for (i in 1:nrow(ag_external)) { 
    vname <- paste(ag_external[i,]$instance_type, "_hours", sep = "")
    x[, vname] <- ag_external[i,]$sum_usage_value
  }
  x$week <- NULL
  
  return (x) 
}

countEC2 <- function(week, year) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  testAccount <- getTestAccounts()
  x <- data_frame(week = week)
  
  conn <- dbConnect(driver, url)
  
  # Get EC2 developer count
  SQL <- paste("SELECT count(DISTINCT acct_dim.account_id)
               FROM a9cs_metrics.fact_aws_weekly_est_revenue f
               INNER JOIN a9cs_metrics.DIM_AWS_ACTIVITY_TYPES DIM_ACT ON F.ACTIVITY_TYPE_ID = DIM_ACT.ACTIVITY_TYPE_ID
               INNER JOIN a9cs_metrics.DIM_EC2_ACCOUNTS acct_dim ON f.account_seq_id = acct_dim.account_seq_id
               WHERE
               DIM_ACT.BIZ_PRODUCT_GROUP = 'EC2'
               AND DIM_ACT.BIZ_PRODUCT_NAME IN ('EC2 Instance Usage', 'EC2 RI Leases')  -- EC2 Box Usage and Rerservations
               AND f.week_begin_date='2015-05-24'        -- Change the Week Begin Date
               AND acct_dim.is_internal_flag='N'         -- External accounts only
               AND acct_dim.is_fraud_flag='N'            -- Non Fraud accounts only
               AND f.is_compromised_flag='N'             -- Non Compromised usage only
               AND acct_dim.account_status_code='Active' -- taking Active instead of Non-Suspended, as we have N/A and Dev Token", sep = "")
  
  message("Getting EC2 developer count. ")
  x$ec2_count <- dbGetQuery(conn, SQL)$count
  
  message("- Query succeed: ec2 dev count retrieved.")
  dbDisconnect(conn)
  
  x$week <- NULL
  return (x)
}


calcTop100 <- function(week, year, customerList) {
  if (missing(customerList)) {
    message("Please pass in customer data")
    return (0)
  }
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  rev <- count4weekRevenue(week, year, customerList)
  
  rev <- rev[order(-rev$charge_0),]
  rev <- head(rev, 100)
  
  
  colnames(rev)[which(names(rev) == "charge_0")] <- as.character(week)
  colnames(rev)[which(names(rev) == "charge_1")] <- as.character(week-1)
  colnames(rev)[which(names(rev) == "charge_2")] <- as.character(week-2)
  colnames(rev)[which(names(rev) == "charge_3")] <- as.character(week-3)
  
  rev <- rev[, c("account_id", 
                 "company", 
                 as.character(week-3), 
                 as.character(week-2), 
                 as.character(week-1),
                 as.character(week))]
  
  
  
  write.csv(rev, customerFilePath)
}

count4weekRevenue <- function(week, year, customerList) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  if (missing(customerList)) {
    message("Please pass in customer data")
    return (0)
  }
  
  testAccount <- getTestAccounts()
  x <- data_frame(week = week)
  
  cust <- customerList[c("account_id", "company", "payer_account_id")]
  rm(customerList)
  
  t1 <- getWeeklyCharges(week - 3)
  t1 <- t1[t1$is_internal_flag=="N",]
  t1 <- t1[!(t1$account_id %in% testAccount$account_id), ]
  
  t2 <- getWeeklyCharges(week - 2)
  t2 <- t2[t2$is_internal_flag=="N",]
  t2 <- t2[!(t2$account_id %in% testAccount$account_id), ]
  
  t3 <- getWeeklyCharges(week - 1)
  t3 <- t3[t3$is_internal_flag=="N",]
  t3 <- t3[!(t3$account_id %in% testAccount$account_id), ]
  
  t4 <- getWeeklyCharges(week)
  t4 <- t4[t4$is_internal_flag=="N",]
  t4 <- t4[!(t4$account_id %in% testAccount$account_id), ]
  
  t1 <- aggregate(billed_amount ~ account_id, data = t1, sum)
  names(t1)[2] <- "charge_3"
  t2 <- aggregate(billed_amount ~ account_id, data = t2, sum)
  names(t2)[2] <- "charge_2"
  t3 <- aggregate(billed_amount ~ account_id, data = t3, sum)
  names(t3)[2] <- "charge_1"
  t4 <- aggregate(billed_amount ~ account_id, data = t4, sum)
  names(t4)[2] <- "charge_0"
  
  
  chrg <- merge(t1, t2, by="account_id", all=TRUE)
  chrg <- merge(chrg, t3, by="account_id", all=TRUE)
  chrg <- merge(chrg, t4, by="account_id", all=TRUE)
  chrg <- merge(chrg, cust, by="account_id", all.x=TRUE)
  chrg <- chrg[!is.na(chrg$company), ]
  
  chrg <- within(chrg, account_id <- ifelse(!is.na(payer_account_id),payer_account_id,account_id))
  
  chrg[is.na(chrg)] <- 0
  
  chrg <- aggregate(cbind(charge_0, charge_1, charge_2, charge_3) ~ account_id + company, data = chrg, sum)
  
  
  return (chrg)
  
}

calcGainer <- function(week, year, customerList) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  if (missing(customerList)) {
    message("Please pass in customer data")
    return (0)
  }
  
  rev <- count4weekRevenue(customerList = customerList)
  
  rev$diff <- rev$charge_0 - rev$charge_1
  
  rev <- rev[order(-rev$diff),]
  
  gainer <- head(rev, 30)
  
  gainer <- gainer[, c(1, 2, 7, 5, 6, 3, 4, 5, 6)]
  
  sepLine <- data.frame(account_id = "------------",
                        company = "------------",
                        charge_0 = 0, 
                        charge_1 = 0, 
                        charge_2 = 0, 
                        charge_3 = 0, 
                        diff = 0)
  
  loser <- tail(rev, 30)
  loser <- loser[order(loser$diff), ]
  
  chrg <- bind_rows(gainer, sepLine, loser)
  # write to csv
  
  write.csv(chrg, gainerFilePath)
  return (chrg)
  
}

calcCredits <- function(week, year, customerList) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  if (missing(customerList)) {
    message("Please pass in customer data")
    return (0)
  }
  
  
  testAccount <- getTestAccounts()
  x <- data_frame(week = week)
  
  charge <- getWeeklyCharges(week = week, year = year)
  charge <- charge[!(charge$account_id %in% testAccount$account_id), ]
  
  charge <- charge[!is.na(charge$credit_id), ]
  
  credits <- merge(charge, customerList, by=c("account_id"), all.x=TRUE)
  credits<- within(credits, account_id <- ifelse(!is.na(payer_account_id),payer_account_id,account_id))
  
  credits <- aggregate(charge$billed_amount, by = list(credits$account_id, credits$charge_item_desc), FUN = sum)
  names(credits)[1] <- "account_id"
  names(credits)[2] <- "charge_item_desc"
  names(credits)[3] <- "billed_amount"
  
  credits <- merge(credits, customerList, by=c("account_id"), all.x=TRUE)
  
  credits <- credits[, c("account_id", "company", "charge_item_desc", "billed_amount")]
  
  credits <- credits[order(credits$billed_amount), ]
  total_credits <- sum(credits$billed_amount)
  credits <- head(credits, 15)
  other_credits <- total_credits - sum(credits$billed_amount)
  
  sum_row <- data.frame(account_id = "",
                        company = "other",
                        charge_item_desc = "",
                        billed_amount = other_credits)
    
  credits <- bind_rows(credits, sum_row)
  
  #write to weekly report
  write.csv(credits, creditsFilePath)
  
  return (credits)
  
}

# the pricing info for new regions are missing. Need to add price for 10+th region.
# Many usage types does NOT have a price tag associated, and were NOT counted as cost

countCost <- function(week, year) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  #Get price
  prices <- read.csv(priceFilePath)
  
  priceBase <- prices[, c(1:3)]
  
  priceList <- data.frame()
  for (i in c(4:length(prices))) {
    region <- data.frame(region = colnames(prices)[i])
    price <- merge(region, prices[, i], all = TRUE)
    price <- cbind(priceBase, price)
    colnames(price)[5] <- "price"
    priceList <- rbind(priceList, price)
  }
  
  
  SQL <- paste("select year, week, client_product_code, account_id, usage_type, usage_value, billed_amount 
               from a9cs_metrics.es_cs_weekly_charges
               where year = ", year, "
               and week = ", week, sep = "")
  
  
  conn <- dbConnect(driver, url)
  message("Getting AES usage cost data. ")
  t <- dbGetQuery(conn, SQL)
  
  t <- aggregate(cbind(usage_value, billed_amount) ~ client_product_code + usage_type, data = t, sum)
  t <- t[order(t$client_product_code), ]
  
  message("- Query succeed: AES usage cost retrieved.")
  dbDisconnect(conn)
  
  # calculate EC2 cost  --- Complete
  # aligned with Yana's model, but Yana's model doesn't include region DUB/EU (it has region EUW1 instead of EU), while this model has
  usage <- t[t$client_product_code == "AmazonEC2", ]
  ec2_usage <- filter(usage, grepl("BoxUsage", usage_type))
  ec2_usage <- transform(ec2_usage, usage=colsplit(ec2_usage$usage_type, ":", names=c("1","2")))
  ec2_usage$usage_type <- NULL
  
  colnames(ec2_usage)[which(names(ec2_usage) == "usage.1")] <- "region_str"
  colnames(ec2_usage)[which(names(ec2_usage) == "usage.2")] <- "usage_type"
  ec2_usage <- transform(ec2_usage, region=colsplit(ec2_usage$region_str, "-", names=c("1","2")))
  colnames(ec2_usage)[which(names(ec2_usage) == "region.1")] <- "region"
  ec2_usage <- within(ec2_usage, region <- ifelse(nchar(region.2)==0, "", region))
  ec2_usage$region <- translateRegion(ec2_usage$region)
  
  costs <- merge(ec2_usage, priceList, by = c("region", "usage_type", "client_product_code"), all.x = TRUE)
  ec2_cost <- sum(costs$price * costs$usage_value)
  
  # calculate EBS cost -- Only VolumeUsage types are counted. !!  
  # completely aligned with Yana's model
  ebs_usage <- filter(usage, grepl("VolumeUsage", usage_type, ignore.case = FALSE))
  ebs_usage <- transform(ebs_usage, tmp=colsplit(ebs_usage$usage_type, ":", names=c("1","2")))
  colnames(ebs_usage)[which(names(ebs_usage) == "tmp.1")] <- "region_str"
  ebs_usage$usage_type <- ebs_usage$tmp.2
  
  ebs_usage <- transform(ebs_usage, region=colsplit(ebs_usage$region_str, "-", names=c("1","2")))
  colnames(ebs_usage)[which(names(ebs_usage) == "region.1")] <- "region"
  
  ebs_usage$region <- translateRegion(ebs_usage$region)
  
  costs <- merge(ebs_usage, priceList, by = c("region", "usage_type", "client_product_code"), all.x = TRUE)
  ebs_cost <- sum(costs$price * costs$usage_value)
  
  # calculate ELB cost
  # # Completely aligned with Yana's model
  elb_usage <- filter(usage, grepl("LoadBalancer|DataProcessing-Bytes", usage_type, ignore.case = FALSE))
  elb_usage <- transform(elb_usage, tmp=colsplit(elb_usage$usage_type, "-", names=c("1","2")))
  colnames(elb_usage)[which(names(elb_usage) == "tmp.1")] <- "region"
  elb_usage$region <- translateRegion(elb_usage$region)
  
  elb_usage <- within(elb_usage, tmp.2 <- ifelse(region == "IAD", usage_type, tmp.2))
  elb_usage$usage_type <- elb_usage$tmp.2
  costs <- merge(elb_usage, priceList, by = c("region", "usage_type", "client_product_code"), all.x = TRUE)
  elb_cost <- sum(costs$price * costs$usage_value)
  
  # calculate S3 cost  - ONLY TimedStorage-ByteHrs is counted.  
  # Completely aligned with Yana's model
  s3_usage <- t[t$client_product_code == "AmazonS3", ]
  s3_usage <- transform(s3_usage, tmp=colsplit(s3_usage$usage_type, "-", names=c("1","2")))
  colnames(s3_usage)[which(names(s3_usage) == "tmp.1")] <- "region"
  s3_usage$region <- translateRegion(s3_usage$region)
  s3_usage <- within(s3_usage, tmp.2 <- ifelse(region == "IAD", usage_type, tmp.2))
  s3_usage$usage_type <- s3_usage$tmp.2
  
  costs <- merge(s3_usage, priceList, by = c("region", "usage_type", "client_product_code"), all.x = TRUE)
  s3_cost <- sum(costs$price * costs$usage_value, na.rm = TRUE)
  
  # calculate DynamoDB cost  
  # Completely aligned with Yana's model
  dynamo_usage <- t[t$client_product_code == "AmazonDynamoDB", ]
  dynamo_usage <- transform(dynamo_usage, tmp=colsplit(dynamo_usage$usage_type, "-", names=c("1","2")))
  colnames(dynamo_usage)[which(names(dynamo_usage) == "tmp.1")] <- "region"
  dynamo_usage$region <- translateRegion(dynamo_usage$region)
  dynamo_usage <- within(dynamo_usage, tmp.2 <- ifelse(region == "IAD", usage_type, tmp.2))
  dynamo_usage$usage_type <- dynamo_usage$tmp.2
  
  costs <- merge(dynamo_usage, priceList, by = c("region", "usage_type", "client_product_code"), all.x = TRUE)
  dynamo_cost <- sum(costs$price * costs$usage_value, na.rm = TRUE)
  
  
  #cost of monitor usage (logs & metrics)
  
  monitor_usage <- filter(t, grepl("MonitorUsage", usage_type, ignore.case = FALSE))
  
  monitor_usage <- transform(monitor_usage, tmp=colsplit(monitor_usage$usage_type, "-", names=c("1","2")))
  colnames(monitor_usage)[which(names(monitor_usage) == "tmp.1")] <- "region"
  monitor_usage$region <- translateRegion(monitor_usage$region)
  monitor_usage <- within(monitor_usage, tmp.2 <- ifelse(region == "IAD", usage_type, tmp.2))
  monitor_usage$usage_type <- gsub("CW:", "", monitor_usage$tmp.2)
  
  costs <- merge(monitor_usage, priceList, by = c("region", "usage_type", "client_product_code"), all.x = TRUE)
  monitor_cost <- sum(costs$price * costs$usage_value, na.rm = TRUE)
  
  #cost of SWF, cloudFront and Router53
  swf_cost <- 0.5 * sum(filter(t, client_product_code=="AmazonSWF")$billed_amount)
  cf_cost <- 0.5 * sum(filter(t, client_product_code=="AmazonCloudFront")$billed_amount)
  r53_cost <- 0.5 * sum(filter(t, client_product_code=="AmazonRoute53")$billed_amount)
  
  # AWSP, Bandwidth Costs
  # Completely aligned with Yana's model: 11% * (ec2_cost + ebs_cost + elb_cost + s3_cost)
  
  band_cost <- 0.11 * (ec2_cost + ebs_cost + elb_cost + monitor_cost + s3_cost)
  
  # Sales cost
  # Completely aligned with Yana's model: 4.3% * revenue
  charge_total <- getWeeklyCharges(week, year)
  testAccount <- getTestAccounts()
  
  charge_total <- subset(charge_total, !(account_id %in% testAccount$account_id))
  
  weekly_revenue <- sum(charge_total$billed_amount)
  
  sales_cost <- 0.043 * weekly_revenue
  
  #calculate the total cost
  x <- data.frame(overall_cost = sum(ec2_cost, 
                    ebs_cost, 
                    elb_cost, 
                    monitor_cost, 
                    s3_cost, 
                    dynamo_cost, 
                    swf_cost, 
                    cf_cost,
                    r53_cost,
                    band_cost, 
                    sales_cost))
  # Calculate margin, 
  # Following the assumption in Yana's model:
  # The cost for internal & external customer are propotional to their revenue
  x$overall_margin <- 1 - x$overall_cost / weekly_revenue
  
  
  return (x)
}

calcWeeklyStats <- function(week, year) {

  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  #worksheet <- "weekly_metrics.csv"
  
  if(file.exists(metricsFilePath)) {
    # read from worksheet
    #t <- read.xlsx(reportFilePath, sheetName = worksheet)
    t <- read.csv(metricsFilePath)
    t <- data.frame(t(t))
    colnames(t) <- as.matrix(t[1,])
    t <- filter(t, week != "week")
    
    x <- t[t$week == week, ]
    if (nrow(x) > 0) {
      return (x)
    }
  } else {
    t <- data.frame()
  }
  
  x <- data.frame(year = year, week = week)
  
  x <- cbind(x, countDomains(week = week, year = year))
  x <- cbind(x, countCustomers(week = week, year = year))
  x <- cbind(x, countRevenue(week = week, year = year, customerList = cust))
  x <- cbind(x, countInstanceUsage(week = week, year = year))
  x <- cbind(x, countRegionCustomer(week = week, year = year))
  x <- cbind(x, countRegionRevenue(week = week, year = year))
  x <- cbind(x, countEC2(week = week, year = year))
  x <- cbind(x, countCost(week = week, year = year))
  
  x <- lapply(x, factor)
  t <- bind_rows(t, x)
  t <- t[order(t$year, t$week), ]
  
  #write to weekly report
  write.csv(x=t(t), file=metricsFilePath)
  #write.xlsx2(t(t), reportFilePath, sheetName = "weekly_metrics", append = TRUE)
  
  return (x)
  
}


calcWeeklyStatsN <- function(week, year, nWeek = 1) {
  
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  t <- data.frame()
  for (i in 0:(nWeek - 1)) {
    x <- calcWeeklyStats(week = (week - i), year = year)
    
    t <- bind_rows(t, x)
    t <- t[order(t$year, t$week), ]
  }
  return (t)
}
  
