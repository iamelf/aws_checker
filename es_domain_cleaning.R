# Some MacOS security feature prevent this library being loaded. The following line is a quick hack.
# http://charlotte-ngs.github.io/2016/01/MacOsXrJavaProblem.html
Sys.setenv(JAVA_HOME='/Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/jre')
dyn.load('/Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/jre/lib/server/libjvm.dylib')

library(RJDBC)
library(dplyr)
library(jsonlite)
library(elastic)
library(stringr)
library(reshape2)
library(openxlsx)
library(lubridate)

weeknum <- function(week, year, weekDiff) {
  if (missing(week) || missing(year)) {
    dateStr <- Sys.Date()
  } else {
    dateStr <- as.Date(paste("2", week, year, sep = "-"), format = "%w-%W-%Y")
  }
  if (missing(weekDiff)) {
    weekDiff <- 0
  }
  
  x <- as.Date(dateStr) + weekDiff * 7
  
  t <- as.POSIXlt(x)
  
  week <- as.numeric(strftime(t,format="%W"))
  year <- as.numeric(strftime(t,format="%Y"))
  x <- data.frame(week = week,
                  year = year)
  
  return (x)
}

# weeknum <- function(dateStr, diff) {
#   if (missing(dateStr)) {
#     dateStr <- Sys.Date()
#   }
#   if (missing(diff)) {
#     diff <- 0
#   }
#   
#   t <- as.POSIXlt(dateStr)
#   
#   week <- as.numeric(strftime(t,format="%W"))
#   
#   
#   return (week)
# }


# !!! A9 cluster has been retired !!!
#url <- "jdbc:redshift://a9dba-fin-rs2.db.amazon.com:8192/a9aws?user=maghuang&password=pw20160926NOW&tcpKeepAlive=true&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"


#AWS database
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-1.1.9.1009.jar", identifier.quote="`")
url <- "jdbc:postgresql://54.85.28.62:8192/datamart?user=maghuang&password=Youcan88$&tcpKeepAlive=true"


rootPath <- "/Users/maghuang/aws/weekly_report"

# Input files
inputPath <- file.path(rootPath, "_input")

priceFilePath <- file.path(inputPath, "es_prices.csv")
testAccountFilePath <- file.path(inputPath, "test_accounts.csv")
goalFile <- paste("es_goal_", as.integer(format(Sys.Date(), "%Y")), ".csv", sep="")
goalFilePath <- file.path(inputPath, goalFile)
ec2countFilePath <- file.path(inputPath, "es_ec2count.csv")
metricsFilePath <- file.path(inputPath, "es_weekly_metrics.csv")
reportTemplatePath <- file.path(inputPath, "Elasticsearch-metrics-report-template.xlsx")

main <- function(week, year) {
  
  if (missing(week)) {
    week = weeknum(weekDiff = -1)$week
  }
  
  if (missing(year)) {
    year = weeknum(weekDiff = -1)$year
  }
  
  
  # Output files
  outputPath <- file.path(rootPath, paste("week", week, sep=""))
  if(!dir.exists(outputPath)) {
    dir.create(outputPath)
  }
  
  outputPath <- file.path(outputPath, "Elasticsearch")
  if(!dir.exists(outputPath)) {
    dir.create(outputPath)
  }
  
  # global variable to store customer list, so each function doesn't need to retrieve it everytime. 
  
  if(!exists("customerList")) {
    customerList <<- getCustomerData()
  }
  
  if(!exists("testAccount")) {
    testAccount <<- getTestAccounts()
  }
  
  if(!exists("qwikLabsAccount")) {
    qwikLabsAccount <<- getQwikLabsAccounts()
  }
  
 
  return (0)
  
}

clearEnv <- function()  {
  #clean global variables.
  vnames <- grep("chargeWeek|meterWeek", ls(envir = .GlobalEnv), value = T)
  for (i in vnames) {
    cmd <- paste("rm (", i, ")", sep = "")
    eval(parse(text = cmd), envir = .GlobalEnv)
  }
  
  # Remove the buffer file for last week
  dataPath <- file.path(rootPath, "data_buffer")
  
  fname <- paste("WeeklyCharge_wk", weeknum(weekDiff = -1)$week, ".csv", sep="")
  fpath <- file.path(dataPath, fname);
  
  if(file.exists(fpath) & file.size(fpath) < 100000) {
    file.remove(fpath)
  }
  
  fname <- paste("WeeklyMetering_wk", weeknum(weekDiff = -1)$week, ".csv", sep="")
  fpath <- file.path(dataPath, fname);
  
  if(file.exists(fpath) & file.size(fpath) < 100000) {
    file.remove(fpath)
  }
}




translateRegion <- function (alias) {
  if (missing(alias)) {
    message("Please pass in a region alias!")
    return (-1)
  }
  regions <- data.frame(location = c("", "USW2","USW1", "EU", "APN1", "APS1", "APS2",	"APS3", "SAE1", "EUC1", "APN2", "USE2"),
                        airport = c("IAD","PDX","SFO","DUB", "NRT", "SIN", "SYD", "BOM", "GRU", "FRA", "ICN", "CMH"))
  
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

# QwikLabs is excluded from the domain counts, but it's not excluded from the revenue. 
getQwikLabsAccounts <- function() {
  x <- filter(customerList, grepl("qwikLABS|Cloud vLab", company, ignore.case = TRUE))$account_id
  return (x)
}

getTestAccounts <- function() {
  
  t <- read.csv(testAccountFilePath)
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  
  # lab <- data.frame(account_id = getQwikLabsAccounts())
  # lab$Type <- "test"
  # 
  # t <- bind_rows(t, lab)
  
  return (t)
}


getCustomerData <- function () {
  
  dataPath <- file.path(rootPath, "data_buffer")
  
  if (!dir.exists(dataPath)) {
    dir.create(dataPath)
  }
  
  fname <- paste("CustomerData-wk", weeknum()$week,".csv", sep="")
  fpath <- file.path(dataPath, fname)
  
  if(file.exists(fpath)) {
    message("Reading customer data from file: ", fpath)
    t <- read.csv(fpath, sep=",")
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    t$payer_account_id <- str_pad(as.character(t$payer_account_id), 12, pad="0")
  } else {
    conn <- dbConnect(driver, url)
    message("- Connected to DB.")
    
    # SQL <- "WITH account
    # AS (SELECT DISTINCT account_id, payer_account_id, is_internal_flag
    # FROM   dim_aws_accounts
    # WHERE  end_effective_date IS NULL
    # AND    current_record_flag = 'Y')   -- tt33060829 dupe stop
    # SELECT aa.account_id,
    # coalesce(bb.account_field_value, 'e-mail Domain:'
    # || aa.email_domain) AS company,
    # aa.clear_name name,
    # aa.clear_lower_email email,
    # account.is_internal_flag,
    # account.payer_account_id
    # FROM   t_customers aa
    # left outer join o_aws_account_field_values bb
    # ON ( aa.account_id = bb.account_id
    # AND bb.account_field_id = 2 )
    # left outer join account
    # ON ( account.account_id = aa.account_id ) WHERE  aa.account_id IN (SELECT DISTINCT account_id
    # FROM   o_aws_subscriptions
    # -- WHERE  offering_id IN ( '26555', '113438', '128046' )
    # WHERE  offering_id IN ('607996', '607997', '615324', '615325', '729920' )
    # AND ( end_date IS NULL
    # OR end_date >= ( SYSDATE - 9 )));"
    
    SQL <- "select aa.payer_account_id,
    company_name as company,
    customer_clear_lower_email as email,
    CASE WHEN internal= 'external' then 'N' else 'Y' END as is_internal_flag
    from public.account_dm aa join (
    select distinct
    payer_account_id
    from public.account_dm aa join  awsdw_ods.o_aws_subscriptions k on k.account_id = aa.account_id
    and offering_id IN ('607996', '607997', '615324', '615325', '729920' )
    and NVL(end_date,sysdate) >= ( SYSDATE - 9 ) ) p
    on p.payer_account_id = aa.payer_account_id; "
    
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
    week = weeknum(weekDiff = -1)$week
  }
  
  if (missing(year)) {
    year = weeknum(weekDiff = -1)$year
  }
  message("- Getting weekly charge data for week ", week)
  
  vname <- paste("chargeWeek", week, sep="")
  if (exists(vname)) {
    return (eval(as.symbol(vname)))
  }
  
  dataPath <- file.path(rootPath, "data_buffer")
  
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
                 where year = '", year, "' AND week='", week,"'", sep="")
    conn <- dbConnect(driver, url)
    
    message("- Running query: weekly charge data...")
    t <- dbGetQuery(conn, SQL)
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    dbDisconnect(conn)
    write.csv(t, fpath)
  }
  t <- subset(t, !(account_id %in% testAccount$account_id))
  
  assign(vname, t, envir = .GlobalEnv)
  message("Charge data for week ", week, " retrieved.")
  
  return(t)
}

getWeeklyMetering <- function(week, year) {
  
  if (missing(week)) {
    week = weeknum(weekDiff = -1)$week
  }
  
  if (missing(year)) {
    year = weeknum(weekDiff = -1)$year
  }
  
  vname <- paste("meterWeek", week, sep="")
  if (exists(vname)) {
    return (eval(as.symbol(vname)))
  }
  
  message("- Getting weekly metering data for week ", week)
  
  dataPath <- file.path(rootPath, "data_buffer")
  
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
    
    message("- Running query: weekly metering data for week ", week)
    t <- dbGetQuery(conn, SQL)
    dbDisconnect(conn)
    
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    t <- subset(t, !(account_id %in% testAccount$account_id))
    
    write.csv(t, fpath)
  }
  
  assign(vname, t, envir = .GlobalEnv)
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
# Currently, when counting customer with 1 or more domains,
# we're using the account # from the weekly charge data, and exclude the "credit only" accounts, which has 0 domains

getInvalidDomains <- function(week, year) {
  
  if (missing(week)) {
    week = weeknum(weekDiff = -1)$week
  }
  
  if (missing(year)) {
    year = weeknum(weekDiff = -1)$year
  }
  
  x <- data.frame(week = week)
  
  meter <- getWeeklyMetering(week, year)
  charge_account <- getWeeklyCharges(week, year)$account_id
  
  meter <- subset(meter, !(account_id %in% testAccount$account_id))
  meter <- subset(meter, !(account_id %in% qwikLabsAccount))
  
  meter <- subset(meter, !account_id %in% charge_account)
  meter <- filter(meter, grepl("ESInstance", usage_type))
  meter <- transform(meter, usage=colsplit(meter$usage_type, ":", names=c("1","2")))
  
  meter$usage_type <- meter$usage.2
  
  priceList <- getCostPrice()
  
  meter <- merge(meter, priceList, by=c("usage_type", "region"), all.x = T)
  meter$cost <- meter$price * meter$sum_usage_value
  
  # meter <- merge(meter, customerList, by=c("account_id"), all.x = T)
  # meter <- filter(meter, is.na(company))
  
  meter$account_id <- str_pad(as.character(meter$account_id), 12, pad="0")
  meter$usage.1 <- NULL
  meter$usage.2 <- NULL
  meter$product <- NULL
  meter$X <- NULL
  meter$X.x <- NULL
  meter$X.y <- NULL
  meter$client_product_code <- NULL
  
  x <- getCustomerStatus(meter)
  meter <- merge(meter, x, by=c("account_id"), all = T)
  
  meter <- filter(meter, account_status_code == "Suspended")  
  
  y <- getSubscriptionStatus(meter)
  meter <- merge(meter, y, by = c("account_id"), all = T)
  
  
  write.csv(meter, "/Users/maghuang/Desktop/invalid_domains/es_invalid_domains.csv", row.names = F)
  return (meter)
  
}

getCustomerStatus <- function(dt) {
  
  all_accounts <- dt$account_id
  
  queryStr <- paste("'", all_accounts[1], "'", sep = "")
  for (i in all_accounts) {
    queryStr<- paste(queryStr, ", '", i, "'", sep = "")
  }
  
  SQL <- paste("select * from awsdw_dm_billing.dim_aws_accounts 
               where account_id in (", queryStr,")", sep = "");
  
  conn <- dbConnect(driver, url)
  
  message("- Running query: get customer status... ")
  t <- dbGetQuery(conn, SQL)
  dbDisconnect(conn)
  
  activeRec <- filter(t, account_status_code == "Active")
  activeRec$end_effective_date <- ifelse(is.na(activeRec$end_effective_date), as.character(Sys.Date()), as.character(activeRec$end_effective_date))
  activeRec <- aggregate(end_effective_date~account_id, data = activeRec, max)
  colnames(activeRec)[which(names(activeRec) == "end_effective_date")] <- "lastActiveDate"
  
  
  suspendRec <- filter(t, account_status_code == "Suspended")
  suspendRec$start_effective_date <- as.character(suspendRec$start_effective_date)
  suspendRec <- aggregate(start_effective_date ~ account_id, data = suspendRec, min)
  colnames(suspendRec)[which(names(suspendRec) == "start_effective_date")] <- "firstSuspendDate"
  
  rec <- merge(activeRec, suspendRec, by = c("account_id"), all = T)
  
  rec$lastActiveDate <- ifelse(is.na(rec$lastActiveDate), rec$firstSuspendDate, rec$lastActiveDate)
  # rec$lastActiveDate <- as.Date(rec$lastActiveDate)
  
  rec$suspendedDate <- ifelse(rec$lastActiveDate > rec$firstSuspendDate, rec$lastActiveDate, rec$firstSuspendDate)
  # rec$suspendedDate <- as.Date(rec$suspendDate)
  
  rec$suspendedDays <- Sys.Date() - as.Date(rec$suspendedDate)
  
  statusRec <- filter(t, current_record_flag == "Y")[,c("account_id", "account_status_code")]
  statusRec <- merge(statusRec, rec, by = c("account_id"), all.x = T)
  statusRec$lastActiveDate <- NULL
  statusRec$firstSuspendDate <- NULL
  
  return(statusRec)
  
}

getSubscriptionStatus <- function(dt) {
  
  all_accounts <- dt$account_id
  
  queryStr <- paste("'", all_accounts[1], "'", sep = "")
  for (i in all_accounts) {
    queryStr<- paste(queryStr, ", '", i, "'", sep = "")
  }
  
  SQL <- paste("select * from awsdw_ods.o_aws_subscriptions
               where offering_id in ('607997', '607996', '615324','615325','729920')
               and account_id in (", queryStr,")", sep = "");
  
  conn <- dbConnect(driver, url)
  
  message("- Running query: get customer status... ")
  t <- dbGetQuery(conn, SQL)
  dbDisconnect(conn)
  
  subRec <- aggregate(end_date ~ account_id, t, max)
  colnames(subRec)[which(names(subRec) == "end_date")] <- "subscription_end_date"
  subRec$unsubscribedDays <- Sys.Date() - as.Date(subRec$subscription_end_date)
  
  return(subRec)
  
}


domainTrend <- function() {
  m <- data.frame()
  for (i in c(30:50)) {
    t <- getInvalidDomains(week =i, year = 2016)
    internal <- filter(t, is_internal_flag.x == "Y")
    external <- filter(t, is_internal_flag.x == "N")
    internal_cost <- sum(internal$cost, na.rm = T)
    external_cost <- sum(external$cost, na.rm = T)
    x <- data.frame(week = i, 
                    internal_count = nrow(unique(internal[, c("account_id", "usage_resource")])),
                    external_count = nrow(unique(external[, c("account_id", "usage_resource")])),
                    internal_cost = internal_cost,
                    external_cost = external_cost)
    m<-rbind(m, x)
  }
  return(m)
}

getCostPrice <- function() {
  
  
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
  return (priceList)
  
}

