# Some MacOS security feature prevent this library being loaded. The following line is a quick hack.
# http://charlotte-ngs.github.io/2016/01/MacOsXrJavaProblem.html
Sys.setenv(JAVA_HOME='/Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/jre')
dyn.load('/Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/jre/lib/server/libjvm.dylib')

library(RJDBC)
library(dplyr)
library(jsonlite)
library(stringr)
library(reshape2)
library(lubridate)

weeknum <- function(dateStr=Sys.Date()) {
  
  t <- as.POSIXlt(dateStr)
  t <- strftime(t,format="%W")
  return (as.numeric(t))
}



monthnum <- function(dateStr=Sys.Date()) {
  
  t <- as.POSIXlt(dateStr)
  t <- strftime(t,format="%m")
  return (as.numeric(t))
}

#A9 cluster
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-1.1.9.1009.jar", identifier.quote="`")
url <- "jdbc:redshift://a9dba-fin-rs2.db.amazon.com:8192/a9aws?user=maghuang&password=pw20160926NOW&tcpKeepAlive=true&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"

#AWS database
#url <- "jdbc:postgresql://54.85.28.62:8192/datamart?user=maghuang&password=Youcan88$&tcpKeepAlive=true"


rootPath <- "/Users/maghuang/aws/weekly_report"

# Input files
inputPath <- file.path(rootPath, "_input")

priceFilePath <- file.path(inputPath, "es_prices.csv")
testAccountFilePath <- file.path(inputPath, "test_accounts.csv")
goalFile <- paste("es_goal_", as.integer(format(Sys.Date(), "%Y")), ".csv", sep="")
goalFilePath <- file.path(inputPath, goalFile)
ec2countFilePath <- file.path(inputPath, "es_ec2count.csv")
metricsFilePath <- file.path(inputPath, "es_weekly_metrics.csv")
#reportTemplatePath <- file.path(inputPath, "Elasticsearch-metrics-report-template.xlsx")


getPriceList <- function() {
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



clearEnv <- function()  {
  #clean global variables.
  vnames <- grep("chargeWeek|meterWeek", ls(envir = .GlobalEnv), value = T)
  for (i in vnames) {
    cmd <- paste("rm (", i, ")", sep = "")
    eval(parse(text = cmd), envir = .GlobalEnv)
  }
  
  # Remove the buffer file for last week
  dataPath <- file.path(rootPath, "data_buffer")
  
  fname <- paste("WeeklyCharge_wk", weeknum()-1, ".csv", sep="")
  fpath <- file.path(dataPath, fname);
  
  if(file.exists(fpath) & file.size(fpath) < 100000) {
    file.remove(fpath)
  }
  
  fname <- paste("WeeklyMetering_wk", weeknum()-1, ".csv", sep="")
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

getQwikLabsAccounts <- function() {
  x <- filter(customerList, grepl("qwikLABS|Cloud vLab", company, ignore.case = TRUE))$account_id
  return (x)
}

getTestAccounts <- function() {
  
  t <- read.csv(testAccountFilePath)
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  
  lab <- data.frame(account_id = getQwikLabsAccounts())
  lab$Type <- "test"
  
  t <- bind_rows(t, lab)
  
  return (t)
}

getCustomerData <- function () {
  
  dataPath <- file.path(rootPath, "data_buffer")
  
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
    OR end_date >= ( SYSDATE - 9 )));"
    
    # SQL <- "select aa.account_id as account_id,
    # payer_account_id,
    # company_name as company,
    # customer_clear_lower_email as email,
    # CASE WHEN internal= 'external' then 'N' else 'Y' END as is_internal_flag 
    # from public.account_dm aa join  (SELECT DISTINCT account_id
    # FROM   awsdw_ods.o_aws_subscriptions
    # WHERE  offering_id IN ( '26555', '113438', '128046' )
    # AND ( end_date IS NULL
    # OR end_date >= ( SYSDATE - 9 ) )) k on k.account_id = aa.account_id"
    
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


getFreetierMonthlyCharges <- function(month, year) {
  if (missing(month)) {
    month = monthnum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  message("- Getting weekly charge data for month ", month)
  
  dateStr <- paste(str_pad(year, 4, pad = "0"), "-", str_pad(month, 2, pad = "0"), sep = "")
    SQL <- paste("select account_id, 
                 is_internal_flag, 
                 billed_amount, 
                 usage_type, 
                 usage_value,
                 price_per_unit,
                 region, 
                 credit_id,
                 charge_item_desc
                 from a9cs_metrics.es_daily_charges
                 where is_internal_flag = 'N'
                 and usage_type like '%t2.micro%' and billed_amount = 0 
                 and computation_date like '%", dateStr, "%'", sep="")
    conn <- dbConnect(driver, url)
    
    message("- Running query: weekly charge data...")
    t <- dbGetQuery(conn, SQL)
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    dbDisconnect(conn)
    
  t <- subset(t, !(account_id %in% testAccount$account_id))
  
  message("Charge data for month ", month, " retrieved.")
  
  return(t)
}

getWeeklyMetering <- function(week, year) {
  
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
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
# 
# getPricePlan <- function(id) {
#   planPath <- "/Users/maghuang/aws/price"
#   planFile <- c("F4468EE8-free.csv", 
#                 "FF9EADEE-nonfree.csv",
#                 "8BD4758E-AISPLnonfree.csv",
#                 "7A1CF623-AISPLfree.csv",
#                 "AC90A348-custom.csv")
#   
#   planFile <- file.path(planPath, planFile[id]);
#   
#   t <- read.csv(planFile)
#   
# }

fpath <- "/Users/maghuang/aws/freetier_cost.csv"
testAccount <-  getTestAccounts()

calcMonthlyFreetierCost <- function(month, year) {
  
  if (missing(month)) {
    month = monthnum() - 1
  }
  
  if (missing(year)) {
    year = as.integer(format(Sys.Date(), "%Y"))
  }
  
  if (file.exists(fpath)) {
    t <- read.csv(fpath)
    x <- t[t$month == month, ]
    if (nrow(x) > 0) {
      return (x)
    }
  }
  
  freetier <- getFreetierMonthlyCharges(month)
  freetier <- transform(freetier, usage=colsplit(freetier$usage_type, ":", names=c("1","2")))
  freetier$usage_type <- NULL
  colnames(freetier)[which(names(freetier) == "usage.2")] <- "usage_type"
  freetier$usage.1 <- NULL
  
  freetier <- aggregate(usage_value ~ region, data = freetier, sum)
  
  prices <- getPriceList()
  prices <- prices[prices$usage_type == "t2.micro", ]
  
  
  freetier <- merge(freetier, prices, by=c("region"), all.x = T)
  freetier <- filter(freetier, !is.na(price))
 
  
  cost <- sum(freetier$usage_value * freetier$price)
  
  x <- data.frame(month = month,
                  cost = cost)
  return (x)
}

calcMonthlyTotal <- function(nMonth) {
  if (missing(nMonth)) {
    nMonth = monthnum() - 1
  }
  
  SQL <- paste("select substring(computation_date, 1, 7), sum(billed_amount) 
                from a9cs_metrics.es_daily_charges where is_internal_flag = 'N'
                group by 1", sep = "")
  
  conn <- dbConnect(driver, url)
  
  message("- Running query: monthly charges for the past ", nMonth, " months.")
  t <- dbGetQuery(conn, SQL)
  dbDisconnect(conn)
  
  return (t)
}

calcFreetier <- function(nMonth) {
  if (missing(nMonth)) {
    nMonth = monthnum() - 1
  }
  
  x <- data.frame()
  for ( i in c((monthnum() - nMonth) : (monthnum() - 1))) {
    t <- calcMonthlyFreetierCost(i, 2016)
    x <- rbind(x,t)
    
    write.csv(x, fpath, row.names = F)
  }

  x <- merge(x, total, by = c("month"), all.x = T)
  
  return (x)
  
}

getFreeTier <- function() {
  
  SQL <- paste("select account_id, week from a9cs_metrics.es_freetier", sep = "")
  
  conn <- dbConnect(driver, url)
  
  message("- Running query: weekly freetier customer list")
  t <- dbGetQuery(conn, SQL)
  dbDisconnect(conn)
  
  return (t)
}

getT2small <- function(week) {
  if (missing(week)) {
    week = weeknum() - 1
  }
  
  SQL <- paste("select account_id, 
               billed_amount, 
               usage_type, 
               usage_value,
               price_per_unit,
               week,
               region
               from a9cs_metrics.es_weekly_charges
               where is_internal_flag = 'N'
               and usage_type like '%t2.small%'
               and week = ", week, sep = "")
  
  conn <- dbConnect(driver, url)
  
  message("- Running query: weekly t2.small usage")
  t <- dbGetQuery(conn, SQL)
  dbDisconnect(conn)
  
  return (t)
}

calcT2smallCost <- function(nWeek) {
  if (missing(nWeek)) {
    nWeek = weeknum() - 1
  }
  freetier <- getFreeTier()
  
  free_amount <- 24 * 7 
  
  x <- data.frame()
  for ( i in c((weeknum() - nWeek) : (weeknum() - 1))) {
    t2 <- getT2small(i)
    freetier_week <- freetier[freetier$week == i, ]
    
    t2free <- filter(t2, account_id %in% freetier_week$account_id)
    prices <- unique(t2free[, c("region", "price_per_unit")])
    
    t2usage <- aggregate(usage_value ~ account_id + region, data = t2free, sum)
    t2usage <- merge(t2usage, prices, by = c("region"))
    t2usage$free_amount <- sapply(t2usage$usage_value, function(x) {if (x > free_amount) return(free_amount) else return(x)})
    
    cost <- sum(t2usage$free_amount * t2usage$price_per_unit)
    
    t <- data.frame(week = i, 
                    cost = cost, 
                    freetier_count = length(unique(freetier_week$account_id)),
                    t2small_user_count = length(unique(t2usage$account_id)))
    x <- rbind(x,t)
  }
  return (x)
  
}
