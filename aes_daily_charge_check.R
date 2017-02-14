# Some MacOS security feature prevent this library being loaded. The following line is a quick hack.
dyn.load('/Library/Java/JavaVirtualMachines/jdk1.8.0_121.jdk/Contents/Home/jre/lib/server/libjvm.dylib')

library(RJDBC)
library(dplyr)
library(jsonlite)
library(elastic)
library(stringr)
library(shiny)
library(reshape2)


# !!! A9 cluster has been retired !!!
#url <- "jdbc:redshift://a9dba-fin-rs2.db.amazon.com:8192/a9aws?user=maghuang&password=pw20160926NOW&tcpKeepAlive=true&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"

#AWS database
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-1.1.9.1009.jar", identifier.quote="`")
url <- "jdbc:postgresql://dbsm-redshift-prd.db.amazon.com:8192/datamart?user=maghuang&password=Youcan88$&tcpKeepAlive=true"


rootPath <- "/Users/maghuang/aws/weekly_report"

# Daily analysis 
# * daily external, internal revenue
# * Total credit daily
# * Top customers
# * Top gainers, top losers
# * Count of total paying customer accounts
# * Count of 0 revenue accounts
# * Single Customer usage trending


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

weekDates <- function(week, year) {
  if (missing(week) || missing(year)) {
    x <- weeknum(weekDiff = -1)
    week <- x$week
    year <- x$year
  }
  
  return(as.Date(paste(year, week, c(1:6), sep="-"), "%Y-%U-%u")-1)
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

  
# Refresh daily charge data for day N-
getDailyChargeN <- function (n) {
  for (i in c(n:1)) {
    getDailyCharge(format(Sys.Date()-i, "%Y-%m-%d"))
  }
}

dailyGainer <- function (dateStr) {
  if (missing(dateStr)) {
    t<- Sys.Date()-1
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dateStr)
      return()
    }
  }
  
  cust <- customerList[c("account_id", "company")]
  
  d1 <- format(t-1, "%Y-%m-%d")
  d2 <- format(t, "%Y-%m-%d")
  dateStr <- d2
  
  t1 <- getDailyCharge(d1)
  t1 <- t1[t1$is_internal_flag=="N",]
  t2 <- getDailyCharge(d2)
  t2 <- t2[t2$is_internal_flag=="N",]
  
  ag1 <- aggregate(t1$billed_amount, by=list(t1$account_id), FUN=sum)
  ag2 <- aggregate(t2$billed_amount, by=list(t2$account_id), FUN=sum)
  
  
  colnames(ag1)[1] <- "account_id"
  colnames(ag1)[2] <- "charge_yesterday"
  colnames(ag2)[1] <- "account_id"
  colnames(ag2)[2] <- "charge_today"
  
  chrg <- merge(ag1, ag2, by="account_id", all=TRUE)
  chrg <- merge(chrg, cust, by="account_id", all.x=TRUE)
  
  chrg[is.na(chrg)] <- 0
  
  chrg[,"diff"] <- chrg["charge_today"] - chrg["charge_yesterday"]
  chrg <- chrg[order(-chrg$diff),]
  chrg$date <- dateStr
  
  return (chrg)
}


dailyUsageDiff <- function (dateStr) {
  if (missing(dateStr)) {
    t<- Sys.Date()-1
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dateStr)
      return()
    }
  }
  
  d1 <- format(t-1, "%Y-%m-%d")
  d2 <- format(t, "%Y-%m-%d")
  dateStr <- d2
  
  t1 <- getDailyMetering(d1)
  #t1 <- t1[t1$is_internal_flag=="N",]
  t2 <- getDailyMetering(d2)
  #t2 <- t2[t2$is_internal_flag=="N",]
  
  ag1 <- aggregate(usage_value ~ account_id+usage_resource+usage_type, data = t1, FUN=sum)
  ag2 <- aggregate(usage_value ~ account_id+usage_resource+usage_type, data = t2, FUN=sum)
  
  
  colnames(ag1)[4] <- "usage_yesterday"
  colnames(ag2)[4] <- "usage_today"
  
  chrg <- merge(ag1, ag2, by=c("usage_resource", "account_id", "usage_type"), all=TRUE)
  
  chrg[is.na(chrg)] <- 0
  
  chrg$diff <- chrg$usage_today - chrg$usage_yesterday
  chrg <- chrg[order(-chrg$diff),]
  chrg$date <- dateStr
  
  p <- getPricePlan(2)
  p <- p[, c("Usage.Type", "Price.Unit")]
  colnames(p)[1] <- "usage_type"
  colnames(p)[2] <- "price"
  chrg <- merge(chrg, p, by=c("usage_type"), all.x = T)
  chrg$chargeDiff <- chrg$diff * chrg$price
  
  customerList <- getCustomerData()
  chrg <- merge(chrg, customerList, by=c("account_id"), all.x=T)
  chrg <- filter(chrg, !is.na(payer_account_id))
  
  return (chrg)
}

dailyUsageChargeDiff <- function (dateStr) {
  if (missing(dateStr)) {
    t<- Sys.Date()-1
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dateStr)
      return()
    }
  }
  
  
  d1 <- format(t-1, "%Y-%m-%d")
  d2 <- format(t, "%Y-%m-%d")
  dateStr <- d2
  
  t1 <- getDailyCharge(d1)
  t1 <- t1[t1$is_internal_flag=="N",]
  t2 <- getDailyCharge(d2)
  t2 <- t2[t2$is_internal_flag=="N",]
  
  ag1 <- aggregate(t1$billed_amount, by=list(t1$usage_type), FUN=sum)
  ag2 <- aggregate(t2$billed_amount, by=list(t2$usage_type), FUN=sum)
  
  
  colnames(ag1)[1] <- "usage_type"
  colnames(ag1)[2] <- "charge_yesterday"
  colnames(ag2)[1] <- "usage_type"
  colnames(ag2)[2] <- "charge_today"
  
  chrg <- merge(ag1, ag2, by="usage_type", all=TRUE)
  
  
  chrg[is.na(chrg)] <- 0
  
  chrg[,"diff"] <- chrg["charge_today"] - chrg["charge_yesterday"]
  chrg <- chrg[order(-chrg$diff),]
  chrg$date <- dateStr
  
  return (chrg)
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
    SQL <- paste("select *
                 from a9cs_metrics.es_weekly_charges
                 where year = '", year, "' AND week='", week,"'", sep="")
    conn <- dbConnect(driver, url)
    
    message("- Running query: weekly charge data...")
    t <- dbGetQuery(conn, SQL)
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    dbDisconnect(conn)
    write.csv(t, fpath)
  }
  #t <- subset(t, !(account_id %in% testAccount$account_id))
  
  assign(vname, t, envir = .GlobalEnv)
  message("Charge data for week ", week, " retrieved.")
  
  return(t)
}

getAccountCharge <- function (dateStr) {
  if (missing(dateStr)) {
    t<- Sys.Date()-1
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 500) {
      message("Wrong date string: ", dateStr)
      return()
    }
  }
  
  cust <- customerList[c("account_id", "company")]
  
  dateStr <- format(t, "%Y-%m-%d")
  
  dt <- getDailyCharge(dateStr)
  dt <- dt[dt$is_internal_flag=="N",]
  
  ag <- aggregate(dt$billed_amount, by=list(dt$account_id), FUN=sum)
  
  colnames(ag)[1] <- "account_id"
  colnames(ag)[2] <- "billed_amount"
  
  ag$account_id <- str_pad(as.character(ag$account_id), 12, pad="0")
  
  ag <- merge(ag, cust, by="account_id", all.x=TRUE)
  
  ag$bill_date <- dateStr
  
  return (ag)
}

getDailyCharge <- function(dateStr) {
  #set the date you want to analysze. If not specified, by default it's yesterday. 
  if (missing(dateStr)) {
    dateStr <- format(Sys.Date()-1, "%Y-%m-%d")
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 500) {
      message("Wrong date string: ", dateStr)
      return()
    }
    dateStr <- format(t, "%Y-%m-%d")
  }
  message("- Getting charge data for ", dateStr)
  
  outputDir <- "/Users/maghuang/aws/daily"
  if (!dir.exists(outputDir)) {
    dir.create(outputDir)
  }
  
  fname <- paste("dailycharge_", dateStr, ".csv", sep="")
  fpath <- file.path(outputDir, fname);
  
  if (file.exists(fpath)) {
    t <- read.csv(fpath, sep=",")
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  } else {
    conn <- dbConnect(driver, url)
    message("- Connected to DB.")
    
    sqldaily <- paste("select computation_date,charge_item_desc,account_id,payer_id as payer_account_id,
                      usage_type, usage_value, offering_id, billed_amount, price_per_unit, region, is_internal_flag 
                      from a9cs_metrics.es_daily_charges
                      where computation_date = '",dateStr,"'", sep="")


    message("- Running query: daily revenue data...")
    t <- dbGetQuery(conn, sqldaily)
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    message("- Query succeed: daily revenue for ", dateStr, " retrieved.")
    dbDisconnect(conn)
    write.csv(t, fpath, sep=",")
    message("Charge data for ", dateStr," saved to file: ", fpath)
  }

  return(t)
}


getDailyMetering <- function(dateStr) {
  #set the date you want to analysze. If not specified, by default it's yesterday. 
  if (missing(dateStr)) {
    dateStr <- format(Sys.Date()-1, "%Y-%m-%d")
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 500) {
      message("Wrong date string: ", dateStr)
      return()
    }
    dateStr <- format(t, "%Y-%m-%d")
  }
  message("- Getting daily metering data for ", dateStr)
  
  outputDir <- "/Users/maghuang/aws/daily"
  if (!dir.exists(outputDir)) {
    dir.create(outputDir)
  }
  
  fname <- paste("dailymeter_", dateStr, ".csv", sep="")
  fpath <- file.path(outputDir, fname);
  
  if (file.exists(fpath)) {
    t <- read.csv(fpath, sep=",")
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  } else {
    conn <- dbConnect(driver, url)
    message("- Connected to DB.")
    
    sqldaily <- paste("select * from a9cs_metrics.es_daily_metering where request_day = '", dateStr, "'", sep="")
    
    
    message("- Running query: daily revenue data...")
    t <- dbGetQuery(conn, sqldaily)
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    message("- Query succeed: daily revenue for ", dateStr, " retrieved.")
    dbDisconnect(conn)
    write.csv(t, fpath, sep=",")
    message("Charge data for ", dateStr," saved to file: ", fpath)
  }
  
  return(t)
}


getRegion <- function(dateStr) {
  #set the date you want to analysze. If not specified, by default it's yesterday. 
  if (dateStr == "") {
    dateStr <- format(Sys.Date()-1, "%Y-%m-%d")
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dateStr)
      return()
    }
    dateStr <- format(t, "%Y-%m-%d")
  }
    message("- Getting region data for ", dateStr)
  
    message("- Connecting to DB...")
    conn <- dbConnect(driver, url)
    
    message("- Connected to DB.")
    
    sqldaily <- paste("WITH ut 
     AS (SELECT DISTINCT usage_type, 
                      region 
                      FROM   awsdw_dm_billing.dim_aws_usage_types) 
                      SELECT ut.usage_type, 
                      ut.region 
                      FROM   ut 
                      WHERE  EXISTS (SELECT 1 
                      FROM   awsdw_dm_metering.d_daily_metering_sum dms 
                      WHERE  dms.usage_type = ut.usage_type 
                      AND dms.client_product_code = 'AmazonES' 
                      AND dms.request_day >= '{YYYY}-{MM}-{DD}' - 60)
                      ")
    message("- Running query: region data...")
    t <- dbGetQuery(conn, sqldaily)
    message("- Query succeed: region for ", dateStr, " retrieved.")
    dbDisconnect(conn)
    write.csv(t, fpath, sep=",")
    message("Charge data for ", dateStr," saved to file: ", fpath)
  
  return(t)
}





initializeES <- function () {
  
  esHost <- "http://127.0.0.1"
  esPort <- 9200
  
  
  esUser <- NULL
  esPwd <- NULL
  
  connect(es_host = esHost, es_port = esPort, es_user=esUser, es_pwd = esPwd)
  
}

loadDaily2ES <- function(dateStr, nDay=1) {
  if (missing(dateStr)) {
    t <- Sys.Date()-1
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dateStr)
      return ()
    }
  }
  
  initializeES()
  
  r <- c(0:(nDay-1))
  for (i in r) {
    dateStr <- format(t-i, "%Y-%m-%d")
    indexName <- paste("daily-charge-", dateStr,sep="")
    
    if (index_exists(indexName)) {
      message("Index ", indexName, " already exists. Skip loading.")
    } else {
      message("Loading index ", indexName, " into Elasticsearch...")
      dt <- getDailyCharge(dateStr)
      # create index with mapping
      body <- paste('{
        "settings" : {
        "number_of_shards" : 1
        },
        "mappings": {
          "', indexName, '": {
            "properties": {
              "client_product_code" : {"type" : "string", "index":  "not_analyzed"},
              "charge_item_desc" : {"type" : "string", "index":  "not_analyzed"},
              "account_id" : {"type" : "string", "index":  "not_analyzed"},
              "payer_id" : {"type" : "string", "index":  "not_analyzed"},
              "product_code" : {"type" : "string", "index":  "not_analyzed"},
              "usage_type" : {"type" : "string", "index":  "not_analyzed"},
              "usage_value" : {"type" : "float", "index":  "not_analyzed"},
              "pricing_plan_id" : {"type" : "string", "index":  "not_analyzed"},
              "offering_id" : {"type" : "string", "index":  "not_analyzed"},
              "rate_id" : {"type" : "string", "index":  "not_analyzed"},
              "credit_id" : {"type" : "string", "index":  "not_analyzed"},
              "operation" : {"type" : "string", "index":  "not_analyzed"},
              "region" : {"type" : "string", "index":  "not_analyzed"},
              "is_internal_flag" : {"type" : "string", "index":  "not_analyzed"}
              }
            }
          }
        }', sep="")
      index_create(indexName, body=body)
      
      docs_bulk(dt, indexName)
      message("Index ", indexName, " loading complete!")
    }
    
    # indexName <- paste("account-charge-", dateStr,sep="")
    # 
    # if (index_exists(indexName)) {
    #   message("Index ", indexName, " already exists. Skip loading.")
    # } else {
    #   message("Loading index ", indexName, " into Elasticsearch...")
    #   dt <- getAccountCharge(dateStr)
    #   # create index with mapping
    #   body <- paste('{
    #     "settings" : {
    #       "number_of_shards" : 1
    #     },
    #     "mappings": {
    #       "', indexName, '": {
    #         "properties": {
    #           "account_id" : {"type" : "string", "index":  "not_analyzed"},
    #           "company" : {"type" : "string", "index":  "not_analyzed"}
    #         }
    #       }
    #     }
    #   }', sep="")
    #   index_create(indexName, body=body)      
    #   docs_bulk(dt, indexName)
    #   message("Index ", indexName, " loading complete!")
    # }
    # 
    # indexName <- paste("gainer-", dateStr,sep="")
    # 
    # if (index_exists(indexName)) {
    #   message("Index ", indexName, " already exists. Skip loading.")
    # } else {
    #   message("Loading index ", indexName, " into Elasticsearch...")
    #   dt <- dailyGainer(dateStr)
    #   # create index with mapping
    #   body <- paste('{
    #                 "settings" : {
    #                 "number_of_shards" : 1
    #                 },
    #                 "mappings": {
    #                 "', indexName, '": {
    #                 "properties": {
    #                 "account_id" : {"type" : "string", "index":  "not_analyzed"},
    #                 "company" : {"type" : "string", "index":  "not_analyzed"}
    #                 }
    #                 }
    #                 }
    #             }', sep="")
    #   index_create(indexName, body=body)      
    #   docs_bulk(dt, indexName)
    #   message("Index ", indexName, " loading complete!")
    # }
    # 
    # indexName <- paste("usage-diff-", dateStr,sep="")
    # 
    # if (index_exists(indexName)) {
    #   message("Index ", indexName, " already exists. Skip loading.")
    # } else {
    #   message("Loading index ", indexName, " into Elasticsearch...")
    #   dt <- dailyUsageDiff(dateStr)
    #   # create index with mapping
    #   body <- paste('{
    #                 "settings" : {
    #                 "number_of_shards" : 1
    #                 },
    #                 "mappings": {
    #                 "', indexName, '": {
    #                 "properties": {
    #                 "usage_type" : {"type" : "string", "index":  "not_analyzed"}
    #                 }
    #                 }
    #                 }
    # }', sep="")
    #   index_create(indexName, body=body)      
    #   docs_bulk(dt, indexName)
    #   message("Index ", indexName, " loading complete!")
    # }
    # 
    # indexName <- paste("usage-charge-diff-", dateStr,sep="")
    # 
    # if (index_exists(indexName)) {
    #   message("Index ", indexName, " already exists. Skip loading.")
    # } else {
    #   message("Loading index ", indexName, " into Elasticsearch...")
    #   dt <- dailyUsageChargeDiff(dateStr)
    #   # create index with mapping
    #   body <- paste('{
    #                 "settings" : {
    #                 "number_of_shards" : 1
    #                 },
    #                 "mappings": {
    #                 "', indexName, '": {
    #                 "properties": {
    #                 "usage_type" : {"type" : "string", "index":  "not_analyzed"}
    #                 }
    #                 }
    #                 }
    # }', sep="")
    #   index_create(indexName, body=body)      
    #   docs_bulk(dt, indexName)
    #   message("Index ", indexName, " loading complete!")
    # }
    
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
    
    SQL <- "select account_id, 
    payer_account_id, 
    internal, 
    company_name as account_name,
    (select company_name from account_dm 
    where account_dm.account_id = k.payer_account_id ) as payer_name
    from
    (select distinct k.account_id,a.payer_account_id,a.company_name,a.internal
    from a9cs_metrics.es_weekly_metering k join account_dm a 
    on a.account_id = k.account_id) k"
    
    
    SQL <- "select account_id, 
    payer_account_id, 
    internal, 
    company_name as account_name,
    (select company_name from account_dm 
    where account_dm.account_id = k.payer_account_id ) as payer_name
    from
    (select distinct k.account_id,a.payer_account_id,a.company_name,a.internal
    from a9cs_metrics.es_weekly_metering k join account_dm a 
    on a.account_id = k.account_id) k"
    
    message("- Running query: customer data...")
    t <- dbGetQuery(conn, SQL)
    message("- Query succeed: customer data retrieved.")
    
    t$account_id <- str_pad(t$account_id, 12, pad="0")
    t$payer_account_id <- str_pad(t$payer_account_id, 12, pad="0")
    t$is_internal_flag <- ifelse(t$internal == "external", "N", "Y")
    t$internal <- NULL
    
    dt_child <- t[, c("account_id", "payer_account_id", "account_name")]
    dt_child$payer_account_id <- ifelse(dt_child$account_id == dt_child$payer_account_id, NA, dt_child$payer_account_id)
    colnames(dt_child)[which(names(dt_child) == "account_name")] <- "company"
    
    dt_payer <- t[, c("payer_account_id", "payer_name")]
    
    colnames(dt_payer)[which(names(dt_payer) == "payer_account_id")] <- "account_id"
    dt_payer$payer_account_id <- NA
    colnames(dt_payer)[which(names(dt_payer) == "payer_name")] <- "company"
    
    t <- unique(rbind(dt_child, dt_payer))
    
    
    
    
    w <- weeknum(weekDiff = -1)
    SQL <- paste("select distinct account_id, is_internal_flag, offering_id
                 from a9cs_metrics.es_weekly_charges
                 where year = ", w$year, " and week = ", w$week ,
                 "and offering_id in ('607996','615324','729920', '615325', '607997', '631467')", sep="")
    x <- dbGetQuery(conn, SQL)
    
    t <- merge(t, x, all.x = T)
    
    dbDisconnect(conn)
    
    write.csv(t, fpath, row.names = F)
    message("Customer data saved to file: ", fpath)
  }
  
  return(t)
}

getWeeklyUsage <- function (week) {
  
  outputDir <- "/Users/maghuang/aws/weekly"
  if (!dir.exists(outputDir)) {
    dir.create(outputDir)
  }
  if (missing(week)) {
    week <- weeknum() -1
  } 
  week <- as.character(week)
  
  fname <- paste("WeeklyUsage-wk", week,".csv", sep="")
  fpath <- file.path(outputDir, fname);
  
  if(file.exists(fpath)) {
    t <- read.csv(fpath, sep=",")
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  } else {
    conn <- dbConnect(driver, url)
    
    message("- Connected to DB.")
    
    SQL <- paste("select account_id, sum(usage_value) as sum_usage_value, is_internal_flag, usage_resource, region
      from a9cs_metrics.es_weekly_metering
      where year=2016 and week = ", weeknum()-1, "
      group by account_id, week, year, usage_resource, is_internal_flag, region;", sep="")
    
    message("- Running query: weekly usage data...")
    t <- dbGetQuery(conn, SQL)
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    message("- Query succeed: weekly usage data retrieved.")
    dbDisconnect(conn)
    
    write.csv(t, fpath, sep=",")
    message("Weekly usage data saved to file: ", fpath)
  }
  
  return(t)
}


# get the top customer by revenue $ in the past 7 days by region
topCustomerByRegion <- function (week) {
  
  message("Generating top customer list by region...")
  
  if (missing(week)) {
    week <- weeknum() - 1
  }
  week <- as.character(week)
  
  outputDir <- "/Users/maghuang/aws"
  serviceFile <- file.path(outputDir, "AES_service.csv")
  svcDt <- read.csv(serviceFile)
  svcDt$account_id <- str_pad(as.character(svcDt$account_id), 12, pad="0")
  svcDt <- svcDt[svcDt$product_name != "NULL",]
  
  outputDir <- paste("/Users/maghuang/aws/weekly/topRegionCustomer-week", week, sep="")
  if (!dir.exists(outputDir)) {
    dir.create(outputDir)
  }
  
  chrg <- getWeeklyCharges(week)
  regionList <- na.omit(unique(chrg$region))
  
  chrg_i <- chrg[chrg$is_internal_flag=="Y", ]
  chrg_e <- chrg[chrg$is_internal_flag=="N", ]
  rm(chrg)
  
  
  usage <- getWeeklyUsage(week)
  
  usage_i <- usage[usage$is_internal_flag=="Y", c("account_id","usage_resource", "region")]
  usage_i$account_id <- str_pad(as.character(usage_i$account_id), 12, pad="0")
  
  usage_e <- usage[usage$is_internal_flag=="N", c("account_id","usage_resource", "region")]
  usage_e$account_id <- str_pad(as.character(usage_e$account_id), 12, pad="0")
  
  rm(usage)
  
  
  cust <- customerList[, c("account_id", "company", "email")]
  cust$account_id <- str_pad(as.character(cust$account_id), 12, pad="0")
  
  ag_i <- aggregate(chrg_i$billed_amount, by=list(chrg_i$account_id, chrg_i$region), FUN=sum)
  rm(chrg_i)
  colnames(ag_i)[1] <- "account_id"
  colnames(ag_i)[2] <- "region"
  colnames(ag_i)[3] <- "billed_amount"
  ag_i$account_id <- str_pad(as.character(ag_i$account_id), 12, pad="0")
  ag_i <- merge(ag_i, cust, by="account_id", all.x=TRUE)
  
  
  ag_e <- aggregate(chrg_e$billed_amount, by=list(chrg_e$account_id, chrg_e$region), FUN=sum)
  rm(chrg_e)
  colnames(ag_e)[1] <- "account_id"
  colnames(ag_e)[2] <- "region"
  colnames(ag_e)[3] <- "billed_amount"
  ag_e$account_id <- str_pad(as.character(ag_e$account_id), 12, pad="0")
  ag_e <- merge(ag_e, cust, by="account_id", all.x=TRUE)
  
  # Find the service contract info for each customer
  ag_e <- merge(ag_e, svcDt, by="account_id", all.x=TRUE)
  ag_e$pricing_plan_name <- NULL
  ag_e$begin_date <- NULL
  names(ag_e)[6] <- "service_contract"
  
  for (i in regionList) {
    message("Generating top customer list for region ", i)
    chrg_i <- ag_i[ag_i$region == i, ]
    regionUsage  <- usage_i[usage_i$region == i, c("account_id", "usage_resource")]
    t <- merge(chrg_i, regionUsage, by="account_id", all.x=TRUE)
    t <- t[order(-t$billed_amount), ]
    t$billed_amount <- NULL
    
    fname <- paste("TopCustomer-internal-wk", week,"-",i,".csv", sep="")
    fpath <- file.path(outputDir, fname);
    write.csv(t, fpath)
    
    chrg_e <- ag_e[ag_e$region == i, ]
    regionUsage  <- usage_e[usage_e$region == i, c("account_id", "usage_resource")]
    t <- merge(chrg_e, regionUsage, by="account_id", all.x=TRUE)
    t <- t[order(-t$billed_amount), ]
    t$billed_amount = NULL
    
    fname <- paste("TopCustomer-external-wk", week,"-",i,".csv", sep="")
    fpath <- file.path(outputDir, fname);
    write.csv(t, fpath)
  }
  zipname <- paste(outputDir, ".zip", sep="")
  zip(zipname, outputDir)
  
  message("Top customer lists saved to ", zipname)
}

getCustomerCharge <- function(account_id) {
  
  payer <- customerList[customerList$account_id == account_id, ]$payer_account_id
  if (is.na(payer)) {
    payer <- account_id
  }
  
  all_accounts <- filter(customerList, payer_account_id == payer)$account_id
  all_accounts <- c(account_id, all_accounts)
  
  
  queryStr <- paste("'", all_accounts[1], "'", sep = "")
  for (i in all_accounts) {
    queryStr<- paste(queryStr, ", '", i, "'", sep = "")
  }
  
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  SQL <- paste("select * from a9cs_metrics.es_daily_charges
            where computation_date >= '2016-10-01' 
              and account_id in (", queryStr,")", sep = "")
  
  message("- Running query: weekly usage data...")
  t <- dbGetQuery(conn, SQL)
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  message("- Query succeed: weekly usage data retrieved.")
  dbDisconnect(conn)
  
 return(t)
}

getCustomerUsage <- function(account_id, type) {
  
  if(missing(type)) {
    type = "ES"
  } else {
    type = "CS"
  }
  payer <- customerList[customerList$account_id == account_id, ]$payer_account_id
  if (is.na(payer)) {
    payer <- account_id
  }
  
  
  all_accounts <- filter(customerList, payer_account_id == payer)$account_id
  
  
  
  queryStr <- paste("'", all_accounts[1], "'", sep = "")
  for (i in all_accounts) {
    queryStr<- paste(queryStr, ", '", i, "'", sep = "")
  }
  
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  #Elasticsearch usage
  
  if(type == "ES") {
    SQL <- paste("select request_day, 
                 account_id, 
                 usage_type, 
                 usage_value, 
                 usage_resource,
                 operation
                 from a9cs_metrics.es_daily_metering
                 where request_day >= '2016-01-01' 
                 and account_id in (", queryStr,")", sep = "")
  } else {
    #Cloudsearch usage
    SQL <- paste("select computation_date, 
                 client_product_code, 
                 account_id, 
                 usage_type, 
                 usage_value, 
                 billed_amount, 
                 operation, 
                 price_per_unit,
                 region from a9cs_metrics.daily_charges
                 where computation_date >= '2016-01-01' 
                 and account_id in (", queryStr,")", sep = "")
    
    SQL <- paste("select request_day, 
                 client_product_code, 
                 account_id, 
                 usage_type, 
                 usage_value,
                 usage_resource,
                 operation
                 from a9cs_metrics.daily_metering
                 where request_day >= '2016-01-01' 
                 and account_id = '", account_id,"'", sep = "")
  }
  
  
  
  message("- Running query: weekly usage data...")
  t <- dbGetQuery(conn, SQL)
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  message("- Query succeed: weekly usage data retrieved.")
  dbDisconnect(conn)
  
  return(t)
}

getPricePlan <- function(id) {
  planPath <- "/Users/maghuang/aws/price"
  planFile <- c("F4468EE8-free.csv", 
                "FF9EADEE-nonfree.csv",
                "8BD4758E-AISPLnonfree.csv",
                "7A1CF623-AISPLfree.csv",
                "AC90A348-custom.csv")
  
  planFile <- file.path(planPath, planFile[id]);
  
  t <- read.csv(planFile)
  
}

calculateCharge <- function() {
  chrg <- getMonthlyCustomerCharge()
  p <- getPricePlan(5)[, c("Price.Unit","Usage.Type")]
  colnames(p)[1] <- "price_unit_custom"
  colnames(p)[2] <- "usage_type"
  
  s <- merge(chrg, p, by="usage_type", all.x = TRUE)
  
  s$custom_billed_amount <- s$price_unit_custom * s$usage_value
  s[,"diff"] <- s$billed_amount - s$custom_billed_amount
  
  return (s)
}


checkDailySum <- function(dateStr) {
  #set the date you want to analysze. If not specified, by default it's yesterday. 
  if (missing(dateStr)) {
    dateStr <- format(Sys.Date()-1, "%Y-%m-%d")
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 500) {
      message("Wrong date string: ", dateStr)
      return()
    }
    dateStr <- format(t, "%Y-%m-%d")
  }
  message("- Getting charge data for ", dateStr)
  conn <- dbConnect(driver, url)
  message("- Connected to DB.")
  
  sqldaily <- paste("select fct.computation_date  AS cal_date,
                    'ElasticSearch' AS Service_name,
                    Sum(fct.billed_amount * fct.amortization_factor) ,
                    count(distinct acct.account_id) as developer_count
                    FROM    awsdw_dm_billing.fact_aws_daily_est_revenue fct
                    inner join  awsdw_dm_billing.dim_aws_accounts acct using (account_seq_id) 
                    inner join  awsdw_dm_billing.dim_aws_accounts pacct
                    on fct.payer_account_seq_id = pacct.account_seq_id
                    inner join  awsdw_dm_billing.dim_aws_offering_pricing_plans opp using(offering_pricing_plan_seq_id)
                    inner join  awsdw_dm_billing.dim_aws_usage_types ut using (usage_type_seq_id)
                    inner join awsdw_dm_billing.dim_aws_operations op using(operation_seq_id)
                    inner join awsdw_dm_billing.dim_aws_products product using(product_seq_id)
                    inner join a9cs_metrics.es_region_mapping rm
                    on ut.usage_type=rm.usage_type
                    WHERE   fct.computation_date >= '2016-09-01'
                    AND acct.is_internal_flag = 'N' AND product.product_code = 'AmazonES'
                    and  (coalesce(acct.fraud_enforcement_status, 'OK') in ('OK', 'Reinstated'))
                    GROUP  BY 1,2", sep="")
  message("- Running query: daily revenue data...")
  t <- dbGetQuery(conn, sqldaily)
  message("- Query succeed: daily revenue for ", dateStr, " retrieved.")
  dbDisconnect(conn)
  message("Charge data for ", dateStr," saved to file: ", fpath)
  
  return(t)
}

getSQLData <- function() {
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  SQL <- paste("select distinct account_id, is_internal_flag, offering_id
                 from a9cs_metrics.es_weekly_charges
               where year = 2017 and week = 5
               and offering_id in ('607996','615324','729920', '615325', '607997', '631467')", sep="")
  
  message("- Running query: weekly usage data...")
  t <- dbGetQuery(conn, SQL)
  # t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  message("- Query succeed: weekly usage data retrieved.")
  dbDisconnect(conn)
  
  return(t)
  
}

getDomainDiff <- function(week) {
  
  if (missing(week)) {
    week = weeknum() -1
  }
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  SQL <- paste("select DISTINCT year, week, account_id, usage_resource, region
  from a9cs_metrics.es_weekly_metering
  where week in ('", week,"', '", week - 1, "') and year = '2016' and is_internal_flag = 'N'", sep="")
  
  message("SQL = ", SQL)
  
  message("- Running query: weekly domain count data...")
  t <- dbGetQuery(conn, SQL)
  
  message("- Query succeed: weekly domain count data retrieved.")
  dbDisconnect(conn)
  
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  
  w1 <- t[t$week == week-1, ]
  w2 <- t[t$week == week, ]
  
  w1 <- aggregate(w1$usage_resource, by=list(w1$account_id, w1$region), FUN=length)
  w2 <- aggregate(w2$usage_resource, by=list(w2$account_id, w2$region), FUN=length)
  
  names(w1)[1] <- "account_id"
  names(w1)[2] <- "region"
  names(w1)[3] <- "d1"
  names(w2)[1] <- "account_id"
  names(w2)[2] <- "region"
  names(w2)[3] <- "d2"
  
  w <- merge(w1, w2, by=c("account_id", "region"), all=TRUE)
  
  w[is.na(w)] <- 0
  w$diff <- w$d2 - w$d1
  
  
  w <- merge(w, customerList[,c("account_id", "company")], by="account_id", all.x=TRUE)
  
  w <- aggregate(cbind(diff, d1, d2) ~ company + region, data=w, sum)
  
  return (w)
}

getCustomerDiff <- function(week) {
  
  customerList <- within(customerList, payer_account_id <- ifelse(is.na(payer_account_id), account_id, payer_account_id))
  
  if (missing(week)) {
    week = weeknum() -1
  }
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  SQL <- paste("select * from a9cs_metrics.es_new_inactive_customers where year = '2016'
               and week in ('", week, "')
               and is_internal_flag = 'N'", sep="")
  
  message("SQL = ", SQL)
  
  message("- Running query: weekly inactive customer count data...")
  inactive_cust <- dbGetQuery(conn, SQL)
  inactive_cust$account_id <- str_pad(as.character(inactive_cust$account_id), 12, pad="0")
  
  message("- Query succeed: weekly inactive customer count data retrieved.")
  
  
  SQL <- paste("select * from a9cs_metrics.es_new_active_customers where year = '2016'
               and week in ('", week, "')
               and is_internal_flag = 'N'", sep="")
  
  message("SQL = ", SQL)
  
  message("- Running query: weekly active customer count data...")
  active_cust <- dbGetQuery(conn, SQL)
  active_cust$account_id <- str_pad(as.character(active_cust$account_id), 12, pad="0")
  
  message("- Query succeed: weekly inactive customer count data retrieved.")
  
  dbDisconnect(conn)
  
  
  # get payer_account_id for account_id
  inactive_cust <- merge(inactive_cust, customerList, by=c("account_id"), all.x=TRUE)
  active_cust <- merge(active_cust, customerList, by=c("account_id"), all.x=TRUE)
  
  ### process the new inactive customer data
  
  inactive_cust <- aggregate(inactive_cust$account_id, by=list(inactive_cust$payer_account_id), FUN=length)
  
  names(inactive_cust)[1] <- "account_id"
  names(inactive_cust)[2] <- "inactive_cnt"
  
  
  ### process the new active customer data
 
  
  active_cust <- aggregate(active_cust$account_id, by=list(active_cust$payer_account_id), FUN=length)
  
  names(active_cust)[1] <- "account_id"
  names(active_cust)[2] <- "active_cnt"
  
 
  w <- merge(active_cust, inactive_cust, by=c("account_id"), all=TRUE)
  
  w[is.na(w)] <- 0
  w$diff <- w$active_cnt - w$inactive_cnt
  
  w <- merge(w, customerList[,c("account_id", "company")], by="account_id", all.x=TRUE)
  w <- aggregate(diff ~ company, data=w, sum)
  return (w)
}

calculateFreetierCost <- function(nDay) {
  
  res <- data.frame(charge_date = character(nDay), 
                    revenue = numeric(nDay), 
                    current_cost= numeric(nDay), 
                    new_cost = numeric(nDay),
                    ratio = numeric(nDay),
                    stringsAsFactors = FALSE)
  
  for (i in c(nDay:1)) {
    idx <- nDay - i + 1
    dateStr <- format(Sys.Date()-i, "%Y-%m-%d")
    t <- getDailyCharge(dateStr)
    t <- t[t$is_internal_flag == "N", ]
    
    res$charge_date[idx] <- dateStr
    res$revenue[idx] <- sum(t$billed_amount, na.rm=TRUE)
    
    t <- t[grep("t2.micro", t$usage_type),]
    t <- t[t$offering_id=="607996",]
    t <- t[t$price_per_unit==0,]
    
    t <- t[,c("account_id", "usage_type", "usage_value", "price_per_unit")]
    
    t$new_usage_type <- gsub("t2.micro", "t2.small", t$usage_type, fixed = TRUE)
    
    price <- getPricePlan(2)
    price <- price[,c("Usage.Type", "Price.Unit")]
    names(price)[1] <- "usage_type"
    names(price)[2] <- "price_per_unit"
    price$cost <- price$price_per_unit / 1.4
    
    t <- merge(t, price, by="usage_type", all.x = TRUE)
    
    t <- merge(t, price, by.x = "new_usage_type", by.y = "usage_type", all.x = TRUE)
    
    t$current_cost <- t$usage_value * t$cost.x
    t$new_cost <- t$usage_value * t$cost.y
    
    res$current_cost[idx] <- sum(t$current_cost)
    res$new_cost[idx] <- sum(t$new_cost)
    
  }
  res$ratio <- res$current_cost / res$revenue
  return (res)
  
}


getDomainUsage <- function(week) {
  
  if (missing(week)) {
    week = weeknum() -1
  }
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  SQL <- paste("select distinct usage_resource, usage_type, sum(usage_value) 
              from a9cs_metrics.es_weekly_metering
              where year = '2016' and week = '37'
              and usage_type like '%ESInstance%'
              group by 1,2
              order by 3 desc", sep="")
  
  message("- Running query: domain usagge data...")
  t <- dbGetQuery(conn, SQL)
  
  message("- Query succeed: domain usage data retrieved.")
  dbDisconnect(conn)
  
  tmp <- str_split_fixed(t$usage_type, ":", 2)
  
  t$instance_type <- tmp[,2]
  return (t)
# 
#   w <-aggregate(t$usage_resource, by=list(t$instance_type), FUN=length)
# 
#   names(w)[1] <- "instance_type"
#   names(w)[2] <- "domain_count"
#   s <- sum(w$domain_count)
#   w$percentage <- w$domain_count / s
# 
#   return (w)
  
}

clearDailyData <- function (dateStr) {
  index_name <- paste("*", dateStr, sep = "")
  index_delete(index_name)
  
  outputDir <- "/Users/maghuang/aws/daily"
  fname <- paste("dailycharge_", dateStr, ".csv", sep="")
  fpath <- file.path(outputDir, fname);
  file.remove(fpath)
}

customerCountTrend <- function () {
  
}


workloadType <- function() {
  # get metering data of a specific day
  
  x <- merge(x, customerList[, c("account_id", "is_internal_flag")], by = c("account_id"), all.x=T)
  
  # find out all the domain names
  
  
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

bitcoinDomains <- function() {
  fdir <- "/Users/maghuang/Downloads/bitcoin/open-domains/"
  x <- list.files(fdir)
  
  filename <- x[grep("*csv", x)]
  region <- gsub(".csv", "", filename)
  flist <- data.frame(filename = filename, region = region)
  
  allDomain <- data.frame()
  for (i in 1:nrow(flist)) {
    x <- flist[i,]
    fpath <- file.path(fdir, x$filename)
    dt <- read.csv(fpath)
    colnames(dt)[1] <- "domain"
    dt$region <- x$region
    dt$X <- NULL
    
    dt <- transform(dt, tmp=colsplit(dt$domain, ":", names=c("1","2")))
    colnames(dt)[which(names(dt) == "tmp.1")] <- "account_id"
    colnames(dt)[which(names(dt) == "tmp.2")] <- "domain_name"
    allDomain <- rbind(allDomain, dt)
  }
  allDomain$account_id <- str_pad(allDomain$account_id, 12, pad="0")
  
  domainStats <- data.frame(ransomed = nrow(allDomain))
  
  message("== Total ", domainStats$ransomed, " domains are compromised with a ransom message.")
  # suspendedDomains <- read.csv("/Users/maghuang/Downloads/bitcoin/es_invalid_domains.csv")
  # allDomain <- filter(allDomain, !account_id %in% suspendedDomains$account_id)
  
  domainStatus <- getCustomerStatus(allDomain)
  allDomain <- merge(allDomain, domainStatus[,c("account_id", "account_status_code")], by = c("account_id"), all.x = T)
  
  domainStats$suspended <- domainStats$ransomed - nrow(allDomain)
  message("== Of the ransomed domains, ", domainStats$suspended, " domains are suspended.")
  
  # if(!exists("customerList")) {
  #   customerList <<- getCustomerData()
  # }
  
  # allDomain <- merge(allDomain, customerList[, c("account_id", "payer_account_id", "is_internal_flag")], by = c("account_id"), all.x = T)
  # allDomain <- filter(allDomain, is_internal_flag == "N")
  
  # domainStats$external <- domainStats$ransomed - domainStats$ransomed - nrow(allDomain)
  message("== Of the active (non-suspended) domains, ", domainStats$external, "domains are active and external. ")
  
  t1 <- getWeeklyCharges()
  
  tmp <- unique(t1[, c("account_id", "is_internal_flag")])
  allDomain <- merge(allDomain, tmp, by = c("account_id"), all.x = T)
  # t1 <- filter(t1, is_internal_flag == "N")
  # t1 <- merge(t1, customerList, by = c("account_id"), all.x = TRUE)
  # t1 <- within(t1, payer_account_id <- ifelse(!is.na(payer_account_id),payer_account_id,account_id))
  # t1 <- aggregate(billed_amount ~ payer_account_id, data = t1, sum)
  # t1 <- transform(t1, rank=rank(-billed_amount))
  
  # allDomain <- within(allDomain, payer_account_id <- ifelse(!is.na(payer_account_id),payer_account_id,account_id))
  # allDomain <- merge(allDomain, t1, by = c("payer_account_id"), all.x = T)
  
  # allDomain <- allDomain[order(-allDomain$billed_amount),]
  # colnames(allDomain)[which(names(allDomain) == "billed_amount")] <- "customer_weekly_spending"
  
  allDomain$arn <- paste0("arn:aws:es:", allDomain$region, ":", allDomain$account_id, ":domain/", allDomain$domain_name)
  allDomain$domain <- NULL
  allDomain$X <- NULL
  allDomain$name <- NULL
  allDomain$email <- NULL
  
  #allDomain$payer_account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  
  # pl <- getPricePlan(2)
  
  supportLevel <- read.csv("/Users/maghuang/Downloads/bitcoin/customer_support_level.csv")
  supportLevel$account_id <- str_pad(supportLevel$account_id, 12, pad = '0')
  
  allDomain <- merge(allDomain, supportLevel[, c("account_id", "product_name")], by=c("account_id"), all.x = T)
  
  impactedList <- read.csv("/Users/maghuang/Downloads/bitcoin/customer_without_support.csv", sep = "|")
  
  impactedList <- filter(impactedList, !grepl("arn", account_id))
  impactedList$impacted <- "Y"
  allDomain <- merge(allDomain, impactedList, by = c("account_id"), all.x=T)
  
  outputPath <- file.path()
  write.csv(allDomain, "/Users/maghuang/Downloads/bitcoin/domain_list.csv", row.names = F)
  
  nonSupportDomain <- filter(allDomain, is_internal_flag == "N")
  nonSupportDomain <- filter(nonSupportDomain, account_status_code == "Active")
  nonSupportDomain <- filter(nonSupportDomain, !(account_id %in% supportLevel$account_id))
  
  # nonSupportDomain <- filter(nonSupportDomain, !(payer_account_id %in% supportLevel$account_id))
  nonSupportDomain$arn <- trimws(nonSupportDomain$arn, which = c("both", "left", "right"))
  
  write.csv(nonSupportDomain, "/Users/maghuang/Downloads/bitcoin/AES_customer_without_support_full_list.csv", row.names = F)
  
  nonSupportDomain <- filter(nonSupportDomain, impacted != "Y")
  x <- aggregate(arn ~ account_id, data = nonSupportDomain, paste, collapse = "|")
  x$account_id <- str_pad(x$account_id, 12, pad = '0')
  write.table(x, "/Users/maghuang/Downloads/bitcoin/customer_nosupport_noimpact.csv",sep = "|", row.names = F, quote=FALSE)
  
  return (allDomain)
  
}

getDomainConfig <- function(dateStr) {
  if (missing(dateStr)) {
    t<- Sys.Date()-1
   
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dateStr)
      return()
    }
  }
  dateStr <- as.character(t)
  
  dateStr1 <- as.character(as.Date(dateStr) - 1)
  dateStr2 <- as.character(as.Date(dateStr) - 2)
  
  
  # meter0 <- getDailyMetering(dateStr)
  
  # meter2 <- getDailyMetering(dateStr2)
  meter1 <- getDailyMetering(dateStr)
  charge1 <- getDailyCharge(dateStr)
  charge1 <- unique(charge1[,c("account_id", "is_internal_flag")])
  
  meter1 <- filter(meter1, account_id %in% charge1$account_id)
  meter1 <- merge(meter1, charge1[, c("account_id", "is_internal_flag")], all.x = T)
  
  #meter1 <- filter(meter1, usage_resource %in% meter0$usage_resource)
  #meter1 <- filter(meter1, usage_resource %in% meter2$usage_resource)
  
  meter1_storage <- filter(meter1, grepl("Storage|PIOPS", usage_type))
  meter1_storage <- aggregate(usage_value ~ usage_resource, data = meter1_storage, sum)
  colnames(meter1_storage)[which(names(meter1_storage) == "usage_value")] <- "storage_size"
  
  meter1_instance <- filter(meter1, grepl("ESInstance", usage_type))
  
  price <- getPricePlan(2)
  colnames(price)[which(names(price) == "Usage.Type")] <- "usage_type"
  colnames(price)[which(names(price) == "Price.Unit")] <- "price"
  price <- price[, c("usage_type", "price")]
  meter1_instance <- merge(meter1_instance, price, by=c("usage_type"), all.x = TRUE)
  meter1_instance <- filter(meter1_instance, !is.na(price))
  
  nodecount <- aggregate(usage_value ~ usage_resource, data = meter1_instance, sum)
  colnames(nodecount)[which(names(nodecount) == "usage_value")] <- "instance_hour"
  
  meter1_instance$tmp <- paste0(meter1_instance$price, "-", meter1_instance$usage_type)
  nodetype <- aggregate(tmp ~ usage_resource+account_id+is_internal_flag, data = meter1_instance, max)
  
  domainConfig <- merge(nodetype, nodecount, by = c("usage_resource"), all = T)
  domainConfig <- merge(domainConfig, meter1_storage, by = c("usage_resource"), all=T)
  
  domainConfig <- transform(domainConfig, usage=colsplit(domainConfig$tmp, ":", names=c("1","2")))
  colnames(domainConfig)[which(names(domainConfig) == "usage.2")] <- "instance_type"
  domainConfig$usage.1 <- NULL
  domainConfig$storage_type <- ifelse(is.na(domainConfig$storage_size), "ephemeral", "EBS")
  
  ephemeral_mapping <- data.frame(instance_type = c("m3.medium","m3.large","m3.xlarge","m3.2xlarge",
                                                    "r3.large", "r3.xlarge", "r3.2xlarge","r3.4xlarge","r3.8xlarge", 
                                                    "i2.xlarge","i2.2xlarge", "i2.4xlarge", "i2.8xlarge"),
                                  ephemeral_storage_size = c(4, 32, 80, 160, 32, 80, 160, 320, 640, 800, 1600, 3200, 6400))
  domainConfig <- merge(domainConfig, ephemeral_mapping, by = c("instance_type"), all.x = T)
  
  domainConfig$node_count <- round(domainConfig$instance_hour/24)
  
  domainConfig$storage_size <- ifelse(domainConfig$storage_type == "EBS", domainConfig$storage_size, domainConfig$ephemeral_storage_size * domainConfig$node_count)
  
  domainConfig <- filter(domainConfig, !is.na(account_id))
  
  domainConfig$tmp <- NULL
  domainConfig$ephemeral_storage_size <- NULL
  
  write.csv(domainConfig, "/Users/maghuang/Downloads/domainConfig.csv", row.names = F)
  
  return (domainConfig)
}

test <- function() {
  t <- Sys.Date()-1
  for (i in c(0:9)) {
    dateStr <- as.character(t - 7*i)
    domainConfig <- getDomainConfig(dateStr)
    x <- filter(domainConfig, storage_type != "EBS")
    message("----instance storage size on ", dateStr, "is ", sum(x$storage_size, na.rm = T))
 
  }
  
}

getEC2Prices <- function() {
  t <- read.csv("/Users/maghuang/Downloads/AmazonEC2PublicPlan_Gamma.csv")
  
  ec2 <- filter(t, grepl("Linux", Description))
  ec2 <- filter(ec2, grepl("BoxUsage", Usage.Type))
  ec2 <- filter(ec2, !grepl("Dedicated", Description))
  ec2 <- transform(ec2, tmp=colsplit(ec2$Usage.Type, ":", names=c("1","2")))
  colnames(ec2)[which(names(ec2) == "tmp.1")] <- "region"
  colnames(ec2)[which(names(ec2) == "tmp.2")] <- "instance_type"
  ec2$region <- gsub("BoxUsage", "", ec2$region)
  ec2$region <- gsub("-", "", ec2$region)
  
  #ec2 <- ec2[, c("Price.Unit", "Usage.Unit", "region", "instance_type")]
  
  storage <- filter(t, grepl("VolumeUsage|VolumeP-IOPS", Usage.Type))
  storage <- filter(storage, !grepl("sc1|st1", Usage.Type))
  storage <- transform(storage, tmp=colsplit(storage$Usage.Type, ":", names=c("1","2")))
  colnames(storage)[which(names(storage) == "tmp.1")] <- "region"
  colnames(storage)[which(names(storage) == "tmp.2")] <- "instance_type"
  
  storage$region <- gsub("EBS", "", storage$region)
  storage$region <- gsub("-", "", storage$region)
  storage$instance_type <- gsub("VolumeUsage.gp2", "GP2-Storage", storage$instance_type)
  storage$instance_type <- gsub("VolumeUsage.piops", "PIOPS-Storage", storage$instance_type)
  storage$instance_type <- gsub("VolumeUsage", "Magnetic-Storage", storage$instance_type)
  storage$instance_type <- gsub("VolumeP-IOPS.piops", "PIOPS", storage$instance_type)
  
  
  #storage <- storage[, c("Price.Unit", "Usage.Unit", "region", "instance_type")]
  
  x <- rbind(ec2, storage)
  return (x)
  
}



generateESPriceList <- function() {
  regions <- read.csv("/Users/maghuang/Documents/Pricing/new_instance_type/regions.csv")
  pm <- read.csv("/Users/maghuang/Documents/Pricing/new_instance_type/new_es_price_matrix.csv")
  pm <- melt(pm, id=c("Price.prefix"))
  
  pm <- filter(pm, value > 0)
  
  pm$variable <- gsub("NA.", "", pm$variable)
  pm$Usage.Type <- paste0(pm$variable, ifelse(pm$variable=="", "", "-"), pm$Price.prefix)
  
  
  instanceList <- filter(pm, grepl("ESInstance", Usage.Type))
  
  instanceList <- merge(instanceList, regions, by.x = c("variable"), by.y = c("prefix"), all.x = T)
  
  currentProduct <- read.csv("/Users/maghuang/Documents/Pricing/new_instance_type/current_aes_products.csv")
  
  instanceList <- filter(instanceList, !Usage.Type %in% currentProduct$usageType)
}

calcCredits <- function(week, year) {
  if (missing(week)) {
    week = weeknum(weekDiff = -1)$week
  }
  
  if (missing(year)) {
    year = weeknum(weekDiff = -1)$year
  }
  
  wdates <- weekDates(year = year, week = week)
  
  SQL <- paste0("SELECT acct.account_id,
               charge_item_desc as description,
                SUM(fct.billed_amount*amortization_factor) as credits,
                SUM(fct.refund_amount*amortization_factor) as refunds
                
                FROM   awsdw_dm_billing.fact_aws_daily_est_revenue_reporting fct, 
                awsdw_dm_billing.dim_aws_accounts acct, 
                awsdw_dm_billing.dim_aws_products product
                WHERE  fct.account_seq_id = acct.account_seq_id 
                AND product.product_seq_id = fct.product_seq_id 
                AND product.product_code = 'AmazonRDS'       
                AND fct.computation_date >= '", min(wdates), "'", 
                "AND fct.computation_date <= '", max(wdates), "'", 
                "GROUP  BY fct.computation_date, 
                fct.charge_item_desc, 
                acct.account_id,
                fct.charge_period_start_date, 
                fct.charge_period_end_date, 
                product.product_code,
                fct.credit_id,
                charge_item_desc,
                fct.base_currency_code
                having  SUM(fct.billed_amount*amortization_factor) <0 or SUM(fct.refund_amount*amortization_factor) <0")
  
  conn <- dbConnect(driver, url)
  message("Getting AES credits and refund data. ")
  charge <- dbGetQuery(conn, SQL)
  
  message("- Query succeed: AES credits and refund data retrieved.")
  dbDisconnect(conn)
  
  charge <- merge(charge, customerList, by=c("account_id"), all.x=T)
  charge$account <- ifelse(is.na(charge$payer_account_id), charge$account_id, charge$payer_account_id)
  
  refunds <- filter(charge, refunds < 0)
  credits <- filter(charge, credits < 0)
  edp <- filter(refunds, grepl("enterprise|Enterprise", description))
  edp <- filter(edp, grepl("discount|Discount", description))

  t <- data.frame(credits_amount = sum(credits$credits),
                  credits_accounts = length(unique(credits$account)),
                  refunds_amount = sum(refunds$refunds),
                  refunds_accounts = length(unique(refunds$account)),
                  edp_amount = sum(edp$refunds),
                  edp_accounts = length(unique(edp$account)))
  return (t)
}

getCreditsHistory <- function(week, year, nWeek) {
  if (missing(nWeek)) {
    nWeek = 10
  }
  
  if (missing(week)) {
    week = weeknum()$week
  }
  if (missing(year)) {
    year = weeknum()$year
  }
  filename <- "credits_history_rds.csv"
  
  if(file.exists(filename)) {
    history <- read.csv(filename)
  } else {
    history <- data.frame()
  }
    
  for (i in c(1:nWeek)) {
    dt <- weeknum(week = week, year = year, weekDiff = -1*i)
   
    x<-calcCredits(week = dt$week, year=dt$year)
    x$week <- dt$week
    x$year <- dt$year
    
    history <- rbind(x, history)
    history <- unique(history)
    write.csv(history, filename, row.names = F)
  }
  
  return (history)
}


newDomainType <- function(nWeek) {
  if (missing(nWeek)) {
    nWeek = 8
  }
  
  x <- data.frame()
  for (i in c(nWeek:1)) {
    w1  <- weeknum(weekDiff = (-1*i))
    w2  <- weeknum(weekDiff = (-1*i - 1))
    
    meter1 <- getWeeklyMetering(week = w1$week, year = w1$year)
    meter2 <- getWeeklyMetering(week = w2$week, year = w2$year)
    
    newDomain <- filter(meter1, !meter1$usage_resource %in% meter2$usage_resource)
    message("Totally ", length(unique(newDomain$usage_resource)), " was created for week ", w1$week)
    newDomain <- unique(newDomain[, c("usage_resource", "usage_type")])
    newDomain <- transform(newDomain, tmp=colsplit(newDomain$usage_type, ":", names=c("1","2")))
    colnames(newDomain)[which(names(newDomain) == "tmp.2")] <- "instance_type"
    
    ag <- aggregate(usage_resource ~ instance_type, data = newDomain, FUN = length)
    cname <- paste0("domain_count_week", w1$week)
    t <- ag$usage_resource
    colnames(ag)[which(names(ag) == "usage_resource")] <- cname
    ag <- ag[, c("instance_type", cname)]
    
    t <- t / sum(t)
    ag$tmp <- t
    colnames(ag)[which(names(ag) == "tmp")] <- paste0("pct_", w1$week)
    
    if (is.null(x$instance_type)) {
      x <- ag
    } else {
      x <- merge(x, ag, by = c("instance_type"), all.x = T)
    }
    write.csv(x, "domainCount_vs_instanceType.csv")
  }
  
  write.csv(x, "domainCount_vs_instanceType.csv")
  return (x)
}

readDomainConfig <- function() {
  fname <- "/Users/maghuang/aws/domain_config/SFO-all-domains"
  l <- scan(fname, what = "", sep = "\n")
  txt <- paste(l, collapse = ",")
  txt <- paste("[", txt, "]", sep = "")
  config <- fromJSON(txt)
}

newDomainRevenue <- function() {
  meter <- getWeeklyMetering()
  lastWeek <- weeknum(weekDiff = -2)
  
  meterLast <- getWeeklyMetering(year = lastWeek$year, week = lastWeek$week)
  
  newDomains <- filter(meter, !meter$usage_resource %in% meterLast$usage_resource)
  
  price <- getPricePlan(2)
  
  rev <- getWeeklyCharges()
  lastRev <- getWeeklyCharges(year = lastWeek$year, week = lastWeek$week)
  
  
  
  
  return(newDomains)
  
}

domainConfig <- function () {
  conf <- readDomainConfig()
  
  customerList <- getCustomerData()
  price <- getPricePlan(2)
  price <- price[, c("Usage.Type", "Price.Unit")]
  
  conf <- merge(conf, customerList, by.x = "client_id", by.y="account_id", all.X = T)
  
  
  conf$data_node_usage_type <- paste0("ESInstance:", conf$data_node_type)
  conf <- merge(conf, price, by.x="data_node_usage_type", by.y = "Usage.Type")
  
  conf <- transform(conf, tmp=colsplit(conf$data_node_count, "x ", names=c("1","2")))
  conf$data_node_count <- conf$tmp.1
  
  conf$cost <- conf$Price.Unit * 24 * conf$data_node_count
  return (conf)
  
  
  # docs_bulk(conf, "sfo_domains")
  
}


trackFreeTierCost <- function(nDay) {
  
  price <- getPricePlan(2)
  
  price <- price[,c("Usage.Type", "Price.Unit")]
  names(price)[1] <- "usage_type"
  names(price)[2] <- "original_price"
  price$cost <- price$original_price / 1.4
  
  res <- data.frame()
  
  for (i in c(nDay:1)) {
    idx <- nDay - i + 1
    dateStr <- format(Sys.Date()-i, "%Y-%m-%d")
    rec <- data.frame(charge_date = dateStr)
    
    t <- getDailyCharge(dateStr)
    
    t <- filter(t, is_internal_flag == "N")
    t <- filter(t, offering_id=="607996" | offering_id=="615324")
    
    t2small <- filter(t, grepl("t2.small", usage_type))
    t2small <- merge(t2small, price, by="usage_type", all.x=T)
    t2small_free <- filter(t2small, price_per_unit == 0)
    
    
    t2micro <- filter(t, grepl("t2.micro", usage_type))
    t2micro <- merge(t2micro, price, by="usage_type", all.x=T)
    t2micro_free <- filter(t2micro, price_per_unit == 0)
    
    rec$t2.small.bill <- sum(t2small$billed_amount)
    rec$t2.micro.bill <- sum(t2micro$billed_amount)
    rec$t2.small.cost <- sum(t2small_free$usage_value * t2small_free$cost)
    rec$t2.micro.cost <- sum(t2micro_free$usage_value * t2micro_free$cost)

    res <- rbind(res, rec)
  }
  return(res)
  
}




