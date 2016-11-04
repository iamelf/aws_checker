# Some MacOS security feature prevent this library being loaded. The following line is a quick hack.
dyn.load('/Library/Java/JavaVirtualMachines/jdk1.8.0_111.jdk/Contents/Home/jre/lib/server/libjvm.dylib')

library(RJDBC)
library(dplyr)
library(jsonlite)
library(elastic)
library(stringr)


#A9 cluster
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-1.1.9.1009.jar", identifier.quote="`")
#url <- "jdbc:redshift://a9dba-fin-rs2.db.amazon.com:8192/a9aws?user=maghuang&password=pw20160926NOW&tcpKeepAlive=true&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"

#AWS database
url <- "jdbc:postgresql://54.85.28.62:8192/datamart?user=maghuang&password=Youcan88$&tcpKeepAlive=true"



# Daily analysis 
# * daily external, internal revenue
# * Total credit daily
# * Top customers
# * Top gainers, top losers
# * Count of total paying customer accounts
# * Count of 0 revenue accounts
# * Single Customer usage trending


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
  
  t1 <- getDailyCharge(d1)
  t1 <- t1[t1$is_internal_flag=="N",]
  t2 <- getDailyCharge(d2)
  t2 <- t2[t2$is_internal_flag=="N",]
  
  ag1 <- aggregate(t1$usage_value, by=list(t1$usage_type), FUN=sum)
  ag2 <- aggregate(t2$usage_value, by=list(t2$usage_type), FUN=sum)
  
  
  colnames(ag1)[1] <- "usage_type"
  colnames(ag1)[2] <- "usage_yesterday"
  colnames(ag2)[1] <- "usage_type"
  colnames(ag2)[2] <- "usage_today"
  
  chrg <- merge(ag1, ag2, by="usage_type", all=TRUE)
  
  chrg[is.na(chrg)] <- 0
  
  chrg[,"diff"] <- chrg["usage_today"] - chrg["usage_yesterday"]
  chrg <- chrg[order(-chrg$diff),]
  chrg$date <- dateStr
  
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


getWeeklyCharges <- function(week) {
  if (missing(week)) {
    week <- weeknum() - 1
  }
  
  message("- Getting weekly charge data for week ", week)
  
  outputDir <- "/Users/maghuang/aws/weekly"
  if (!dir.exists(outputDir)) {
    dir.create(outputDir)
  }
  
  fname <- paste("WeeklyCharge_wk", week, ".csv", sep="")
  fpath <- file.path(outputDir, fname);
  
  if (file.exists(fpath)) {
    t <- read.csv(fpath, sep=",")
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
  } else {
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
    write.csv(t, fpath, sep=",")
  }
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
    
    sqldaily <- paste("SELECT fct.computation_date, 
       opp.client_product_code, 
                      fct.charge_item_desc, 
                      acct.account_id, 
                      pacct.payer_account_id                                 AS PAYER_ID, 
                      fct.charge_period_start_date, 
                      fct.charge_period_end_date, 
                      product.product_code, 
                      ut.usage_type, 
                      sum(fct.usage_value * fct.amortization_factor)   AS usage_value, 
                      opp.pricing_plan_id, 
                      opp.offering_id, 
                      opp.rate_id, 
                      Sum(fct.billed_amount * fct.amortization_factor) AS billed_amount, 
                      op.operation, 
                      opp.price_per_unit,
                      fct.credit_id,
                      rm.region,
                      acct.is_internal_flag 
                      FROM   awsdw_dm_billing.fact_aws_daily_est_revenue_current fct, 
                      awsdw_dm_billing.dim_aws_accounts acct, 
                      awsdw_dm_billing.dim_aws_accounts pacct, 
                      awsdw_dm_billing.dim_aws_offering_pricing_plans opp, 
                      awsdw_dm_billing.dim_aws_usage_types ut, 
                      awsdw_dm_billing.dim_aws_operations op, 
                      a9cs_metrics.es_region_mapping rm,
                      awsdw_dm_billing.dim_aws_products product
                      WHERE  fct.account_seq_id = acct.account_seq_id 
                      AND fct.payer_account_seq_id = pacct.account_seq_id 
                      AND fct.offering_pricing_plan_seq_id = opp.offering_pricing_plan_seq_id 
                      AND fct.usage_type_seq_id = ut.usage_type_seq_id 
                      AND fct.operation_seq_id = op.operation_seq_id 
                      AND fct.operation_seq_id = op.operation_seq_id 
                      AND product.product_seq_id = fct.product_seq_id 
                      AND product.product_code = 'AmazonES' 
                      AND ut.usage_type = rm.usage_type        
                      AND fct.computation_date = ('", dateStr,"') 
                      GROUP  BY fct.computation_date, 
                      opp.client_product_code, 
                      fct.charge_item_desc, 
                      acct.account_id, 
                      pacct.payer_account_id,
                      fct.charge_period_start_date, 
                      fct.charge_period_end_date, 
                      product.product_code, 
                      ut.usage_type, 
                      opp.pricing_plan_id, 
                      opp.offering_id, 
                      opp.rate_id, 
                      op.operation, 
                      opp.price_per_unit, 
                      fct.credit_id,
                      rm.region,
                      acct.is_internal_flag 
                      ORDER  BY acct.account_id ASC", sep="")
    
    
    # sqldaily <- paste("SELECT fct.computation_date,
    #    opp.client_product_code,
    #                   fct.charge_item_desc,
    #                   acct.account_id,
    #                   pacct.payer_account_id                                 AS PAYER_ID,
    #                   fct.charge_period_start_date,
    #                   fct.charge_period_end_date,
    #                   product.product_code,
    #                   ut.usage_type,
    #                   sum(fct.usage_value * fct.amortization_factor)   AS usage_value,
    #                   opp.pricing_plan_id,
    #                   opp.offering_id,
    #                   opp.rate_id,
    #                   Sum(fct.billed_amount * fct.amortization_factor) AS billed_amount,
    #                   op.operation,
    #                   opp.price_per_unit,
    #                   fct.credit_id,
    #                   rm.region,
    #                   acct.is_internal_flag
    #                   FROM   fact_aws_daily_est_revenue_current fct,
    #                   dim_aws_accounts acct,
    #                   dim_aws_accounts pacct,
    #                   dim_aws_offering_pricing_plans opp,
    #                   dim_aws_usage_types ut,
    #                   dim_aws_operations op,
    #                   es_region_mapping rm,
    #                   dim_aws_products product
    #                   WHERE  fct.account_seq_id = acct.account_seq_id
    #                   AND fct.payer_account_seq_id = pacct.account_seq_id
    #                   AND fct.offering_pricing_plan_seq_id = opp.offering_pricing_plan_seq_id
    #                   AND fct.usage_type_seq_id = ut.usage_type_seq_id
    #                   AND fct.operation_seq_id = op.operation_seq_id
    #                   AND fct.operation_seq_id = op.operation_seq_id
    #                   AND product.product_seq_id = fct.product_seq_id
    #                   AND product.product_code = 'AmazonES'
    #                   AND ut.usage_type = rm.usage_type
    #                   AND fct.computation_date = ('", dateStr,"')
    #                   GROUP  BY fct.computation_date,
    #                   opp.client_product_code,
    #                   fct.charge_item_desc,
    #                   acct.account_id,
    #                   pacct.payer_account_id,
    #                   fct.charge_period_start_date,
    #                   fct.charge_period_end_date,
    #                   product.product_code,
    #                   ut.usage_type,
    #                   opp.pricing_plan_id,
    #                   opp.offering_id,
    #                   opp.rate_id,
    #                   op.operation,
    #                   opp.price_per_unit,
    #                   fct.credit_id,
    #                   rm.region,
    #                   acct.is_internal_flag
    #                   ORDER  BY acct.account_id ASC", sep="")


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
  
  #esBase <- "https://search-testkibana-6cicgmffkxcr6qbgwbu7jgplo4.us-west-1.es.amazonaws.com"
  #esBase <- "https://search-testkibana-zl2kau6porzgk7jv3g26tigus4.sa-east-1.es.amazonaws.com"
  #esBase <- "https://search-maggietest-xxsbuyd663jbcikgukmxueetba.us-east-1.es-integ.amazonaws.com"
  #esPort <- 443
  
  esBase <- "http://10.49.45.188"
  #esBase <- "http://admin:awsrocks@10.49.32.92"
  esPort <- 9200
  
  
  esUser <- NULL
  esPwd <- NULL
  
  connect(es_base = esBase, es_port = esPort, es_user=esUser, es_pwd = esPwd)
  
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
    
    indexName <- paste("account-charge-", dateStr,sep="")
    
    if (index_exists(indexName)) {
      message("Index ", indexName, " already exists. Skip loading.")
    } else {
      message("Loading index ", indexName, " into Elasticsearch...")
      dt <- getAccountCharge(dateStr)
      # create index with mapping
      body <- paste('{
        "settings" : {
          "number_of_shards" : 1
        },
        "mappings": {
          "', indexName, '": {
            "properties": {
              "account_id" : {"type" : "string", "index":  "not_analyzed"},
              "company" : {"type" : "string", "index":  "not_analyzed"}
            }
          }
        }
      }', sep="")
      index_create(indexName, body=body)      
      docs_bulk(dt, indexName)
      message("Index ", indexName, " loading complete!")
    }
    
    indexName <- paste("gainer-", dateStr,sep="")
    
    if (index_exists(indexName)) {
      message("Index ", indexName, " already exists. Skip loading.")
    } else {
      message("Loading index ", indexName, " into Elasticsearch...")
      dt <- dailyGainer(dateStr)
      # create index with mapping
      body <- paste('{
                    "settings" : {
                    "number_of_shards" : 1
                    },
                    "mappings": {
                    "', indexName, '": {
                    "properties": {
                    "account_id" : {"type" : "string", "index":  "not_analyzed"},
                    "company" : {"type" : "string", "index":  "not_analyzed"}
                    }
                    }
                    }
                }', sep="")
      index_create(indexName, body=body)      
      docs_bulk(dt, indexName)
      message("Index ", indexName, " loading complete!")
    }
    
    indexName <- paste("usage-diff-", dateStr,sep="")
    
    if (index_exists(indexName)) {
      message("Index ", indexName, " already exists. Skip loading.")
    } else {
      message("Loading index ", indexName, " into Elasticsearch...")
      dt <- dailyUsageDiff(dateStr)
      # create index with mapping
      body <- paste('{
                    "settings" : {
                    "number_of_shards" : 1
                    },
                    "mappings": {
                    "', indexName, '": {
                    "properties": {
                    "usage_type" : {"type" : "string", "index":  "not_analyzed"}
                    }
                    }
                    }
    }', sep="")
      index_create(indexName, body=body)      
      docs_bulk(dt, indexName)
      message("Index ", indexName, " loading complete!")
    }
    
    indexName <- paste("usage-charge-diff-", dateStr,sep="")
    
    if (index_exists(indexName)) {
      message("Index ", indexName, " already exists. Skip loading.")
    } else {
      message("Loading index ", indexName, " into Elasticsearch...")
      dt <- dailyUsageChargeDiff(dateStr)
      # create index with mapping
      body <- paste('{
                    "settings" : {
                    "number_of_shards" : 1
                    },
                    "mappings": {
                    "', indexName, '": {
                    "properties": {
                    "usage_type" : {"type" : "string", "index":  "not_analyzed"}
                    }
                    }
                    }
    }', sep="")
      index_create(indexName, body=body)      
      docs_bulk(dt, indexName)
      message("Index ", indexName, " loading complete!")
    }
    
  }
}


getCustomerData <- function () {
  
  outputDir <- "/Users/maghuang/aws/weekly"
  if (!dir.exists(outputDir)) {
    dir.create(outputDir)
  }
  
  fname <- paste("CustomerData-wk", weeknum(),".csv", sep="")
  fpath <- file.path(outputDir, fname)
  
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
    WHERE  offering_id IN ( '26555', '113438', '128046' )
    AND ( end_date IS NULL 
    OR end_date >= ( SYSDATE - 8 ) ));"
    
    message("- Running query: customer data...")
    t <- dbGetQuery(conn, SQL)
    
    t$account_id <- str_pad(t$account_id, 12, pad="0")
    message("- Query succeed: customer data retrieved.")
    dbDisconnect(conn)
    
    write.csv(t, fpath, sep=",")
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
  
  
  
  queryStr <- paste("'", all_accounts[1], "'", sep = "")
  for (i in all_accounts) {
    queryStr<- paste(queryStr, ", '", i, "'", sep = "")
  }
  
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  SQL <- paste("select computation_date, 
            client_product_code, 
            account_id, 
            usage_type, 
            usage_value, 
            billed_amount, 
            operation, 
            price_per_unit,
            region from es_daily_charges
            where computation_date >= '2016-01-01' 
              and account_id in (", queryStr,")", sep = "")
  
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
  
  SQL <- "select * from a9cs_metrics.es_new_inactive_customers"
  
  message("- Running query: weekly usage data...")
  t <- dbGetQuery(conn, SQL)
  t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
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








