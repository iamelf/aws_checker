library(RJDBC)
library(dplyr)
library(jsonlite)
library(elastic)
library(stringr)

driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-1.1.9.1009.jar", identifier.quote="`")
url <- "jdbc:redshift://a9dba-fin-rs2.db.amazon.com:8192/a9aws?user=maghuang&password=ThisPasswordIS5ecret&tcpKeepAlive=true&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"


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

dailyGainer <- function (dateStr, customerList) {
  if (missing(dateStr)) {
    t<- Sys.Date()-1
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dataStr)
      return()
    }
  }
  if (missing(customerList)) {
    message("Please pass in the customer info!")
    return ()
  }
  
  cust <- customerList[c("account_id", "company")]
  rm(customerList)
  
  d1 <- format(t-1, "%Y-%m-%d")
  d2 <- format(t, "%Y-%m-%d")
  
  t1 <- getDailyCharge(d1)
  t1 <- t1[t1$is_internal_flag=="N",]
  t2 <- getDailyCharge(d2)
  t2 <- t2[t2$is_internal_flag=="N",]
  
  d1 <- format(t-1, "%Y%m%d")
  d2 <- format(t, "%Y%m%d")
  
  ag1 <- aggregate(t1$billed_amount, by=list(t1$account_id), FUN=sum)
  ag2 <- aggregate(t2$billed_amount, by=list(t2$account_id), FUN=sum)
  
  
  colnames(ag1)[1] <- "account_id"
  name1 <- paste("charge_", d1, sep="")
  colnames(ag1)[2] <- name1
  colnames(ag2)[1] <- "account_id"
  name2 <- paste("charge_", d2, sep="")
  colnames(ag2)[2] <- name2
  
  chrg <- merge(ag1, ag2, by="account_id", all=TRUE)
  chrg <- merge(chrg, cust, by="account_id", all.x=TRUE)
  
  chrg[is.na(chrg)] <- 0
  
  chrg[,"diff"] <- chrg[name2] - chrg[name1]
  chrg <- chrg[order(-chrg$diff),]
  
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
    SQL <- paste("select account_id, region, billed_amount, is_internal_flag from es_weekly_charges
               where year = '2016' AND week='", week,"'", sep="")
    conn <- dbConnect(driver, url)
    
    message("- Connected to DB.")
    
    
    message("- Running query: weekly charge data...")
    t <- dbGetQuery(conn, SQL)
    t$account_id <- str_pad(as.character(t$account_id), 12, pad="0")
    message("- Query succeed: weekly charge for week", week, " retrieved.")
    dbDisconnect(conn)
    write.csv(t, fpath, sep=",")
  }
  return(t)
}

getAccountCharge <- function (dateStr, customerList) {
  if (missing(dateStr)) {
    t<- Sys.Date()-1
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dataStr)
      return()
    }
  }
  
  if(missing(customerList)) {
    message("Please pass in customer data!")
    return()
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
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dataStr)
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
                      pacct.account_id                                 AS PAYER_ID, 
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
                      FROM   fact_aws_daily_est_revenue fct, 
                      dim_aws_accounts acct, 
                      dim_aws_accounts pacct, 
                      dim_aws_offering_pricing_plans opp, 
                      dim_aws_usage_types ut, 
                      dim_aws_operations op, 
                      es_region_mapping rm,
                      dim_aws_products product
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
                      pacct.account_id, 
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
      message("Wrong date string: ", dataStr)
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

loadDaily2ES <- function(dateStr, nDay=1, customerList) {
  if (missing(dateStr)) {
    t <- Sys.Date()-1
  } else {
    t <- as.Date(dateStr) 
    if (abs(as.integer(Sys.Date()-t)) > 100) {
      message("Wrong date string: ", dataStr)
      return ()
    }
  }
  if (missing(customerList)) {
    message("Please passin customer data!")
    return ()
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
      dt <- getAccountCharge(dateStr, customerList = cust)
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
  } else {
    conn <- dbConnect(driver, url)
    message("- Connected to DB.")
    
    SQL <- "WITH account 
    AS (SELECT DISTINCT account_id, is_internal_flag 
    FROM   dim_aws_accounts 
    WHERE  end_effective_date IS NULL
    AND    current_record_flag = 'Y')   -- tt33060829 dupe stop
    SELECT aa.account_id, 
    coalesce(bb.account_field_value, 'e-mail Domain:' 
    || aa.email_domain) AS company, 
    aa.clear_name                                        name, 
    aa.clear_lower_email                                 email, 
    account.is_internal_flag 
    FROM   t_customers aa 
    left outer join o_aws_account_field_values bb 
    ON ( aa.account_id = bb.account_id 
    AND bb.account_fieldd = 2 ) 
    left outer join account 
    ON ( account.account_id = aa.account_id ) WHERE  aa.account_id IN (SELECT DISTINCT account_id 
    FROM   o_aws_subscriptions 
    WHERE  offeringd IN ( '26555', '113438', '128046' )
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
  
  cust <- getCustomerData()[, c("account_id", "company", "email")]
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
  
  rm(cust)
  
  for (i in regionList) {
    message("Generating top customer list for region ", i)
    chrg_i <- ag_i[ag_i$region == i, ]
    regionUsage  <- usage_i[usage_i$region == i, c("account_id", "usage_resource")]
    t <- merge(chrg_i, regionUsage, by="account_id", all.x=TRUE)
    t <- t[order(-t$billed_amount), ]
    
    fname <- paste("TopCustomer-internal-wk", week,"-",i,".csv", sep="")
    fpath <- file.path(outputDir, fname);
    write.csv(t, fpath)
    
    chrg_e <- ag_e[ag_e$region == i, ]
    regionUsage  <- usage_e[usage_e$region == i, c("account_id", "usage_resource")]
    t <- merge(chrg_e, regionUsage, by="account_id", all.x=TRUE)
    t <- t[order(-t$billed_amount), ]
    
    fname <- paste("TopCustomer-external-wk", week,"-",i,".csv", sep="")
    fpath <- file.path(outputDir, fname);
    write.csv(t, fpath)
  }
  zipname <- paste(outputDir, ".zip", sep="")
  zip(zipname, outputDir)
  
  message("Top customer lists saved to ", zipname)
}

getMonthlyCustomerCharge <- function() {
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  SQL <- "select computation_date, 
            client_product_code, 
            account_id, 
            usage_type, 
            usage_value, 
            billed_amount, 
            operation, 
            price_per_unit,
            region from es_daily_charges
            where computation_date >= '2016-08-01' 
              and computation_date <= '2016-08-31'
              and account_id in ('869367471674',
                                  '400278399699',
                                  '453596679811',
                                  '638079997145',
                                  '845761140255',
                                  '804447731759',
                                  '459366277915',
                                  '554845750457',
                                  '613753454641',
                                  '145882864129',
                                  '487263008213',
                                  '817208923784',
                                  '164340697576',
                                  '652491123196',
                                  '782628914375',
                                  '433255044884',
                                  '461056700594',
                                  '081988607789',
                                  '149436164766',
                                  '673391553318',
                                  '695254844687',
                                  '362249708244',
                                  '035874941075',
                                  '496267495478',
                                  '294872759125',
                                  '430267173975')"
  
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
  s$diff <- s$billed_amount - s$custom_billed_amount
  
  return (s)
}




