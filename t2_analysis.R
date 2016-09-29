library(RJDBC)
library(dplyr)
library(jsonlite)
library(stringr)
library(RCurl)

#A9 cluster
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-1.1.9.1009.jar", identifier.quote="`")
url <- "jdbc:redshift://a9dba-fin-rs2.db.amazon.com:8192/a9aws?user=maghuang&password=ThisPasswordIS5ecret&tcpKeepAlive=true&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"

getDomainUsage <- function(dateStr) {
  
  if (missing(dateStr)) {
    dateStr <- format(Sys.Date()-1, "%Y-%m-%d")
  }
  
  conn <- dbConnect(driver, url)
  
  message("- Connected to DB.")
  
  SQL <- paste("select distinct account_id, usage_resource, usage_type, sum(usage_value)
                from a9cs_metrics.es_daily_metering
                where request_day = '", dateStr,"'
                and usage_type like '%ESInstance%'
                group by 1,2,3", sep="")
  
  # SQL <- paste("select distinct account_id, usage_resource, usage_type, sum(usage_value)
  #               from a9cs_metrics.es_daily_metering
  #              where request_day >= '2016-09-10' and request_day <= '2016-09-21'
  #              and usage_type like '%ESInstance%'
  #              group by 1,2,3", sep="")
  
  message("- Running query: domain usagge data...")
  
  t <- dbGetQuery(conn, SQL)
  
  message("- Query succeed: domain usage data retrieved.")
  dbDisconnect(conn)
  
  tmp <- str_split_fixed(t$usage_type, ":", 2)
  
  t$instance_type <- tmp[,2]
  t$domain <- substring(str_match(t$usage_resource, "domain/[a-z0-9-]+"), 8, 100000)
  t$region <- substring(str_match(t$usage_resource, "arn:aws:es:[a-z0-9-]+"), 12, 100000)
  # 
  #   w <-aggregate(t$usage_resource, by=list(t$instance_type), FUN=length)
  # 
  #   names(w)[1] <- "instance_type"
  #   names(w)[2] <- "domain_count"
  #   s <- sum(w$domain_count)
  #   w$percentage <- w$domain_count / s
  # 
  #   return (w)
  return (t)
}

downloadTT <- function(nMon) {
  
  y <- format(Sys.Date(), "%Y")
  m <- format(Sys.Date(), "%m")
  d <- format(Sys.Date(), "%d")
  
  if(missing(nMon)) {
    nMon <- 3
  }
  
  regionList <- c("us-east-1", "eu-west-1", "us-west-2", "ap-northeast-1", "ap-southeast-1", "ap-northeast-2", "eu-central-1", "us-west-1", "ap-southeast-2", "sa-east-1", "ap-south-1")
  tt <- data.frame()
  for (region in regionList) {
    for (i in c(1:nMon)) {
      mon1 <- str_pad(as.numeric(m)-i, 2, pad = "0")
      mon2 <- str_pad(as.numeric(m)-i+1, 2, pad = "0")
      
      # url <- paste("https://tt.amazon.com/search?output=csv&category=&assigned_group=A9%20CloudSearch%20Eng&status=Assigned%3BResearching%3BWork%20In%20Progress%3BPending%3BResolved%3BClosed&impact=&assigned_individual=&requester_login=&login_name=&cc_email=&phrase_search_text=issues%20for%20swift-",region,"-prod&keyword_bq=issues%20for&exact_bq=&or_bq1=swift-", region, "-prod&or_bq2=&or_bq3=&exclude_bq=&create_date=", mon1,"%2F", d,"%2F", y,"%2C", mon2,"%2F", d, "%2F", y, "&modified_date=&tags=&case_type=&building_id=&search=Search%21", sep="")
      outputDir <- "/Users/maghuang/aws/tickets"
      if (!dir.exists(outputDir)) {
        dir.create(outputDir)
      }
       
      fname <- paste("ticket-", region, "-",y,mon2, ".csv", sep="")
      fpath <- file.path(outputDir, fname);
      # message(url)
      # message(fname)
      
      # download.file(url, fpath, "auto")
      t <- read.csv(fpath)
      tt <- rbind(tt, t)
      
    }
  }
  names(tt)<-gsub("\\.", "_", names(t))
  names(tt)<-gsub("-", "_", names(t))
  return (tt)
}


readTT <- function() {
  
  outputDir <- "/Users/maghuang/aws/tickets"
  if (!dir.exists(outputDir)) {
    dir.create(outputDir)
  }
  fileList <- dir(outputDir)
  
  tt <- data.frame()
  for (fname in fileList) {
    fpath <- file.path(outputDir, fname);
    t <- read.csv(fpath)
    tt <- rbind(tt, t)
  }
  
  
  uniqueTT <-  data.frame(Case_ID = unique(tt$Case_ID)) 

  
  uniqueTT <- merge(uniqueTT, tt, by = c("Case_ID"), all.x=TRUE)
  names(uniqueTT)<-gsub("\\.", "_", names(uniqueTT))
  names(uniqueTT)<-gsub("-", "_", names(uniqueTT))
  
  return (tt)
}

domainTypeAnalysis <- function(t) {
  
  if(missing(t)) {
    t <- read.csv("/Users/maghuang/aws/t2micro.csv")
  }
  
  t <- t[, c("Case_ID","Description", "Create_Date")]
  
  #find account_id
  t$account_id <- str_match(t$Description, "[0-9]{12}")
  
  # find domain name
  
  t$domain <- str_match(t$Description, "[:]{1}[a-z0-9-]+")
  
  # ticket exceptions.
  exception_tickets <- t[is.na(t$domain),]
  
  t <- t[!is.na(t$domain),]
  t$domain <- substr(t$domain, 2, 10000)
  
  # find region
  
  t$region <- str_match(t$Description, "swift-[a-z0-9-]+-prod")
  t$region <- substring(t$region, 7, str_length(t$region)-5)
  exception_tickets_1 <- t[is.na(t$region),]
  
  t <- t[!is.na(t$region),]
  
  # find instance type
  t$Create_Date <- format(as.Date(t$Create_Date), "%Y-%m-%d") 
  
  #split all tickets by creation date and find out which resource was used when the ticket was created
  
  allDates <- unique(t$Create_Date)
  res <- data.frame(account_id = character(), 
                    region = character(), 
                    domain= character(), 
                    Description = character(),
                    Case_ID = character(),
                    usage_type = character(),
                    sum = numeric(),
                    instance_type = character(),
                    usage_resource = character(),
                    stringsAsFactors = FALSE)
  
  for (d in allDates) {
    usage <- getDomainUsage(d)
    x <- t[t$Create_Date==d, ]
    m <- merge(x, usage, by=c("account_id", "region", "domain"), all.x=TRUE)
    res <- rbind(res, m)
  }
  return (res)
}

domainByInstanceType <- function(t) {
  if (missing(t)) {
    message("Please pass in the data set. You can get it from domainTypeAnalysis()")
    return ()
  }
  
  t <- t[!is.na(t$instance_type), ]
  
  aggByInstanceType <- aggregate(t$Case_ID, by=list(t$instance_type), FUN=length)
  
  
  
}

# free-tier customer's t2.micro instance

# non-paying customer's t2.micro instance

# cluster vs single-node analysis

nodeCountAnaysis <- function(dateStr, nDay) {
  if (missing(dateStr)) {
    dateStr <- format(Sys.Date()-1, "%Y-%m-%d")
  }
  if (missing(nDay)) {
    nDay = 1
  }
  
  
  # Of all AES domains, how many % are single node? Divide by instance type & node count
  
  # Get data for a specific date
  
  # Aggregate usage hours by account_id and domain name, then divide usage hours by 24 
  
  # Aggregate node type # by account_id and domain name
  
  # 
  
  # based on 
  
  # Of all ticketed domains, how many % are single node? Divided by instance type & node count
}





