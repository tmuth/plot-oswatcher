#Sys.setenv(http_proxy="http://www-proxy.us.oracle.com:80")
#debugModeOverride <- TRUE  | rm(debugModeOverride)
#filePatternOverride <- "^awr-hist.+DB110g.+(\\.out|\\.gz)$" | rm(filePatternOverride)
filePatternOverride <- "^dm02cel0[0-1].+(\\.bz2|\\.gz)$"
setwd("M:/Dropbox/MyFiles/Projects/OSW Mining/OSW-Mining-Pure-R/iostat-data")


oswMinerPlotVersion <- '0.2.0'

list.of.packages <- c("futile.logger","ggplot2", "plyr","gridExtra","scales","reshape","xtable","ggthemes","stringr","data.table","lubridate","gplots")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) {
  options(repos="http://cran.cnr.Berkeley.edu")
  install.packages(new.packages,dependencies = TRUE)  
}


lapply(list.of.packages, function(x) {
  library(x,character.only=TRUE,quietly = TRUE)
})

oswM <- new.env()
oswMdata <- new.env()
flog.threshold(DEBUG)
oswM$debug <- new.env()
oswMdata$dataFiles <- data.frame()




oswM$debug.unitTimes <- data.frame()

appender.fn <- function(lineIn) { 
  lineVars <- str_extract_all(lineIn, "\\[.+?\\]")
  
  strFormat <- function(stringIn){
    stringInternal <- str_replace_all(stringIn,"\\[|\\]|\\n","")
    return(stringInternal)
  }
  
  lineVars <- lapply(lineVars,FUN=strFormat)
  
  oppCode <- as.character("")
  
  if(str_detect(lineVars[[1]][3]," - start$")){
    oppCode <- "start"
    lineVars[[1]][3] <- str_replace(lineVars[[1]][3]," - start$","")
  }
  
  if(str_detect(lineVars[[1]][3]," - end$")){
    oppCode <- "end"
    lineVars[[1]][3] <- str_replace(lineVars[[1]][3]," - end$","")
  }
  
  oswM$debug.unitTimes <<- rbind(oswM$debug.unitTimes,data.frame(db=oswM$cellName,level=lineVars[[1]][1],time=lineVars[[1]][2],message=lineVars[[1]][3],opp=oppCode))
  print(lineIn)
}

#if(debugMode){
  flog.appender(appender.fn)
  
  layout <- layout.format('[~l] [~t] [~m]')
  flog.layout(layout)
#}



filePattern <- "^awr-hist*.*(\\.out|\\.gz)$"
if(exists("filePatternOverride")){
  if(!is.null(filePatternOverride)){
    if(nchar(filePatternOverride)>1){
      filePattern <- filePatternOverride
    }
  }
}


print('Looking in this directory:')
print(getwd())
print(paste0('for files that match the pattern: ',filePattern))
Sys.sleep(2)





parseFile <- function(inFileName,inCellName){
  print(inFileName)
  con  <- file(inFileName, open = "r")
  
  
  maxLoops <- 200000000000000000
  #maxLoops <- 500
  loopCounter <- 0
  baseDate <- NULL
  baseDateTime <- NULL
  baseTimeZone <- NULL
  dataBlock <- NULL
  dataBlockRowCounter <- 0
  currentBlockTime <- NULL
  oswM$DT_IOSTAT <- data.table()
  flog.debug('mainLoop - start')
  while (length(oneLine <- readLines(con, n = 1, warn = FALSE)) > 0) {
    if(is.null(baseDateTime)){
      if(str_detect(oneLine,"^zzz")){ #Found the base date block
        baseDateTmp <- str_replace(oneLine,'^[z\\* ]+','')
        baseDateTmp <- str_replace(baseDateTmp,'(.+)(Sample interval.*)$','\\1')
        baseDateTmp <- str_trim(baseDateTmp)
        
        baseTimeZone <- str_replace(baseDateTmp,'^(.+) ([A-Z]{3,3}) ([0-9]{4,4})$','\\2') #Extract the timezone
        baseDateTmp <- str_replace(baseDateTmp,paste0(baseTimeZone," "),"") #Replace the timezone with null
        
        baseDateTime <- as.POSIXct(baseDateTmp,format="%a %b %d %H:%M:%S %Y",TZ=baseTimeZone) #Convert to a true date format
        baseDate <- round_date(baseDateTime,"day")
        #print(baseDateTmp)
        #print(baseDate)
      }
    }
    
    if(str_detect(oneLine,fixed("Time"))){ #Found a Time block 
      #print(oneLine)
      
      
      dataBlock <- NULL
      currentBlockTimeTmp <- str_replace(oneLine,'Time: ','')
      
      dateStringTmp <- paste0(strftime(baseDate,format="%Y/%m/%d")," ",currentBlockTimeTmp)
      currentBlockTime <- as.POSIXct(dateStringTmp,format="%Y/%m/%d %H:%M:%S",TZ=baseTimeZone) #Convert to a true date format
      
      #print(currentBlockTime)
      cpuBlock1 <- readLines(con, n = 1, warn = FALSE)
      cpuBlock1 <- str_replace(cpuBlock1,'avg-cpu:  ','')
      cpuBlock1 <- str_replace_all(cpuBlock1,'%','')
      cpuBlock2 <- readLines(con, n = 1, warn = FALSE)  
      
      cpuBlock2 <- str_trim(cpuBlock2,side = "left")
      #print(oneLine)
      cpuBlock <- paste0(cpuBlock1,"\n",cpuBlock2,"\n")
      
      dfInt = read.table(file=textConnection(cpuBlock),header=TRUE)
      #print(head(dfInt))
    }
    
    #if(str_detect(oneLine,"^Device|^sd|^md")){ #Found a Time block 
    if(nchar(oneLine) > 85){ # going to try this to see if it's faster
      dataBlock <- paste0(dataBlock,"\n",oneLine)
      dataBlockRowCounter <- dataBlockRowCounter + 1
      #print(oneLine)
      #print(nchar(oneLine))
    }
    
    if(!is.null(dataBlock)){
      if(nchar(dataBlock) > 5000){
        if(str_detect(oneLine,"^$")){
          #print(cat(dataBlock))
          #print(nzchar(dataBlock))
          #flog.debug('read.table - start')
          dfInt2 = read.table(file=textConnection(dataBlock),header=TRUE,comment.char="",stringsAsFactors=FALSE,
                              nrows=dataBlockRowCounter,
                              colClasses=c("character",
                                           "numeric","numeric","numeric","numeric","numeric",
                                           "numeric","numeric","numeric","numeric","numeric",
                                           "numeric")
                              )
          #dfInt2 = fread(input=as.character(dataBlock),header=TRUE)
          #print(head(dfInt2))
          #flog.debug('read.table - end')
          #print(head(dfInt2,n=10))
          dfInt2<- data.table(dfInt2)
          dfInt2 <- rename(dfInt2, c("Device."="device","X.util"="util.pct","r.s"="r.iops","w.s"="w.iops"))
          
          
          dfInt2$deviceType <- "U" # mark all devices as type unknown
          idx_disk <- with(dfInt2, grepl("^sd[a-m]{1}$", device,perl=TRUE))
          dfInt2[idx_disk,]$deviceType <- "D" #marks disks with a D
          
          idx_flash <- with(dfInt2, grepl("^sd[n-z]{1}$|^sda[a-c]{1}$", device,perl=TRUE))
          dfInt2[idx_flash,]$deviceType <- "F" #mark flash with a F
          setkey(dfInt2,deviceType)
          idx_unknown <- !with(dfInt2, deviceType == "U")
          dfInt2<- dfInt2[idx_unknown,] #delete the Unknown rows
          
          
          dfInt2$dateTime <- currentBlockTime
          dfInt2$name <- inCellName
          dfInt2$R.MB.s <- round((dfInt2$rsec.s * 512)/1024/1024,1) # 512b sector size, convert to MB
          dfInt2$W.MB.s <- round((dfInt2$wsec.s * 512)/1024/1024,1)
          dfInt2 <- subset( dfInt2, select = -c(avgrq.sz,rrqm.s,wrqm.s,rsec.s,wsec.s) )
          #dfInt2 <- dfInt2[c("name", "dateTime", "deviceType","device","r.iops", "w.iops", "R.MB.s", "W.MB.s", "avgqu.sz", "await", "svctm", "util.pct")] # reorder the columns
          #setcolorder(dfInt2,c("name", "dateTime", "deviceType","device","r.iops", "w.iops", "R.MB.s", "W.MB.s", "avgqu.sz", "await", "svctm", "util.pct")) # reorder the columns
          dfInt2_grp <- dfInt2[,list(r.iops=sum(r.iops),w.iops=sum(w.iops),R.MB.s=sum(R.MB.s),W.MB.s=sum(W.MB.s),
                                     avgqu.sz=median(avgqu.sz),await=median(await),util.pct=median(util.pct)),
                               by=list(name,dateTime,deviceType)]
          oswMdata$DT_IOSTAT <- rbind(oswMdata$DT_IOSTAT,dfInt2_grp)
          rm(dfInt2)
          dataBlockRowCounter <- 0
          #dfInt2[with(dfInt2, grepl("^sd[a-m]{1}", "Device:",perl=TRUE)),]$deviceType<-"D"
          #print(head(dfInt2,n=60))
        }
      }
    }
    
    if(loopCounter  >=  maxLoops){
      break
      
    }
    else{
      loopCounter <- loopCounter + 1
    }
    
    #print(oneLine)
  } 
  flog.debug('mainLoop - end')
  close(con)
}



oswM$oswfiles <- list.files(pattern=filePattern)
#dm02cel01.aim.com_iostat_12.02.29.1100.dat.bz2
#namePattern <- "awr-hist-([0-9]+)-([a-zA-Z0-9_]+)-.*"
namePattern <- "([a-zA-Z0-9_]+)\\..*"

if(file.exists('oswMinerData.Rda')){
  load(file='oswMinerData.Rda')
}

for (f in oswM$oswfiles) {
  #main$db_name <- c(main$db_name,gsub(pattern = "([a-zA-Z0-9_]+)-.*", replacement="\\1", f))
  oswM$currentFileName <- f
  oswM$cellName <- c(oswM$cellName,gsub(pattern = namePattern, replacement="\\1", f))
  parseFile(f,oswM$cellName)
  oswMdata$dataFiles <- rbind(oswMdata$dataFiles,data.frame(date=now(),version=oswMinerPlotVersion,fileName=f))
  #main$db_id <- c(main$db_id,gsub(pattern = namePattern, replacement="\\2", f))
  flog.info(paste0('Found file for: ',gsub(pattern = namePattern, replacement="\\1", f)))
  flog.trace(f)
}




oswM$debug.unitTimes[oswM$debug.unitTimes == ""] <- NA


oswM$debug.unitTimesWide <- reshape(subset(na.omit(oswM$debug.unitTimes),length(oswM$debug.unitTimes$opp)>2), 
                                     timevar = "opp",
                                     idvar = c("db", "level", "message"),
                                     direction = "wide")

oswM$debug.unitTimesWide$time.start <- as.POSIXct(oswM$debug.unitTimesWide$time.start, format = "%Y-%m-%d %H:%M:%S",tz="UTC")
oswM$debug.unitTimesWide$time.end <- as.POSIXct(oswM$debug.unitTimesWide$time.end, format = "%Y-%m-%d %H:%M:%S",tz="UTC")
oswM$debug.unitTimesWide$duration <- difftime(oswM$debug.unitTimesWide$time.end , oswM$debug.unitTimesWide$time.start , unit="secs")
print(head(oswM$debug.unitTimesWide,30))
save(oswM,file="oswM.Rda")
oswM$DT_IOSTAT
save(oswM,file="oswM-DT_IOSTAT.Rda")
save(oswMdata,file="oswMinerData.Rda")





fileName <- 'dm02cel01.aim.com_iostat_12.02.29.1100.dat.bz2'
con  <- file(fileName, open = "r")
#wholeFile <- readChar(con, file.info(fileName)$size)
wholeFile <- readChar(con,nchars=1e8)
close(con)
wholeFileLines <- strsplit( wholeFile,"\n",fixed=T,useBytes=F)
wholeFileLines <- unlist(wholeFileLines)
# for (i in wholeFileLines){
#   if(str_detect(i,fixed("Time"))){
#    print(i) 
#   }
# }

for (i in 1:length(wholeFileLines)){
  
  if(str_detect(wholeFileLines[i],fixed("Time"))){
    #i=i+1
    #print(wholeFileLines[i]) 
  }
  
  #if(str_detect(wholeFileLines[i],"^Device|^sd|^md")){ #Found a Time block 
  if(nchar(wholeFileLines[i]) > 85){
  
  }
  
#   if(i>200){
#     break
#   }
}




