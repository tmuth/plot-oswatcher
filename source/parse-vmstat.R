#Sys.setenv(http_proxy="http://www-proxy.us.oracle.com:80")
#debugModeOverride <- TRUE  | rm(debugModeOverride)
#filePatternOverride <- "^awr-hist.+DB110g.+(\\.out|\\.gz)$" | rm(filePatternOverride)
#filePatternOverride <- "^dm02cel0[0-1].+(\\.bz2|\\.gz)$"
filePatternOverride <- "*.dat"

#setwd("M:/Dropbox/MyFiles/GitHub/plot-oswatcher/data/vmstat-data")


oswParseVMSTATversion <- '0.3.0'
oswM$DT_VMSTAT <- data.table()


list.of.packages <- c("futile.logger","ggplot2", "dplyr","gridExtra","scales","reshape","xtable","ggthemes","stringr","data.table","lubridate","gplots")
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
  
  #oswM$debug.unitTimes <<- rbind(oswM$debug.unitTimes,data.frame(db=oswM$cellName,level=lineVars[[1]][1],time=lineVars[[1]][2],message=lineVars[[1]][3],opp=oppCode))
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


compareFile <- function(inFileName,inFileInfo){
  flog.debug('compareFile - start')
  shouldParseFile <- TRUE
  return(TRUE)
  #print(head(oswMdata$dataFiles))
  tmpDT <- oswMdata$dataFiles[oswMdata$dataFiles$fileName == inFileName & oswMdata$dataFiles$modifiedDate <= inFileInfo$mtime & oswMdata$dataFiles$size == inFileInfo$size,]
  
  #print(head(tmpDT))
  
  if(ncol(tmpDT) > 0 & nrow(tmpDT) > 0){
    if(oswParseVMSTATversion == tmpDT$version){
      shouldParseFile <- FALSE
    }
  }
  
  

  flog.debug(paste0('compareFile - okToParse = ',shouldParseFile))
  
  flog.debug('compareFile - end')
  return(shouldParseFile)
}


updateFileMetadata <- function(inFileName,inFileInfo,inOkToParse){
  if(inOkToParse){
    print(inFileName)
    print(str(inFileInfo))
    print(inFileInfo$mtime)
    print(head(oswMdata$dataFiles))
    
    
    if(("fileName" %in% names(oswMdata$dataFiles))){
      print(head(oswMdata$dataFiles))
      print(nrow(oswMdata$dataFiles))
      #oswMdata$dataFiles <<- oswMdata$dataFiles[oswMdata$dataFiles$fileName != inFileName]
      print(nrow(oswMdata$dataFiles))
      print(head(oswMdata$dataFiles))
    }
    
    oswMdata$dataFiles <<- rbind(oswMdata$dataFiles,data.frame(parseDate=now(),version=oswParseVMSTATversion,fileName=f,
                                                              modifiedDate=fileInfo$mtime,size=fileInfo$size))
  }
}



parseVMSTATfile <- function(inFileName,inCellName){
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

  
  
  
  timeBlockParse <- function(fileLineIn){
    
    if(str_detect(fileLineIn,"^zzz")){ #Found a time block
      #flog.debug('timeBlock - start')
      dataBlock <<- NULL
      baseDateTmp <- str_replace(fileLineIn,'^[z\\* ]+','')
      baseDateTmp <- str_replace(baseDateTmp,'(.+)(Sample interval.*)$','\\1')
      baseDateTmp <- str_trim(baseDateTmp)
      
      baseTimeZone <- str_replace(baseDateTmp,'^(.+) ([A-Z]{3,3}) ([0-9]{4,4})$','\\2') #Extract the timezone
      baseDateTmp <- str_replace(baseDateTmp,paste0(baseTimeZone," "),"") #Replace the timezone with null
      
      currentBlockTimeInt <- as.POSIXct(baseDateTmp,format="%a %b %d %H:%M:%S %Y",TZ=baseTimeZone) #Convert to a true date format
      return(currentBlockTimeInt)
      #baseDate <- round_date(baseDateTime,"day")
      #print(baseDateTmp)
      #print(baseDate)
    }
  }
  
  
  flog.debug('mainLoop - start')
  while (length(oneLine <- readLines(con, n = 1, warn = FALSE)) > 0) {

    
    
    
    # --------------------------------------------------------------
    
    
    
    #if(str_detect(oneLine,"^Device|^sd|^md")){ #Found a Time block 
    #if(nchar(oneLine) > 50){ # going to try this to see if it's faster
    if(str_detect(oneLine,"^[0-9 ]+$") | str_detect(oneLine,"^(r|b| )+.+free.*$")){ 
      #flog.debug('dataBlock - start')
      dataBlock <- paste0(dataBlock,"\n",oneLine)
      dataBlockRowCounter <- dataBlockRowCounter + 1
      #print(oneLine)
      #print(nchar(oneLine))
    }
    
    if(!is.null(dataBlock)){
      if(nchar(dataBlock) > 100){
        if(str_detect(oneLine,"^zzz") | str_detect(oneLine,"^$")){
          #flog.debug('dataBlock - read.table')
          #print(cat(dataBlock))
          #print(nzchar(dataBlock))
          #flog.debug('read.table - start')
          dfInt2 = read.table(file=textConnection(dataBlock),header=TRUE,comment.char="",stringsAsFactors=FALSE,
                              nrows=dataBlockRowCounter
                              #colClasses=numeric
#                               colClasses=c(
#                                            "numeric","numeric","numeric","numeric","numeric",
#                                            "numeric","numeric","numeric","numeric","numeric",
#                                            "numeric","numeric","numeric","numeric","numeric",
#                                            "numeric","numeric","numeric","numeric","numeric",
#                                            "numeric","numeric")
                              )
          #dfInt2 = fread(input=as.character(dataBlock),header=TRUE)
          #print(head(dfInt2))

          if(("sy.1" %in% names(dfInt2))){ # Solaris and HPUX have 2 columns named "sy"
            dfInt2 <- subset( dfInt2, select = -c(sy) )
            dfInt2 <- rename(dfInt2, c("sy.1"="sy","pi"="si","po"="so"))
          }
          
          if(("swpd" %in% names(dfInt2))){ # Linux
            dfInt2 <- rename(dfInt2, c("swpd"="swap"))
          }

          if(("avm" %in% names(dfInt2))){ # Linux
            dfInt2 <- rename(dfInt2, c("avm"="swap"))
          }



          dfInt2 <- subset( dfInt2, select = c(swap,free,si,so,us,sy,id) )
          
          #print(head(dfInt2))
          #flog.debug('read.table - end')
          #print(head(dfInt2,n=10))
          dfInt2<- data.table(dfInt2)
          dfInt2 <- rename(dfInt2, c("swap"="mem.swap","free"="mem.free","si"="swap.in.kb","so"="swap.out.kb",
                                     "us"="cpu.user","sy"="cpu.sys","id"="cpu.idle"))
          
          
          
          
          dfInt2$dateTime <- currentBlockTime
          dfInt2$name <- inCellName
          #print(head(dfInt2))
          
          #dfInt2 <- dfInt2[c("name", "dateTime", "deviceType","device","r.iops", "w.iops", "R.MB.s", "W.MB.s", "avgqu.sz", "await", "svctm", "util.pct")] # reorder the columns
          #setcolorder(dfInt2,c("name", "dateTime", "deviceType","device","r.iops", "w.iops", "R.MB.s", "W.MB.s", "avgqu.sz", "await", "svctm", "util.pct")) # reorder the columns

                               #by=list(name,dateTime,deviceType,CPU.user)]
          oswMdata$DT_VMSTAT <- rbind(oswMdata$DT_VMSTAT,dfInt2)
          rm(dfInt2)
          dataBlockRowCounter <- 0
          #currentBlockTime <- timeBlockParse(oneLine)
          #dfInt2[with(dfInt2, grepl("^sd[a-m]{1}", "Device:",perl=TRUE)),]$deviceType<-"D"
          #print(head(dfInt2,n=60))
        }
      }
    }
    
    if(str_detect(oneLine,"^zzz")){
      currentBlockTime <- timeBlockParse(oneLine)
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
namePattern <- "([a-zA-Z0-9\\-]+).*"

if(file.exists('oswVMSTATdata.Rda')){
  load(file='oswVMSTATdata.Rda')
}

for (f in oswM$oswfiles) {
  okToParseFile <- TRUE
  #flog.info(paste0('Found file : ',f))
  flog.debug('fileLoop - start')
  print(f)
  #flog.debug(paste0('File: ',f[[1]]))
  
  oswM$currentFileName <- f
  oswM$cellName <- gsub(pattern = namePattern, replacement="\\1", f)
  #oswM$cellName <- c(oswM$cellName,gsub(pattern = namePattern, replacement="\\1", f))
  
  fileInfo <- file.info(f)
  #okToParseFile <- compareFile(f,fileInfo)
  
  if(okToParseFile){
    parseVMSTATfile(f,oswM$cellName)
  }
  
  print(paste0("Rows in DT: ",nrow(oswMdata$DT_VMSTAT)))
  print(unique(oswMdata$DT_VMSTAT$name))
  
  updateFileMetadata(f,fileInfo,okToParseFile)
  
  
  
  #main$db_id <- c(main$db_id,gsub(pattern = namePattern, replacement="\\2", f))
  
  flog.trace(f)
}



oswMdata$DT_VMSTAT$cpu.busy <- (100-oswMdata$DT_VMSTAT$cpu.idle)



# oswM$debug.unitTimes[oswM$debug.unitTimes == ""] <- NA
# 
# 
# oswM$debug.unitTimesWide <- reshape(subset(na.omit(oswM$debug.unitTimes),length(oswM$debug.unitTimes$opp)>2), 
#                                      timevar = "opp",
#                                      idvar = c("db", "level", "message"),
#                                      direction = "wide")
# 
# if(nrow(oswM$debug.unitTimesWide)>0){
#   oswM$debug.unitTimesWide$time.start <- as.POSIXct(oswM$debug.unitTimesWide$time.start, format = "%Y-%m-%d %H:%M:%S",tz="UTC")
#   oswM$debug.unitTimesWide$time.end <- as.POSIXct(oswM$debug.unitTimesWide$time.end, format = "%Y-%m-%d %H:%M:%S",tz="UTC")
#   oswM$debug.unitTimesWide$duration <- difftime(oswM$debug.unitTimesWide$time.end , oswM$debug.unitTimesWide$time.start , unit="secs")
#   print(head(oswM$debug.unitTimesWide,30))
# }

#save(oswM,file="oswM.Rda")

print(head(oswMdata$DT_VMSTAT,n=30))
print(head(oswMdata$dataFiles))
#save(oswM,file="oswM-DT_VMSTAT.Rda")
save(oswMdata,file="oswVMSTATdata.Rda")
print(unique(oswMdata$DT_VMSTAT$name))











