list.of.packages <- c("futile.logger","ggplot2", "dplyr","gridExtra","scales","reshape","xtable","ggthemes","stringr","data.table","lubridate","gplots")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) {
  options(repos="http://cran.cnr.Berkeley.edu")
  install.packages(new.packages,dependencies = TRUE)  
}


lapply(list.of.packages, function(x) {
  library(x,character.only=TRUE,quietly = TRUE)
})



#median_vals <- ddply(oswMdata$DT_VMSTAT,.(name),summarise, cpu.busy=median(cpu.busy))
#mean_vals <- ddply(x.melt,.(variable),summarise,value=mean(value))
#quant_low <- ddply(x.melt,.(variable),summarise,value=quantile(value,0.25,na.rm=TRUE))
#quant_high <- ddply(x.melt,.(variable),summarise,value=quantile(value,0.75,na.rm=TRUE))


if(file.exists('oswVMSTATdata.Rda')){
  load(file='oswVMSTATdata.Rda')
}


myTheme <- theme_stata(scheme = "s2color") +
  #myTheme <- theme_few() +
  #myTheme <- theme_bw() +
  
  theme(legend.position =    "bottom",
        #plot.margin =        unit(c(3, 3, 3, 3), "lines"),
        axis.title.y = element_text(vjust = .6),
        text =               element_text(family="sans",face = "plain",
                                          colour = "black", size = 8,
                                          hjust = 0.5, vjust = 0.5, angle = 0, lineheight = 0.9),
        axis.text.y =       element_text(angle = 0),
        panel.margin =       unit(0.25, "lines"),
        panel.grid.major = element_line(colour="#dedede", size = 0.2,linetype = "dotted"),
        panel.grid.minor = element_line(colour="#dedede", size = 0.1,linetype = "dotted"),
        axis.text.x=element_text(angle=-30, hjust=-.1,vjust=1,size=6)
  )

theme_set(myTheme)


pdf("vmstat-plots.pdf", width = 11, height = 8.5,useDingbats=FALSE)

boxplot1 <- ggplot(data=oswMdata$DT_VMSTAT,aes(x="a",y=cpu.busy))
boxplot1 + geom_boxplot(colour="black") + 
  geom_point(alpha=0.1,position = position_jitter(width = .4,height=0),aes(),color="#aaaaaa")+
  #geom_text(data=median_vals,aes(y=cpu.busy,label=round(cpu.busy,1)),alpha=0.8,size=2,vjust=-0.8,hjust=0)+
  theme(
        axis.title.x  = element_blank(),
        axis.text.x  = element_blank()
        )+
  ylim(0,100)+
  facet_grid(. ~ name)


ggplot(data=oswMdata$DT_VMSTAT,aes(x=dateTime,y=cpu.busy))+
  geom_line(stat="identity")+
  facet_grid(name ~ .)


VMSTAT.melt <- melt(oswMdata$DT_VMSTAT, id.var = c("dateTime","name"), measure.var = c("cpu.user","cpu.sys"))
summary(VMSTAT.melt)
ggplot(data=VMSTAT.melt,aes(x=dateTime,y=value,fill=variable,color=variable))+
  geom_bar(stat="identity",position="stack",width=30)+
  facet_grid(name ~ .)

dev.off()
