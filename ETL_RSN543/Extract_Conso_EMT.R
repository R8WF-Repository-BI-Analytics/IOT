l_library("RMySQL")

NetAppPath<-"\\\\rsd0100.rou.st.com\\Spotfire\\input\\APC-Advanced Process Control (FDC)\\DATA.SUMMARY\\EMT\\"

TodayMinusN_First<-7
TodayMinusN_Last<-1

for (d in (TodayMinusN_Last:TodayMinusN_First))
{
  load_day=substr(as.character(Sys.Date()-d),1,10)
  
  Query=paste0(" SELECT  `DateTime`, `Port`, `Chamber`, `From_Row`, `To_Row`, `WaferFlow`, `Recipe`, `Lot`, `Slot`, `Step`, 
               `Step_Duration`, `RFHours`, `ElectrodeLifeTime`,`WaferAngle`,
               `MFC_01_BCl3`, `MFC_02_O2`, `MFC_03_Cl2`, `MFC_04_N2_20sccm`, `MFC_05_CHF3`, `MFC_06_Argon`, `MFC_07_N2_100sccm`, `MFC_08_SF6`, `MFC_Helium_Backside`, 
               `Power_Top`, `Power_Bottom`, 
               `Pressure_Chamber`, `Pressure_Foreline`, 
               `Temperature_Chiller`, `Temperature_Middle`, `Temperature_Bottom` 
               FROM `EtchData`.`ecoprint_de_lamal9600ptx` 
               WHERE substr(`DateTime`,1,10)='",load_day,"'") # and Chamber='",Chamber[c],"'")
  
  TestConnect <- try(dbConnect(RMySQL::MySQL(),dbname='EtchData',username='root',password='sbdb',host='localhost'),silent=TRUE)
    rs <- dbSendQuery(TestConnect,Query)
    Dataset_Day <- dbFetch(rs,n=-1)
  dbDisconnect(conn=TestConnect)
  
  save(Dataset_Day,file=paste0(NetAppPath,load_day,"_APC_Summary.RData"))
  write.csv(Dataset_Day,file=paste0(NetAppPath,load_day,"_APC_Summary.csv"), row.names = FALSE)
  
  if (d==1) { DataSet=Dataset_Day } else { DataSet=unique(rbind(Dataset_Day,Dataset)) }
}

Last_day=substr(as.character(Sys.Date()-TodayMinusN_Last),1,10)
First_day=substr(as.character(Sys.Date()-TodayMinusN_First),1,10)

save(Dataset,file=paste0(NetAppPath,"Period_",First_day,"_",Last_day,"_APC_Summary.RData"))
write.csv(Dataset,file=paste0(NetAppPath,"Period_",First_day,"_",Last_day,"_APC_Summary.csv"), row.names = FALSE)
