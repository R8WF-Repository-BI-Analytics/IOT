# R SCript devant être Schedulé toutes les 15minutes.
# Il doit permettre de Loader la Table FF_MACHINE_STATE_FF de la Base APC_DB (Oracle).
# afin de stocker pour chaque Chambre: les Informations Suivantes:
# FF_CLEAN_TIME09K / FF_CLEAN_TIME1-5K /FF_CLEAN_TIME2K /FF_CLEAN_TIME2-5K /
# FF_CleanTHICK_USED / FF_CLEAN_COUNT

# **************** Liste des Function *****************************

# -1/ ------ Function de Validation OverETch ----------
F_OverEtchValid=function(OEtchValue,ratio)
{
  if ((OEtchValue>(ratio-0.2)) & (OEtchValue<(ratio+0.15))) 
  {return("TRUE")} else {return("FALSE")}
}
#  --1/----- Fin de la Fonction -----------


# -2/ --------Fonction de recuperation des Datas Brutes -------------

# Creation de la Mega Requete de selection ------------------
F_RecupeDataBRUTES=function(ENTITY,MaxPreviousRuns)
{
  MainFrame <-substr(ENTITY, 1, 5)
  MyRecipe<-"CLS"
  ClusterID<-substr(ENTITY,6,6)
  suffixe<-".oper.rou.st.com"
  Host_ID<-paste(MainFrame,suffixe,sep="")
  # ---------
 
  #  Pour une connection sur Base MySQL avec RMySQL
  mydb = dbConnect(MySQL(), user='root', password='sbdb', dbname='pcb', host=Host_ID)
  
  ch1="SELECT DISTINCT ch.StartEventTime as DateTime, ch.STRATEGYNAME, ch.MODULEID as Chamber, ch.RecipeID as Recipe,"
  ch2=" ch.strategyid, indic.Label as Indicator, tdh.Value, ch.dcqualityvalue as DCQV "
  ch3="FROM contexthistory ch, treateddatahistory tdh, indicator indic WHERE "
  ch4=paste("ch.MODULEID=","'",ClusterID,"'",sep="")     # filtre sur la chambre en question
  ch5=" and ch.StartEventTime > DATE_FORMAT(date_add(now(),interval - 2000 minute),'%Y%m%d%H%i%s') and upper(indic.Label) like '%DURATION%' "
  ch6=paste("and ch.RecipeID like '",MyRecipe,"%' and ch.STRATEGYNAME LIKE 'CL_%' AND ch.ContextID=tdh.ContextID and tdh.IndicatorID=indic.IndicatorID ",sep="")
  ch61="and indic.strategyid=ch.strategyid  and ch.dcqualityvalue>0 order by DateTime desc limit "
  ch7=paste(ch61,MaxPreviousRuns*2,sep="")   # *2 car 2 indicateurs (D1 & D2) par Run de Clean
  
  SQL_RecupDATA=paste(ch1,ch2,ch3,ch4,ch5,ch6,ch7,sep="")
  # --------------------FIN de la MEGA  REQUETE  ----------------------------------------
  
  rs = dbSendQuery(mydb,SQL_RecupDATA)
  
  resultat = fetch(rs, n=-1)  # Fetch pour rappatrier les Datas
  # This saves the results of the query as a data frame object. The n in the function specifies the number of records to retrieve, using n=-1 retrieves all pending records. 
  NumRecord<-length(resultat$Chamber)
  TOOL<-rep(MainFrame,NumRecord)
  dbDisconnect(mydb)
   return(cbind(resultat,TOOL))
  
} # Fin de ma Fonction F_RecupereDataBrutes --------------
# -2/ --------Fin de la Fonction  -------------

# - 3 -------- Fonction qui retourne l'épaisseur d'une recette de Clean ---------
F_ThickRecipe=function(maRecette)
{if (maRecette=="CLS USG2.5K 540C") {return(2500)}
  if (maRecette=="CLS USG2.5K") {return(2500)}
  if (maRecette=="CLS USG2K 540C") {return(2000)}
  if (maRecette=="CLS USG2K") {return(2000)}
  if (maRecette=="CLS USG1.5K 540C") {return(1500)}
  if (maRecette=="CLS USG1.5K") {return(1500)}
  if (maRecette=="CLS USG1.1K 540C") {return(1100)}
  if (maRecette=="CLS USG1.1K") {return(1100)}
  if (maRecette=="CLS USG0.9K 540C") {return(900)}
  if (maRecette=="CLS USG0.9K") {return(900)}
  if (maRecette=="CLS USG0.5K 540C") {return(500)}
  if (maRecette=="CLS USG0.5K") {return(500)}
  else return(0)
  # On ne liste ici que les noms des recettes génériques (sans l'extension -LA ou -LB)
} # ----------- Fin de la fonction F_ThickRecipe ------------


#  - 4/ --------------- F_CalculInfo_OptimumTime ------------------------
# Cette Fonction permet de retourner un vecteur avec 4 valeurs qui sont dans l'ordre:
#     OptimumTime / %OverETch / Nombre de CleanUsed For Calculation / Thickness

F_CalculInfo_OptimumTime=function(mesDATACHAMBRE)
{
  # -------------- Recherche de la recette ayant le max de clean sur les derniers MaxPreviousRun to Search-----------------
  VecRecipe<-(mesDATACHAMBRE$GenericRecipe) 
  # NB: Ma Recette selection est Une recette Sans l'extension -LA ou -LB ==> Elle est générique
  res<-table(VecRecipe)# retourne une Table avec la nombre Clean par recette
  #VecRecipe
  #CLS USG2.5K 540C   CLS USG2K 540C 
  #           4               36 
  
  matrec<-as.matrix(res) # mise sous forme d'une matrice    > matrec
  #    [,1]
  #CLS USG2.5K 540C    4
  #CLS USG2K 540C     36
  
  C1<-matrec[,1] # extraction de la Première colonne pour la mise sous forme d'un Vecteur  
  VecListRecipe<-names(C1[C1==max(res)]) # retourne 1 vecteur contenant la ou les recette(s) ayant effectué le maximum de Clean
  ma_Recette_Selection<-VecListRecipe[1] # recupere le premier nom de Recette ayant eu le Maximum de clean
  
  # ----------------- Fin de la Selection de la Recette ------------------------
  
  mesDATARECETTE= mesDATACHAMBRE[mesDATACHAMBRE[,"GenericRecipe"]==ma_Recette_Selection, ,drop=FALSE]
  
  
  #   Identification de la  Liste des DateTime ------------
  VecListeDateTime=levels(factor(mesDATARECETTE$DateTime)) # factor dÃ©finit la Colonne2 comme Ã©tant de Type Factor
  # levels affiche la liste exostive des differentes Chambres
  NbrDateTime=length(VecListeDateTime) # compte le nombre de Date distinctes de la chambre                                                  
  # ---------Exemple je prends le premier Run de Clean
  #          monRun=head(mesDATACHAMBRE,2)                                           
  # Attention il faudra mettre une boucle
  
  NbrCleanGood=1
  # Traitement pour chacune des VecListeDateTime
  
  for (j in 1:NbrDateTime)      #Lancement d'une Boucle for Pour le traitement de Chaque POC -------------
  {
    monRun= mesDATARECETTE[mesDATARECETTE[,"DateTime"]==VecListeDateTime[j], ,drop=FALSE]
    
    #   monRun= mesDATACHAMBRE[mesDATACHAMBRE[,"DateTime"]=="2015021015265426", ,drop=FALSE]
    # Il faut vÃ©rifier que j'ai bien le couple OVERETCH et Duration
    NbrIndicators= length(levels(factor(monRun$Indicator)))  # doit Ãªtre egale Ã  2 sinon il faut sortir
    # Fin du Traitement si  NbrIndicator =2
    
    if (NbrIndicators>1){
    
    # ----------traitement pour un Clean -------------                                                           
    # Collecte du Context du Run ---------
    DateTime= monRun[1,"DateTime"]
    test= as.matrix(monRun)
    maChambre=test[1,"Chamber"]
    maRecipe= test[1,"GenericRecipe"]
    monTOOL=test[1,"TOOL"]
   
    # RecupÃ©ration du Temps Total du Clean (Step1 et Step2)
    TotalCLEAN= monRun[monRun[,"Indicator"]=="Duration(CLEAN STEP)", ,drop=FALSE] 
    # Drop=FALSE permet de Garder le Format matrice et pas Vecteur
    
    # RecupÃ©ration du Temps D'OverEtch du Clean
    OverEtchTime= monRun[monRun[,"Indicator"]=="Duration(OVERETCH)", ,drop=FALSE]  # Drop=FALSE permet de Garder le Format matrice et pas Vecteur
    
    # Attribution du Ratio D'overETch By Tool  ( NB:Par Defaut c'est 40%)
    Ratio<- switch(substr(monTOOL,1,5), "DBP01" = 0.25, "DBP03" = 0.25, "DBP05"=0.25, 0.25) 
    #DBP05 est particuliere il faut Viser que 25% ( car RPS) et pas DBP01/03
   
    # Realisation des Calculs
    
    TauxOverEtch= round(OverEtchTime$Value[1]/TotalCLEAN$Value[1],3) # Calcul le % d'OverEtch du Run
    if (F_OverEtchValid(TauxOverEtch,Ratio)==TRUE)  
    {
      OffsetTime=round(((Ratio*TotalCLEAN$Value[1]-OverEtchTime$Value[1])/(1-Ratio)),3)   # Calcul l'Offset de temps Ã  corriger sur Clean Suivant
      
      CurrentTime= round((TotalCLEAN$Value[1]-15),3) # Calcul le Temps rÃ©el du Run 
      NewTime= CurrentTime+ OffsetTime   # Calcul le nouveau Temps pour le Run Suivant
      
      # AggrÃ©gation des rÃ©sultats
      vecResult=cbind(DateTime, maChambre, maRecipe, TauxOverEtch,CurrentTime,OffsetTime,NewTime)
      
      if (NbrCleanGood==1)
      {
        maMatriceOUT<-vecResult
        NbrCleanGood=2
      }
      else {
        maMatriceOUT<-rbind(maMatriceOUT,vecResult)
           }
      
    }
    }  # fin du cas si NbrIndicateur =2
    # ------------------------
  } # fin de la boucle j  --> On passe a la dateTime suivante
  
  # ------- Il faut Maintenant Pousser la Matrice de SynthÃ¨se vers un Fichier
  #                    write.csv(maMatriceOUT,"V:/Trash/denis/POC/sortieData.csv")
  #                   # write.csv(maMatriceOUT,"D:/DONNEES/CMP_COPPER_R/Rout.csv")
  
  ################  FIN DU FICHIER SCRIPT #######################         
  NewTimeAVG_ByRecipe<-tapply(as.numeric(maMatriceOUT[,7]),maMatriceOUT[,3],mean) # Nouveau Temps Moyen By recipe
  AVG_ByRecipe<-tapply(as.numeric(maMatriceOUT[,4]),maMatriceOUT[,3],mean) # Taux d'overEtch by Recipe
  CountByRecipe<-tapply(as.numeric(maMatriceOUT[,4]),maMatriceOUT[,3],length) # compte le nombre de Clean utilisÃ©s pour le calcul
  
  
  # On cherche à retourner l'épaisseur correspondante à la recette de clean utilisée.  --> function SWITH
  CleanThick<- round(F_ThickRecipe(ma_Recette_Selection),0)
  
  # OptimumTime / %OverETch / Nombre de CleanUsed For Calculation / Thickness
  MyVector<-c(round(NewTimeAVG_ByRecipe[[1]],3),round(AVG_ByRecipe[[1]],1),round(CountByRecipe[[1]],0),CleanThick)
  
} # Fin de la fonction qui retourne un Vecteur de l'info Recette Used 



# - 6 --------- F_Calcul_recettesFilles ---------------
# Fonction permettant de sortir le Temps Optimum pour chacune des recettes
# On rentre avec le Temps et L'épaisseur de la recette Optimum
F_Calcul_recettesFilles=function(VecOptimum)
{
  ListCleanRecipe<-c("CLS USG0.5K 540C","CLS USG0.9K 540C","CLS USG1.1K 540C","CLS USG1.5K 540C","CLS USG2K 540C","CLS USG2.5K 540C" )
  # Cette liste ne doit jamais être modifiée car c'est le nom des colonnes qui seront chargées dans la Base APC_DB
  # cette liste doit être Updaté si nouveau nom de recette
  
  # Initialisation du Vecteur sor                
  sor<-rep(0,length(ListCleanRecipe))
  
  for (i in 1:length(ListCleanRecipe)) 
  { # Formule Qui calcul le Temps Optimum ex: [(900/2500)*(TempsOpti2500 +15)-15]
    sor[i]=round((F_ThickRecipe(ListCleanRecipe[i])/VecOptimum[4])*(VecOptimum[1]),2)  
  }
  names(sor)<-ListCleanRecipe
  return(sor)
  # On resort avec ça
  # CLS USG0.9K 540C CLS USG1.5K 540C   CLS USG2K 540C CLS USG2.5K 540C 
  # 24.61            51.01            73.02            95.02 
}

# - 7 --------- F_Purge_LA_LB ---------------
# #Fonction permettant d'ajouter une Colonne nommée:"GenericRecipe"  a mesData'
# cetet Colonne Contient la recette en recette Generic (sans l'extension -LA/-LB)
F_Purge_LA_LB=function(mesData)
{
  Vecteur<-mesData$Recipe
  #Vecteur<-mesData
  NBRClean<-length(Vecteur)
  GenericRecipe<-rep(NA,NBRClean)# initialisation 
  for (i in 1:NBRClean)
  {
    Longueur=nchar(Vecteur[i])
    if ((substr(Vecteur[i],Longueur-2,Longueur)=="-LA") || (substr(Vecteur[i],Longueur-2,Longueur)=="-LB"))  # soit -LA soit -LB
    {GenericRecipe[i]<-substr(Vecteur[i],1,Longueur-3)} else {GenericRecipe[i]<-Vecteur[i]}
  }
  return(cbind(mesData,GenericRecipe))
}# Fin de ma Fonction purge -LA/-LB


# $$$$$$$$$$$$$$$$$$$ FIN DE MES FONCTIONS $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$


 # =================  DEBUT DU PROGRAMME -====================================

# Chargement des Library
library(RJDBC)
library(RODBC)
library(DBI)
library(rJava)    
library(RMySQL)
# ----------------

# Connexion sur Base APC_DB_TEST de Integration 
#drv<-JDBC("oracle.jdbc.driver.OracleDriver", "C:/Oracle/product/11.2.0/client_1/jdbc/lib/ojdbc6.jar") # Chargement du driver de mon PC
#connAPC<-dbConnect(drv, "jdbc:oracle:thin:@orahig01i:1530:adventa","apcadm","admapc")  # Creation d'une Connexion Intégration

# Connexion sur Base de PRODUCTION 
drv<-JDBC("oracle.jdbc.driver.OracleDriver", "C:/Program Files/R/Oracle-JDBC/ojdbc6.jar") # Chargement du driver Server rsn543 (R2.15.3)
connAPC<-dbConnect(drv, "jdbc:oracle:thin:@orahig01:1538:AUTOMDB","apcadm","admapc")  # Creation d'une Connexion Prod

# ------------- Definition des Chambres ------------------
ListCHAMBERS<-c("DBP01A","DBP01B","DBP01C","DBP01D","DBP03A","DBP03B","DBP03C","DBP03D","DBP05A","DBP05B","DBP05C","DBP05D")
NbrChambers<-length(ListCHAMBERS)
MaxPreviousRuns <-20  # paramètrage du nombre de clean Max à rechercher 
chaine1="UPDATE MACHINE_STATE_FF set FEEDFORWARD_VALUE="

for (i in 1:NbrChambers)
 {
       ENTITY<-ListCHAMBERS[i]
     
      # Je me connecte à l'autoBox de l'Entity en question pour sortir avec l'historique des Cleans
     JeuDataBrutes<-F_RecupeDataBRUTES(ENTITY,MaxPreviousRuns) # je récupere le jeu Brute
     JeuDataBrutes<-subset(JeuDataBrutes,Indicator=='Duration(CLEAN STEP)'|Indicator=='Duration(OVERETCH)')
     # Test pour savoir si j'ai des Datas
     NumberRow<-length(JeuDataBrutes$Recipe)
     if(NumberRow>0)
     {
     # J'ajoute une Colonne GenericRecipe à mon JeuDataBrutes (pour Virer -LA/-LB)
     JeuDataPurge<-F_Purge_LA_LB(JeuDataBrutes)
      # Je cherche maintenant à trouver la recette la Plus utilisée, calculer son temps Optimum 
         MonVec_Optimum<-F_CalculInfo_OptimumTime(JeuDataPurge) # ex: 56.7 Sec/41.2% / 18CleansUsed / 2000A 
       #[1]   73.017    0.420   30.000 2000.000   ==> On a uniquement une ligne de vecteur 
     
       # Je cherche maintenant à calculer le temps Optimums des Autres recettes.
        Vec_TimeFilles<-F_Calcul_recettesFilles(MonVec_Optimum)
    # ça donne le resultat ci-dessous
        # CLS USG0.9K 540C CLS USG1.5K 540C   CLS USG2K 540C CLS USG2.5K 540C 
      # 24.61            51.01            73.02            95.02 
    
        # Il faut Maintenant injecter ces valeurs dans la Table de l'APC_DB MACHINE_STATE_FF
        # Creation des 4 chaines de Caractère  UPDATE
      ch_Time05K = paste(chaine1, Vec_TimeFilles["CLS USG0.5K 540C"],", UPDATE_TIME=SYSDATE where MACHINE='",ENTITY,"' AND FEEDFORWARD_NAME='FF_CLEAN_TIME0-5K'",sep="")
      ch_Time09K = paste(chaine1, Vec_TimeFilles["CLS USG0.9K 540C"],", UPDATE_TIME=SYSDATE where MACHINE='",ENTITY,"' AND FEEDFORWARD_NAME='FF_CLEAN_TIME0-9K'",sep="")
      ch_Time11K = paste(chaine1, Vec_TimeFilles["CLS USG1.1K 540C"],", UPDATE_TIME=SYSDATE where MACHINE='",ENTITY,"' AND FEEDFORWARD_NAME='FF_CLEAN_TIME1-1K'",sep="")
      ch_Time15K = paste(chaine1, Vec_TimeFilles["CLS USG1.5K 540C"],", UPDATE_TIME=SYSDATE where MACHINE='",ENTITY,"' AND FEEDFORWARD_NAME='FF_CLEAN_TIME1-5K'",sep="")
      ch_Time2K = paste(chaine1, Vec_TimeFilles["CLS USG2K 540C"],", UPDATE_TIME=SYSDATE where MACHINE='",ENTITY,"' AND FEEDFORWARD_NAME='FF_CLEAN_TIME2K'",sep="")
      ch_Time25K = paste(chaine1, Vec_TimeFilles["CLS USG2.5K 540C"],", UPDATE_TIME=SYSDATE where MACHINE='",ENTITY,"' AND FEEDFORWARD_NAME='FF_CLEAN_TIME2-5K'",sep="")
      ch_Thick = paste(chaine1, round(MonVec_Optimum[4],0),", UPDATE_TIME=SYSDATE where MACHINE='",ENTITY,"' AND FEEDFORWARD_NAME='FF_CLEAN_THICK'",sep="")
      ch_COUNT = paste(chaine1, round(MonVec_Optimum[3],0),", UPDATE_TIME=SYSDATE where MACHINE='",ENTITY,"' AND FEEDFORWARD_NAME='FF_CLEAN_COUNT'",sep="")
       # lancement de 'Update  dans la Base APC_DB de PROD -----------------------
        dbSendUpdate(connAPC, ch_Time05K)
        dbSendUpdate(connAPC, ch_Time09K)
        dbSendUpdate(connAPC, ch_Time11K)
        dbSendUpdate(connAPC, ch_Time15K)
        dbSendUpdate(connAPC, ch_Time2K)
        dbSendUpdate(connAPC, ch_Time25K)
        dbSendUpdate(connAPC, ch_Thick)
        dbSendUpdate(connAPC, ch_COUNT)
     } # Fin du Test si j'ai des records (NumberRow>0)
     
     } # Fin de la Boucle sur chaque Chambre     
  dbDisconnect(connAPC) # Deconnection   de la Base APC_DB 

#     =================  FIN  DU PROGRAMME -====================================



