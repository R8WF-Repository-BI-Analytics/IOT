
#*************************************
#*************************************
#Ouverture d'une Base de Donnée ORACLE  avec "R"
#*************************************
#*************************************
#------------------------------
 	library(RJDBC)
 	library(RODBC)
  library(DBI)
  library(rJava)    # lancement des Librairies

   drv<-JDBC("oracle.jdbc.driver.OracleDriver", "C:/Program Files/R/Oracle-JDBC/ojdbc6.jar") # Chargement du driver

# Connexion sur Base ADV_HIST_PROD
 connhist<-dbConnect(drv, "jdbc:oracle:thin:@oralow13:1532:advhist","advhist","advhist")  # Creation d'une Connexion

# ---------------------- Connexion sur Base APC_DB de Production   ----------------------------------------------
# Cela sera en place le Mardi 16 Décembre 2014
  conn<-dbConnect(drv, "jdbc:oracle:thin:@orahig01:1538:AUTOMDB","apcadm","admapc")  # Creation d'une Connexion
# ---------------------------------------------------------------------------------------------------------------

# Creation de la Mega Requete de selection ------------------
ch1= "select gen.LOGINTIME as PROC_TIME, gen.MACHINE, oute.ACTUALVALUE as DEPRATE_TQ, inp.VALUE as RF_HOUR from T_RDB_GENERALBASIC gen, OUTPUTS oute, INPUTS inp where "
ch2="gen.CONTROLSTRATEGYDOCUMENTOBJID='2072343553' AND gen.MACHINE LIKE 'MSP%' AND gen.RUNCODE='Qualification' AND gen.PROCESSACTION='DEPOSITION' AND oute.RUNID=gen.RUNID "
ch3="AND inp.RUNID=gen.RUNID AND oute.IDENTIFIER='Y_DEPRATE' AND oute.DATATYPE='Number' AND inp.DATATYPE='Number' AND inp.INPUTTYPE='Feedforward' AND inp.IDENTIFIER='FF_CurrentValue_RF_Hour' "
ch4="AND oute.ACTUALVALUE is not NULL ORDER BY gen.LOGINTIME DESC"
SQL_RecupDATA=paste(ch1,ch2,ch3,ch4,sep="")
# -----------------------------------------------------------

# lancement de la Recupération de l'Historique All MSP All DepRate et All RF----------------------
  monHisto= dbGetQuery(connhist, SQL_RecupDATA) # ça mmarche mais ~90sec
                  #                PROC_TIME MACHINE DEPRATE_TQ RF_HOUR
                  #1   2013-10-18 07:20:10.0  MSP233   153.8579     664
                  #2   2013-10-17 14:08:14.0  MSP254   160.7283      24
                  #3   2013-10-17 14:08:14.0  MSP253   162.1518     560
                  #4   2013-10-17 07:22:41.0  MSP254   160.4088      17

 #   Identification de la  Liste des différentes Chambres ------------

     VecListeCHAMBRE=levels(factor(monHisto$MACHINE)) # factor définit la Colonne2 comme étant de Type Factor
                                                           # levels affiche la liste exostive des differentes Chambres

 # Nombre de Chambre différentes Referencées dans l'historique
       NbrCHAMBER=length(VecListeCHAMBRE) # compte le Nombre de Chambres differentes 11 MSP Chambers
      # VecListeCHAMBRE [1]   # "MSP153 Premier element de la Liste VecListeCHAMBRE
 # -------------------------------------------------------------------------------------------
 # -----------------BOUCLE FOR  --------------------------------------------------------------------------
  # -------------------------------------------------------------------------------------------

           for (i in 1:NbrCHAMBER)      #Lancement d'une Boucle for Pour le traitement de Chaque Chambre -------------
                 {
                             chambre= paste("'", VecListeCHAMBRE [i],"'",sep="") # "'MSP153'"




#  ------------------ Filtrage de la MATRICE globale pour extraire une SOUS MATRICE machine par machine


          SOUSLISTE= monHisto[monHisto[,"MACHINE"]==VecListeCHAMBRE[i], ,drop=FALSE]  # Drop=FALSE permet de Garder le Format matrice et pas Vecteur

         SOUSLISTE= head(SOUSLISTE,40)     # Ne garde que les 30 dernières Qualifs pour Chacune des Chambres
                           # c'est de Type Facteur!!!
                           
     #  SOUSMATRICE= data.frame(SOUSMATRICE)  # type Charactere

  # Calcule des coefficients du Polynome---------------
          fit=lm(DEPRATE_TQ~RF_HOUR + I(RF_HOUR^2)+ I(RF_HOUR^3),data=SOUSLISTE)  # Fit contient le polynome


  # ----------------------     en cours de Debug

# Attribution des Variables
                      COEF_RF0=fit$coefficients[1]   #   181.8465     # c'est doncl'intercept
                      COEF_RF1=fit$coefficients[2]   #0.06202771 # RF
                      COEF_RF2=fit$coefficients[3]   #-0.0002368645     I(RF^2)
                      COEF_RF3=fit$coefficients[4]   #1.484192e-07   I(RF^3)


# Creation des 4 chaines de Caractère  UPDATE
                   chaine1="UPDATE MACHINE_STATE_FF set FEEDFORWARD_VALUE="
                     chaine_RF0 = paste(chaine1,COEF_RF0,",UPDATE_TIME=SYSDATE where MACHINE=",chambre,"AND FEEDFORWARD_NAME='FF_POLYNOME_COEF_RF0'",sep=" ")
                     chaine_RF1 = paste(chaine1,COEF_RF1,",UPDATE_TIME=SYSDATE where MACHINE=",chambre,"AND FEEDFORWARD_NAME='FF_POLYNOME_COEF_RF1'",sep=" ")
                     chaine_RF2 = paste(chaine1,COEF_RF2,",UPDATE_TIME=SYSDATE where MACHINE=",chambre,"AND FEEDFORWARD_NAME='FF_POLYNOME_COEF_RF2'",sep=" ")
                     chaine_RF3 = paste(chaine1,COEF_RF3,",UPDATE_TIME=SYSDATE where MACHINE=",chambre,"AND FEEDFORWARD_NAME='FF_POLYNOME_COEF_RF3'",sep=" ")

       # lancement de 'Update  dans la Base APC_DB de PROD -----------------------
                                    reply<- dbSendQuery(conn, chaine_RF0)
                                    reply<-dbSendQuery(conn, chaine_RF1)
                                    reply<-dbSendQuery(conn, chaine_RF2)
                                    reply<-dbSendQuery(conn, chaine_RF3)
         # Validation Commit



                }  # Fin de la Boucle FOR, On Passe à la Chambre Suivante
  # ------------------------------------------------------------------------------------
  #  ------------------------------------------------------------------------------------
  #  ------------------------------------------------------------------------------------

           #      dbCommit(conn)
       dbDisconnect(conn) # Deconnection   de la Base APC_DB
       dbDisconnect(connhist) # Deconnection de la Base ADV_HIST_PROD

   ################  FIN DU FICHIER SCRIPT #######################
   
