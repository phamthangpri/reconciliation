import datetime as dt
from  itertools import combinations
import pandas as pd
import sqlite3
from functions.utils import *
from functions.check_clientops import *

def full_rapprochement_prelvement(df_rejets_prlv,df_rejets_BO,df_rejets_PEI,df_rejets_clientops,df_rejets_clientops_PEI):
    ######## filtrer sur les rejets restants dans ClientOps pour valider #############
    df_rejets_prlv_a_traiter = df_rejets_prlv
    df_rejets_ClientOps_a_traiter = df_rejets_clientops
    df_rejets_ClientOps_all = pd.concat([df_rejets_ClientOps_a_traiter,df_rejets_clientops_PEI])
    df_rejets_ClientOps_all["id_ord_unique"] = df_rejets_ClientOps_all["Réf. de bout en bout"]  + " | " + df_rejets_ClientOps_all["Réf. mandat"]

    ### Regrouper les rejets de meme date
    df_rejets_ClientOps_agg = df_rejets_ClientOps_all.groupby(by=["Date de rejet"]).agg({
                                                                "Montant":"sum",
                                                                "Fonds_rejet":"count",
                                                                "id_ord_unique":";".join}).reset_index().rename(columns=
                                                                {"Fonds_rejet":"nb_ord_rejets"})
    df_rejets_prlv["Id sys"] = df_rejets_prlv["Id sys"].astype(str)
    df_rejets_prlv_agg = df_rejets_prlv_a_traiter.groupby(by="Valeur").agg({"Débit":"sum",
                                                                           "Id sys":";".join,
                                                                           "Nb_ordres":"sum"}).reset_index().rename(columns=
                                                                            {"Nb_ordres":"nb_ord_rejets"})
    ### Rapprocher le montant agrégé exact (tous les ordres de chaque date) ###
    df_match1 = df_rejets_ClientOps_agg.merge(df_rejets_prlv_agg,left_on=["Date de rejet","nb_ord_rejets"],
                                 right_on=["Valeur","nb_ord_rejets"])
    df_match1 = df_match1[abs(df_match1["Montant"]-df_match1["Débit"])<=0.01]
    df_match1 = df_match1.drop_duplicates(subset="Valeur",keep=False)
    df_match1 = df_match1.drop_duplicates(subset="Date de rejet",keep=False)

    #### Rapprocher les ordres unitaires 
    df_rejets_clientops_restant = df_rejets_ClientOps_all[~df_rejets_ClientOps_all["Date de rejet"].isin(df_match1["Date de rejet"])]
    df_rejets_prlv_restant = df_rejets_prlv[~df_rejets_prlv["Id sys"].isin(df_match1["Id sys"])]
    df_rejets_prlv_restant = df_rejets_prlv_restant[df_rejets_prlv_restant["Nb_ordres"]==1]
    df_match2 = df_rejets_clientops_restant.merge(df_rejets_prlv_restant,left_on=["Date de rejet"],
                                 right_on=["Valeur"])
    df_match2 = df_match2[abs(df_match2["Montant"]-df_match2["Débit"])<=0.01]
    df_match2 = df_match2.drop_duplicates(subset="Valeur",keep=False)
    df_match2 = df_match2.drop_duplicates(subset="Date de rejet",keep=False)[["Date de rejet","Valeur","id_ord_unique","Id sys"]]


    ### Ordres trouvés dans ClientOps ###
    df_rejets_ClientOps_trouve1 = df_rejets_ClientOps_all.merge(df_match1[["Date de rejet","Id sys"]], on = "Date de rejet")
    df_rejets_ClientOps_trouve2 = df_rejets_ClientOps_all.merge(df_match2[["Date de rejet","id_ord_unique","Id sys"]], on = ["Date de rejet","id_ord_unique"])
    df_rejets_ClientOps_trouve = pd.concat([df_rejets_ClientOps_trouve1,df_rejets_ClientOps_trouve2])
    # df_rejets_ClientOps_trouve = df_rejets_ClientOps_trouve1

    ### Ordres trouvés dans relevé  ###
    df_rejets_prlv_audit1 = df_rejets_prlv.merge(df_match1[["Valeur","id_ord_unique"]],on="Valeur") ## montant agrégé
    df_rejets_prlv_audit2 = df_rejets_prlv.merge(df_match2[["Valeur","Id sys","id_ord_unique"]],on=["Valeur","Id sys"]) ## montant unitaire
    df_rejets_prlv_audit = pd.concat([df_rejets_prlv_audit1,df_rejets_prlv_audit2])
    # df_rejets_prlv_audit = df_rejets_prlv_audit1
    df_rejets_prlv_audit["Statut"] = "Audit clientops_montant agrégé exact"

    #### ### Retrouver les ordres rapprochés ####
    df_rejets_SUB_audit = df_rejets_BO.merge(df_rejets_ClientOps_trouve[["Réf. mandat","Réf. de bout en bout","Date de rejet","Id sys"]],
                                               left_on=["order_name","ord_prlv"],
                                                right_on = ["Réf. mandat","Réf. de bout en bout"])
    df_rejets_SUB_audit["Statut"] = "Audit clientops_montant agrégé exact"

    df_rejets_PEI_audit = df_rejets_PEI.merge(df_rejets_ClientOps_trouve[["Réf. mandat","Réf. de bout en bout","Date de rejet","Id sys"]],
                                              left_on=["MandateName","ord_prlv"],
                                            right_on = ["Réf. mandat","Réf. de bout en bout"])
    df_rejets_PEI_audit["Statut"] = "Audit clientops_montant agrégé exact"

    df_rejets_BO_audit = pd.concat([df_rejets_SUB_audit,df_rejets_PEI_audit])

    #### Ordres rejetés et virements trouvés dans relevé mais non trouvés dans rejets BO ### (ordres non annulés le jour de rejet)
    df_ordres_non_annules = df_rejets_ClientOps_trouve[~df_rejets_ClientOps_trouve["id_ord_unique"].isin(df_rejets_BO_audit["id_ord_unique"])]

    ######## Restant à faire ### 
    df_rejets_SUB_restant = df_rejets_BO[~df_rejets_BO["id_ord_unique"].isin(df_rejets_SUB_audit["id_ord_unique"])]
    df_rejets_PEI_restant = df_rejets_PEI[~df_rejets_PEI["id_ord_unique"].isin(df_rejets_PEI_audit["id_ord_unique"])]
    df_rejets_prlv_restant = df_rejets_prlv[~df_rejets_prlv["Id sys"].isin(df_rejets_prlv_audit["Id sys"])]


    ########## Rapprochement automatique ###########
    df_rejets_BO_a_traiter = pd.concat([df_rejets_SUB_restant,df_rejets_PEI_restant])
    df_rejets_prlv_restant,df_rejets_BO_a_traiter = rapprochement_auto_prelevement(df_rejets_prlv_restant,df_rejets_BO_a_traiter)

    df_rejets_BO_rapproch_auto = df_rejets_BO_a_traiter[~df_rejets_BO_a_traiter["Id sys"].isnull()]
    df_rejets_SUB_rapproch_auto = df_rejets_BO_rapproch_auto[df_rejets_BO_rapproch_auto["Idmove"].isnull()][df_rejets_SUB_restant.columns]
    df_rejets_PEI_rapproch_auto = df_rejets_BO_rapproch_auto[~df_rejets_BO_rapproch_auto["Idmove"].isnull()][df_rejets_PEI_restant.columns]
    df_rejets_prlv_rapproch_auto = df_rejets_prlv_restant[~df_rejets_prlv_restant["id_ord_unique"].isnull()]
    
    ### Overall
    df_rejets_SUB_trouves = pd.concat([df_rejets_SUB_audit,df_rejets_SUB_rapproch_auto])
    df_rejets_PEI_trouves = pd.concat([df_rejets_PEI_audit,df_rejets_PEI_rapproch_auto])
    df_rejets_BO_trouves = pd.concat([df_rejets_SUB_trouves,df_rejets_PEI_trouves])
    df_rejets_prlv_trouves = pd.concat([df_rejets_prlv_audit,df_rejets_prlv_rapproch_auto])
    
    
    df_rejets_prlv_trouves_partiel  = df_rejets_prlv_trouves[df_rejets_prlv_trouves["Statut"]=="1 rejet pls ordres_rapprochement partiel"]
    df_rejets_prlv_trouves_partiel = df_rejets_prlv_trouves_partiel.merge(df_rejets_BO_trouves[["id_ord_unique","amount"]],on="id_ord_unique")
    df_rejets_prlv_trouves_partiel["Ecart_restant"] = df_rejets_prlv_trouves_partiel["Débit"] - df_rejets_prlv_trouves_partiel["amount"]
    df_rejets_prlv_trouves_partiel["Nb_ordres_trouvés"] = 1
    df_rejets_prlv_trouves_partiel["Nb_ordres_restant"] = df_rejets_prlv_trouves_partiel["Nb_ordres"] - 1
    df_rejets_prlv_trouves_exact = df_rejets_prlv_trouves[df_rejets_prlv_trouves["Statut"]!="1 rejet pls ordres_rapprochement partiel"]
    df_rejets_prlv_trouves_exact["Nb_ordres_trouvés"] = df_rejets_prlv_trouves_exact["Nb_ordres"]
    df_rejets_prlv_trouves = pd.concat([df_rejets_prlv_trouves_exact,df_rejets_prlv_trouves_partiel])
    
    return df_rejets_SUB_trouves,df_rejets_PEI_trouves,df_rejets_prlv_trouves,df_rejets_ClientOps_trouve,df_ordres_non_annules

def rapprochement_auto_prelevement(df_rejets_prlv,df_rejets_BO,bol_rapproch_partiel=True):
    df_rejets_prlv_od_unique = df_rejets_prlv[df_rejets_prlv["Nb_ordres"]==1]
    df_rejets_prlv_pls_od = df_rejets_prlv[df_rejets_prlv["Nb_ordres"]!=1]
    for nb_days in np.arange(2,60,step=20):
        nb_days = int(nb_days)

        ### Rapprocher les rejets uniques - montant exact ####
        df_rejets_prlv_od_unique,df_rejets_BO = match_releve_rejets(df_rejets_prlv_od_unique,df_rejets_BO,nb_days)
        ### Rapprocher les rejets pour plusieurs ordres - montant exact ####
        list_nb_ordres = list(df_rejets_prlv_pls_od["Nb_ordres"].unique())
        for nb_ord in list_nb_ordres:
            ## Meme session
            df_rejets_prlv_pls_od,df_rejets_BO = rapprocher_pls_ord(df_rejets_prlv_pls_od,df_rejets_BO,nb_days,nb_ord,False,True)
            ## différentes sessions
            df_rejets_prlv_pls_od,df_rejets_BO = rapprocher_pls_ord(df_rejets_prlv_pls_od,df_rejets_BO,nb_days,nb_ord,False,False)
        #### Rapprochement partiel - 1 ordre
        df_rejets_prlv_pls_od,df_rejets_BO = match_releve_rejets(df_rejets_prlv_pls_od,df_rejets_BO,nb_days,True)

         #### Rapprochement partiel - plusieurs ordres
#         for nb_ord in np.arange(2,20):
#             # Meme session
#             df_rejets_prlv_pls_od,df_rejets_BO = rapprocher_pls_ord(df_rejets_prlv_pls_od,df_rejets_BO,nb_days,True,True)
#             # Diff session
#             df_rejets_prlv_pls_od,df_rejets_BO = rapprocher_pls_ord(df_rejets_prlv_pls_od,df_rejets_BO,nb_days,nb_ord,True,False)
            #### Concaténation ######
    df_rejets_prlv = pd.concat([df_rejets_prlv_od_unique,df_rejets_prlv_pls_od])
    return df_rejets_prlv,df_rejets_BO

def match_releve_rejets(df_rejets_prlv,df_rejets_BO,nb_days,bol_partiel = False):
    if "id_ord_unique" in df_rejets_prlv.columns:
        df_rejets_prlv_a_traiter = df_rejets_prlv[df_rejets_prlv["id_ord_unique"].isnull()].drop(columns="id_ord_unique")
    else : df_rejets_prlv_a_traiter = df_rejets_prlv
    
    if "Id sys" in df_rejets_BO.columns:
        df_rejets_BO_a_traiter = df_rejets_BO[df_rejets_BO["Id sys"].isnull()].drop(columns="Id sys")
    else : df_rejets_BO_a_traiter = df_rejets_BO
    
    df_rejets_BO_a_traiter["end_date"] = df_rejets_BO_a_traiter["date_session"] + dt.timedelta(days=nb_days)
    conn = sqlite3.connect(':memory:')
    df_rejets_prlv_a_traiter.to_sql('rejets_prlv', conn, index=False)
    df_rejets_BO_a_traiter.to_sql('rejets_bo', conn, index=False)
    if bol_partiel == False:
        qry = '''
            SELECT  *
            FROM
                rejets_prlv LEFT JOIN rejets_bo ON
                (ABS(rejets_prlv.Débit - rejets_bo.amount ) <= 0.01 )
                AND rejets_prlv.Nb_ordres = rejets_bo.nb_ord
                AND rejets_prlv.Fonds = rejets_bo.product
                AND (rejets_prlv.Valeur BETWEEN rejets_bo.date_session AND rejets_bo.end_date)

            '''
    else :
        qry = '''
            SELECT  *
            FROM
                rejets_prlv LEFT JOIN rejets_bo ON
                rejets_prlv.Débit > rejets_bo.amount 
                AND rejets_prlv.Nb_ordres > rejets_bo.nb_ord
                AND rejets_prlv.Fonds = rejets_bo.product
                AND (rejets_prlv.Valeur BETWEEN rejets_bo.date_session AND rejets_bo.end_date)

        '''
    df_match = pd.read_sql_query(qry, conn)
    df_match = df_match[~df_match["id_ord_unique"].isnull()]
    df_match = df_match.drop_duplicates(subset="Id sys",keep=False)
    df_match = df_match.drop_duplicates(subset="id_ord_unique",keep=False)[["Id sys","id_ord_unique"]]
    
    if bol_partiel:
        df_match["rapprochement_partiel"] = True
        statut = "1 rejet pls ordres_rapprochement partiel"
    else : statut = "Rejet unique_montant exact"

    df_match["Statut"] = statut
    df_rejets_prlv = df_rejets_prlv.set_index("Id sys").combine_first(df_match.set_index("Id sys")).reset_index()
    df_rejets_BO = df_rejets_BO.set_index("id_ord_unique").combine_first(df_match.set_index("id_ord_unique")[["Id sys","Statut"]]).reset_index()

    return df_rejets_prlv,df_rejets_BO

def rapprocher_pls_ord(df_rejets_prlv_recent_pls_od,df_rejets_BO,nb_days,nb_ord,bol_partiel=False,bol_meme_session=True):

    if "id_ord_unique" in df_rejets_prlv_recent_pls_od.columns:
        df_rejets_prlv_a_traiter = df_rejets_prlv_recent_pls_od[df_rejets_prlv_recent_pls_od["id_ord_unique"].isnull()].drop(columns="id_ord_unique")
    else : df_rejets_prlv_a_traiter = df_rejets_prlv_recent_pls_od
    
    if "Id sys" in df_rejets_BO.columns:
        df_rejets_BO_a_traiter = df_rejets_BO[df_rejets_BO["Id sys"].isnull()].drop(columns="Id sys")
    else : df_rejets_BO_a_traiter = df_rejets_BO

    df_rejets_prlv_nb_ord = df_rejets_prlv_a_traiter[df_rejets_prlv_a_traiter["Nb_ordres"]==nb_ord]
    min_valeur = df_rejets_prlv_a_traiter["Valeur"].min()
    max_valeur = df_rejets_prlv_a_traiter["Valeur"].max()
    min_valeur = min_valeur + dt.timedelta(days=-nb_days)

    df_rejets_BO_a_traiter = df_rejets_BO_a_traiter[df_rejets_BO_a_traiter["date_session"]<=max_valeur]
    df_rejets_BO_a_traiter = df_rejets_BO_a_traiter[df_rejets_BO_a_traiter["date_session"]>=min_valeur]


    if len(df_rejets_prlv_nb_ord) > 0 :
        if bol_meme_session :
            df_combinations = agreger_pls_ord_meme_session(df_rejets_BO_a_traiter,nb_ord)
        else : df_combinations = agreger_pls_ord_diff_sessions(df_rejets_BO_a_traiter,nb_ord,nb_days)

        ### Rapprocher avec le relevé
        df_rejets_prlv_a_traiter,df_combinations = match_releve_rejets(df_rejets_prlv_a_traiter,df_combinations,nb_days,bol_partiel)

        # ### Prendre uniquement le résultat unique
        if len(df_combinations) > 0 :
            statut = "1 rejet pls ordres"
            if bol_partiel : 
                if "rapprochement_partiel" in df_combinations.columns:
                    df_combinations = df_combinations[["id_ord_unique","Id sys","rapprochement_partiel"]]
                else : df_combinations = df_combinations[["id_ord_unique","Id sys"]]
                statut = statut + "_rapprochement partiel"
            else:  
                df_combinations = df_combinations[["id_ord_unique","Id sys"]]
                statut = statut + "_montant exact"
            df_combinations = df_combinations[~df_combinations["Id sys"].isnull()]
            df_combinations["Statut"] = statut
            df_rejets_prlv_recent_pls_od = df_rejets_prlv_recent_pls_od.set_index("Id sys").combine_first(df_combinations.set_index("Id sys")).reset_index()

            df_combinations["id_ord_unique"] = df_combinations["id_ord_unique"].str.split(";")
            df_combinations = df_combinations.explode('id_ord_unique')
            df_rejets_BO = df_rejets_BO.set_index("id_ord_unique").combine_first(df_combinations.set_index("id_ord_unique")[["Id sys","Statut"]]).reset_index()
    return df_rejets_prlv_recent_pls_od,df_rejets_BO

def agreger_pls_ord_meme_session(df_rejets_BO,nb_ord):
    df_combinations = pd.DataFrame()
    for ord_prlv in set(list(df_rejets_BO["ord_prlv"])): # créer uniquement des combinaisons pour la même session
        df_rejets_BO_session = df_rejets_BO[df_rejets_BO["ord_prlv"]==ord_prlv]
        dict_amount = dict(zip(list(df_rejets_BO_session["id_ord_unique"]),list(df_rejets_BO_session["amount"]))) # créer le dictionnaire {id_ord_unique : amount}
        list_combinations = list(combinations(df_rejets_BO_session["id_ord_unique"], nb_ord)) # toutes les combinaisons des ordres

        #### Créer des combinaisons
        list_combo = []
        list_amount = []
        for combo in list_combinations:
            sum_combo = 0
            for j in np.arange(0,len(combo)):
                sum_combo += dict_amount[combo[j]]
            combo = ";".join(combo)
            list_combo.append(combo)
            list_amount.append(sum_combo)
        dict_result = {"id_ord_unique":list_combo,
                        "amount" : list_amount,
                        "product":list(df_rejets_BO_session["product"])[0],
                        "date_session":list(df_rejets_BO_session["date_session"])[0],
                        "ord_prlv":[ord_prlv] * len(list_combinations),
                        "nb_ord" : [nb_ord] * len(list_combinations)
                        }
        df_combinations = pd.concat([df_combinations,pd.DataFrame.from_dict(dict_result)])
    return df_combinations

def agreger_pls_ord_diff_sessions(df_rejets_BO_a_traiter,nb_ord,nb_days):

    df_combinations = pd.DataFrame()
    ### Etape1 : Prendre les dates existantes et ajouter la date de fin
    df_date_session = df_rejets_BO_a_traiter[["date_session"]].drop_duplicates().sort_values(by="date_session") # prendre la liste des dates
    df_date_session["end_date"] = df_date_session["date_session"]+ dt.timedelta(days=nb_days) # end_date
    
    ### Etape 2 : pour chaque date, chercher les sessions dans l'intervalle date_session et end_date
    for index,rows in df_date_session.iterrows(): 
        df_rejets_BO_session = df_rejets_BO_a_traiter[df_rejets_BO_a_traiter["date_session"]>=rows["date_session"]]
        df_rejets_BO_session = df_rejets_BO_session[df_rejets_BO_session["date_session"]<=rows["end_date"]]
        
        list_sessions = set(list(df_rejets_BO_session["ord_prlv"]))

        if len(list_sessions) > 0:
            dict_amount = dict(zip(list(df_rejets_BO_session["id_ord_unique"]),list(df_rejets_BO_session["amount"]))) # créer le dictionnaire {id_ord_unique : amount}
            list_combinations = list(combinations(df_rejets_BO_session["id_ord_unique"], nb_ord)) # toutes les combinaisons des ordres 
            #### Créer des combinaisons
            list_combo = []
            list_amount = []
            for combo in list_combinations:
                sum_combo = 0
                for j in np.arange(0,len(combo)):
                    sum_combo += dict_amount[combo[j]]
                combo = ";".join(combo)
                list_combo.append(combo)
                list_amount.append(sum_combo)
            dict_result = {"id_ord_unique":list_combo,
                           "amount" : list_amount,
                           "product":list(df_rejets_BO_session["product"])[0],
                           "date_session":max(list(df_rejets_BO_session["date_session"])),
                           "ord_prlv":["pls ord"] * len(list_combinations),
                           "nb_ord" : [nb_ord] * len(list_combinations)
                          }
            df_combinations = pd.concat([df_combinations,pd.DataFrame.from_dict(dict_result)])
        # supprimer l'ordre déjà utilisé
        df_rejets_BO_a_traiter = df_rejets_BO_a_traiter[~df_rejets_BO_a_traiter["id_ord_unique"].isin(df_rejets_BO_session["id_ord_unique"])]
    df_combinations["nb_ord"] = df_combinations["nb_ord"].astype(int)
    # supprimer les doublons qui viennent des ordres ayant le même montant pour la meme session
    df_combinations = df_combinations.groupby(by=['amount','product','date_session','ord_prlv','nb_ord']).agg({"id_ord_unique":"first"}).reset_index()
    return df_combinations