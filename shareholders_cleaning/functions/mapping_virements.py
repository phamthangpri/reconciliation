import datetime as dt
from fuzzywuzzy import fuzz, process # Fuzzy match
from  itertools import product
import pandas as pd
import sqlite3
from functions.utils import *
from functions.check_clientops import *

def basic_mapping_releve_vs_BO(df_virements,df_BO_virs,colonne_mapping,moyen_mapping,bol_partial_score,bol_plusieurs_virs,bol_proposition=False):
    '''Cette fonction permet de rapprocher entre les données bancaires et les données de BO avec la règle basique :
    + Prendre les montants atour de 5€ de différence
    + Comparer les dates aux alentours de 30jours
    + Comparer les noms entre les 2 bases avec un score de fuzzy >= 90

    df_virements : dataframe des lignes de virements
    df_BO_virs : dataframe des ordres du BO ayant le mode de paiement = virement
    colonne_mapping : colonne du nom de client dans le relevé qui est utilisé pour le rapprochement (parmi les Titulaire_clean, Motif1, Motif2)
    moyen_mapping (str): la façon dont le rapprochement est utilisé. C'est le commentaire qu'on veut ajouter pour savoir l'étape du rapprochement
    bol_partial_score (bolean): utiliser le partial_score ou non pour le fuzzy matching
    bol_plusieurs_virs : le cas de plusieurs paiements pour le même ordre ?
    bol_proposition : utiliser pour des propositions ? (le fuzzy score >= 70 au lieu de 90)

    En sortant, si un virement est rapproché avec un ordre, l'Id sys est rempli dans le BO et le Subscription_Order_Name est rempli dans la table des virements
    '''
    
    #Etape 1 : filtrer sur les lignes qui ne sont pas encore mappées et prendre seulement des colonnes nécessaires
    
    if "Subscription_Order_Name" in df_virements.columns: # si la table est déjà mappée
        df_virements_a_traiter = df_virements[df_virements["Subscription_Order_Name"].isnull()]
    else :
            df_virements_a_traiter = df_virements
    
    if "Id sys" in df_BO_virs.columns: # si la table est déjà mappée
        df_BO_a_traiter = df_BO_virs[df_BO_virs["Id sys"].isnull()].drop(columns="Id sys")
    else :
            df_BO_a_traiter = df_BO_virs
    columns_vir = ['Id sys', 'Valeur', 'Crédit', colonne_mapping]
    if bol_plusieurs_virs:
        columns_vir = columns_vir + ["Crédit_total","Max_Valeur"]
    else :columns_vir = columns_vir +  ['Product_motif']
    df_virements_a_traiter = df_virements_a_traiter[columns_vir]
    
    #Etape 2 : code SQL pour prendre les transactions qui ont le même montant et les dates proches
    conn = sqlite3.connect(':memory:')
    df_virements_a_traiter.to_sql('vir_releve', conn, index=False)
    df_BO_a_traiter.to_sql('vir_BO', conn, index=False)
    qry = '''
        SELECT  *
        FROM
            vir_releve LEFT JOIN vir_BO ON
            (ABS(vir_releve.Crédit - vir_BO.Amount ) <= 5)
            AND (vir_releve.Valeur BETWEEN vir_BO.Start_Date AND vir_BO.End_Date)

        '''
    df_match = pd.read_sql_query(qry, conn)

    #Etape 3 : calculer le score fuzzy sur le résultat obtenu. 
    # Cette fonction est divisée en 2 étapes permettant d'éliminer les choix pour le fuzzy matching afin de minimiser le temps de traitement
    df_match = df_match[~df_match[colonne_mapping].isnull()]
    df_match = df_match[df_match[colonne_mapping].str.len()>=4]
    if len(df_match) > 0: 
        df_match["token_sort_score"] = df_match.apply(lambda x: fuzz.token_sort_ratio(x[colonne_mapping], x['ClientName']), axis=1)
        df_match["token_set_ratio"] = df_match.apply(lambda x: fuzz.token_set_ratio(x[colonne_mapping], x['ClientName']), axis=1)
        df_match["full_score"] = df_match.apply(lambda x: fuzz.ratio(x[colonne_mapping], x['ClientName']), axis=1)
        columns_to_keep = ['Id sys','Moyen_mapping','Subscription_Order_Name','ClientName','SubscriptionDate','Amount','Product','NoteBO','token_sort_score','token_set_ratio','full_score','partial_score','max_score'] 
            
        if bol_plusieurs_virs:
            columns_to_keep = columns_to_keep + ["Crédit_total","Max_Valeur"]
        else : columns_to_keep = columns_to_keep + ["Product_motif"]
        
        if bol_partial_score:
            df_match["partial_score"] = df_match.apply(lambda x: fuzz.partial_ratio(x[colonne_mapping], x['ClientName']), axis=1)
        else:
            df_match["partial_score"]  = 0
        df_match["max_score"] = df_match.apply(lambda x: max(x['token_sort_score'], x['partial_score'],x["full_score"],x['token_set_ratio']), axis=1)
        if bol_proposition == False: max_score = 90
        else : max_score = 70
        df_match = df_match[df_match["max_score"]>=max_score]
        df_match["Moyen_mapping"] = moyen_mapping
        # df_match = df_match.drop_duplicates(subset="Id sys",keep=False)[columns_to_keep] #supprimer tous les doublons qui seront traités après
        df_match = df_match.sort_values(by=["Id sys","SubscriptionDate"])
        df_match = df_match.drop_duplicates(subset="Id sys",keep="first")[columns_to_keep]

        # plusieurs virs pour différents comptes :
        # ces calculs sont uniquement appliqués pour le cas de virement unique, le cas de plusieurs virements pour un ordre n'est pas appliqué
        if bol_plusieurs_virs == False:
            df_match_duplicated = df_match[df_match.duplicated(subset="Subscription_Order_Name",keep=False)]
            df_match_duplicated["Meme_product"] = df_match_duplicated["Product_motif"] == df_match_duplicated["Product"] #Prendre le product noté dans le motif
            df_match_duplicated = df_match_duplicated[df_match_duplicated["Meme_product"]==True]
            df_match_duplicated = df_match_duplicated.drop_duplicates(subset="Subscription_Order_Name",keep=False).drop(columns="Meme_product")
            # Concaténer avec les virements uniques
            df_match = pd.concat([df_match,df_match_duplicated])
        
        if bol_proposition == False:
            df_match = df_match.drop_duplicates(subset="Subscription_Order_Name",keep=False)
        else : 
            list_columns = ['Id sys', 'Moyen_mapping','Product']
            df_match = df_match.groupby(by=list_columns).agg({"Subscription_Order_Name":"|".join, 
                                                                "ClientName":"|".join,
                                                                'Amount':"sum",
                                                                'SubscriptionDate':"|".join}).reset_index()

        # Remplir le résultat dans la table virments et la table de BO
        df_virements = df_virements.set_index("Id sys").combine_first(df_match.set_index("Id sys")).reset_index()
        df_BO_virs = df_BO_virs.set_index("Subscription_Order_Name").combine_first(df_match[["Id sys","Subscription_Order_Name"]].set_index("Subscription_Order_Name")).reset_index()
    return df_virements, df_BO_virs

def get_first(x):
    if "|" in x:
        return x.split("|")[0]
    return x
def get_date(id1,id2,valeur1,valeur2):
    if id1==id2: return valeur1
    return valeur2

def agreger_par_interval_date(df_virements_a_traiter,colonne_mapping,nb_days):
    '''Cette fonction permet de récupérer tous les virements dans une intervale de nb_days pour une personne pour chaque date
    Par ex : s'il y a 3 virements de 26,28 et 30 juin, on va les tous agréger pour créer un virement_bis unique afin de mapper avec le BO
    Les libraries existantes ne font qu'agréger par intervale fixe, mais ne regardent pas les virements dans les nb_days pour chaque date
    Par ex : Si quelqu'un fait un virement le 28/3 et un autre au 10/04, les librairies existants vont les agréger dans 2 intervalles différentes
    Cette fonction permet de regarder pour chaque virement, chaque date, de la même personne, on va chercher les virements dans le nb_days après pour les concaténer 

    '''
    
    #Etape 1: agréger les virements pour la même date et même personne pour éviter les doublons dans les étapes suivantes
    # 2.1 ajouter l'intervale
    df_virements_a_traiter_agrege = df_virements_a_traiter.groupby(by=[colonne_mapping,"Valeur"]).\
                                                agg({"Crédit":'sum',\
                                                     "Id sys" : "|".join   # concaténer les Id sys pour la même personne
                                                    }).reset_index()

    #Etape 2: chercher les virements dans l'interval de x jours (nb_days)

    # 2.1 ajouter l'intervale
    df1 = df_virements_a_traiter_agrege[["Id sys",colonne_mapping,"Valeur","Crédit"]].sort_values(by=[colonne_mapping,"Valeur"])
    df2 = df1.copy()
    df2.columns = ["Id sys_2",colonne_mapping +"_2","Valeur_2","Crédit_2"]
    df1["End_date"] = df1["Valeur"]+dt.timedelta(days=+int(nb_days))

    # 2.2 flagger les virements dans l'intervale
    conn = sqlite3.connect(':memory:')
    df1.to_sql('df1', conn, index=False)
    df2.to_sql('df2', conn, index=False)
    qry = f' SELECT  * ' \
            'FROM df1 JOIN df2 ' \
            'ON ' + colonne_mapping  + '=' + colonne_mapping + "_2 "\
            'AND df2.Valeur_2 <= df1.End_date AND df2.Valeur_2 > df1.Valeur'


    df_match_date_sup = pd.read_sql_query(qry,conn)
    #2.3 supprimer les virements déjà flaggés et inclus dans l'intervale du virement de la date de départ
    list_idsys2 = set(list(df_match_date_sup["Id sys_2"])) # list des Id sys qui sont déjà flaggés
    list_idsys_agreges = set(list(df_match_date_sup["Id sys_2"])+list(df_match_date_sup["Id sys"])) # tous les id sys
    df_match_date_sup = df_match_date_sup[~df_match_date_sup["Id sys"].isin(list_idsys2)] # enlever les lignes déjà flaggés
    list_idsys = set(list(df_match_date_sup["Id sys"])) #+list_concat_meme_date

    # 2.4 rajouter les virements de la date de départ
    df_date0_to_add = df_virements_a_traiter_agrege[df_virements_a_traiter_agrege["Id sys"].isin(list_idsys)]
    df_date0_to_add["Id sys_2"] = df_date0_to_add["Id sys"]
    df_date0_to_add["Crédit_2"] = df_date0_to_add["Crédit"]

    df_not_agrege = df_virements_a_traiter_agrege[~df_virements_a_traiter_agrege["Id sys"].isin(list_idsys_agreges)]
    df_not_agrege["Id sys_2"] = df_not_agrege["Id sys"]
    df_not_agrege["Crédit_2"] = df_not_agrege["Crédit"]

    # 2.5 : agréger les données
    df_match_date_sup = pd.concat([df_match_date_sup,df_date0_to_add,df_not_agrege]).sort_values([colonne_mapping,"Valeur"])
    df_match_date_sup["Valeur"] = pd.to_datetime(df_match_date_sup["Valeur"])
    
    # Prendre la date
    df_match_date_sup["Valeur_2"] = df_match_date_sup.apply(lambda f : get_date(f["Id sys"],
                                                                            f["Id sys_2"],
                                                                            f["Valeur"],
                                                                            f["Valeur_2"]), axis=1 )
    
    df_match_date_sup = df_match_date_sup.groupby(by=["Id sys","Valeur"]).agg({"Crédit_2":"sum",\
                                                                                colonne_mapping:'first',
                                                                                "Id sys_2": "|".join,
                                                                               "Valeur_2":"max"
                                                                              }).reset_index()

    df_match_date_sup["Id sys"] = df_match_date_sup["Id sys"].apply(get_first)
    df_match_date_sup = df_match_date_sup[["Id sys_2","Valeur",colonne_mapping,"Crédit_2","Valeur_2"]]
    df_match_date_sup = df_match_date_sup.rename(columns={"Id sys_2":"Id sys","Crédit_2":"Crédit","Valeur_2":"Max_Valeur"})
    df_match_date_sup["Crédit_total"] = df_match_date_sup["Crédit"]
    return df_match_date_sup

def mapping_payer_plusieurs_fois_v2(df_virements,df_BO_virs,nb_days,colonne_mapping,moyen_mapping,bol_partial_score,bol_proposition=False):
    '''Cette fonction permet de rapprocher entre le relevé vs BO pour le cas de plusieurs paiements pour le même order :
    + Agréger les données sur nb_days glissants
    + Rapprocher les données avec la règle basique (Montant, date, nom)
    '''
    df_virements_a_traiter = df_virements[df_virements["Subscription_Order_Name"].isnull()]
    # Agréger par Titulaire, date (15 jours glissants)
    columns_to_keep = ['Id sys','Moyen_mapping','Subscription_Order_Name','ClientName','SubscriptionDate','Amount','Product','NoteBO','token_sort_score','full_score','max_score',"Crédit_total","Max_Valeur"] 
    if bol_partial_score:
        columns_to_keep = columns_to_keep + ["partial_score"]
    df_virs_agrege = agreger_par_interval_date(df_virements_a_traiter,colonne_mapping,nb_days)
    if len(df_virs_agrege)>0:
        df_virs_agrege,df_BO_virs = basic_mapping_releve_vs_BO(df_virs_agrege,df_BO_virs,colonne_mapping, moyen_mapping,bol_partial_score,bol_plusieurs_virs=True,bol_proposition=bol_proposition)
        if "Subscription_Order_Name" in df_virs_agrege.columns: ### s'il y a des ordres BO qui sont rapprochés
            df_virs_agrege = df_virs_agrege[~df_virs_agrege["Subscription_Order_Name"].isnull()][columns_to_keep]
            df_BO_virs = df_BO_virs.set_index("Subscription_Order_Name").combine_first(df_virs_agrege[["Id sys","Subscription_Order_Name"]].set_index("Subscription_Order_Name")).reset_index()
        
            df_virs_agrege['Id sys'] = df_virs_agrege['Id sys'].str.split("|")
            df_virs_agrege = df_virs_agrege.explode('Id sys')
            df_virements = df_virements.set_index("Id sys").combine_first(df_virs_agrege.set_index("Id sys")).reset_index()
    return df_virements, df_BO_virs


def mapping_releve(df_virements,df_BO_virs,motif_mapping,bol_add_doublons,df_vir_doublons=pd.DataFrame(),bol_demembrement=False):
    '''Réunir le mapping basic et mapping payer plusieurs fois
    '''
    # Mapping sur montant exact
    list_colonnes_mapping = ["Motif1","Motif2","Titulaire_clean"]
    for colonne in list_colonnes_mapping:
        moyen_mapping = motif_mapping + "_Montant exact_" + colonne
        df_virements,df_BO_virs = basic_mapping_releve_vs_BO(df_virements,df_BO_virs,colonne,moyen_mapping,True,False)
    
    # Plusieurs virs pour le meme ordre
    if bol_add_doublons:
        df_virements = pd.concat([df_virements,df_vir_doublons])
    for colonne in list_colonnes_mapping:
        moyen_mapping = motif_mapping + "_Plusieurs virs pour un ordre_" + colonne
        if bol_demembrement == True: nb_jours_agrege = 30
        else: nb_jours_agrege = 15
        for nb_day in np.arange(nb_jours_agrege,1,step=-1):
            df_virements,df_BO_virs = mapping_payer_plusieurs_fois_v2(df_virements,df_BO_virs,nb_day,colonne,moyen_mapping,True)
    
    return df_virements,df_BO_virs




def rapprochement_total(df_virements,df_vir_doublons,df_BO_virs,df_ClientOps,bol_demembrement):
    '''Cette fonction est le rapprochement de tous les cas de figures et à la fois du rapprochement automatique et audit ClientOps
    '''
    list_fonds = ["EU","CC","XL"]
    df_virements_auto = pd.DataFrame()
    df_BO_auto = pd.DataFrame()
    if bol_demembrement == True: ### Pour les démembrements, le temps d'attente entre le paiement et la création des parts pourrait être 4 mois
        nb_jours_intervalle = 60
    else: nb_jours_intervalle = 30
    
    df_BO_virs["Start_Date"] = df_BO_virs["SubscriptionDate"]+dt.timedelta(days=-nb_jours_intervalle)
    df_BO_virs["End_Date"] = df_BO_virs["SubscriptionDate"]+dt.timedelta(days=nb_jours_intervalle)

    ############### Etape 1. Titulaire + montant exact et date ###############
    for fonds in list_fonds:
        df_virements_fonds = df_virements[df_virements["Fonds"]==fonds]
        df_BO_virs_fonds = df_BO_virs[df_BO_virs["Product"]==fonds]
        df_vir_doublons_fonds = df_vir_doublons[df_vir_doublons["Fonds"]==fonds]
        if len(df_virements_fonds) > 0:
            df_virements_fonds,df_BO_virs_fonds = mapping_releve(\
                                                                df_virements_fonds,\
                                                                df_BO_virs_fonds,\
                                                                "Rapproch auto_Meme compte",\
                                                                True,df_vir_doublons_fonds,bol_demembrement)

            df_virements_auto = pd.concat([df_virements_auto,df_virements_fonds]) # concaténer les fonds déjà mappés
        df_BO_auto = pd.concat([df_BO_auto,df_BO_virs_fonds]) 

    list_columns = ['Id sys', 'Fonds', 'ClientName','SubscriptionDate','Amount','Subscription_Order_Name','Product','NoteBO',\
                    'Titulaire', 'Titulaire_clean','Motif1', 'Motif2','Valeur','Crédit','Product_motif',\
                    'Date', 'Libellé', 'Type_Transaction', \
                    'Crédit_total','Max_Valeur', 'full_score', 'max_score', 'partial_score', 'token_sort_score','Moyen_mapping']
    df_virements_auto = df_virements_auto[list_columns]
    nb_etape1 = df_virements_auto[~df_virements_auto["Subscription_Order_Name"].isnull()]["Id sys"].nunique()

    ######### Etape 2. Payer pour l'autre compte #############
    df_virements_full = pd.DataFrame()
    for fonds in list_fonds:
        df_virements_fonds = df_virements_auto[df_virements_auto["Fonds"]==fonds]
        df_BO_virs_autres = df_BO_auto[df_BO_auto["Product"]!=fonds]
        df_BO_virs_fonds = df_BO_auto[df_BO_auto["Product"]==fonds]
        if len(df_virements_fonds) > 0:
            df_virements_fonds,df_BO_virs_autres = mapping_releve(\
                                                                df_virements_fonds,\
                                                                df_BO_virs_autres,\
                                                                "Rapproch auto_Virement pour autre compte",\
                                                                False,bol_demembrement=bol_demembrement)

            df_virements_full = pd.concat([df_virements_full,df_virements_fonds]) # concaténer les fonds déjà rapprochés
        df_BO_auto = pd.concat([df_BO_virs_fonds,df_BO_virs_autres]) # MAJ df_BO pour s'assurer de ne pas reprendre l'ordre déjà mappé 
    df_virements_full["Même_compte"] = df_virements_full.apply(lambda f : check_meme_compte(f['Fonds'],f['Product']),axis=1)
    df_virements_auto = df_virements_full
    nb_etape2 = df_virements_auto[~df_virements_auto["Subscription_Order_Name"].isnull()]["Id sys"].nunique()

    ######### 3. Check with ClientOps #############

    # 3.1 Préparer les données BO ClientOps
    df_rapprochement_auto = df_virements_auto[~df_virements_auto["Subscription_Order_Name"].isnull()]
    df_restant = df_virements_auto[df_virements_auto["Subscription_Order_Name"].isnull()]
    df_BO_rapprochement_auto = df_BO_auto[~df_BO_auto["Id sys"].isnull()]

    df_BO_check_script = df_BO_virs.copy() # cette df est pour vérifier le résultat entre script vs ClientOps, donc on prend l'ensemble des données de BO
    df_BO_check_script = prepare_BO_ClientOps(df_BO_check_script)

    df_BO_audit = df_BO_auto[df_BO_auto["Id sys"].isnull()] # cette df pour récupérer les lignes de ClientOps que le script n'arrive pas. Donc prendre uniquement les lignes qui ne sont pas encore rapprochées
    df_BO_audit = prepare_BO_ClientOps(df_BO_audit)

    # 3.2 Vérifier le résultat entre script vs ClientOps pour la partie du rapprochement automatique
    df_check = check_with_BO(df_BO_check_script,df_rapprochement_auto,df_ClientOps)
    df_check["Meme resultat_Script vs ClientOps"] = df_check["Subscription_Order_Name"] == df_check["Référence de l'ordre"]
    nb_etape3_2 = len(df_check)
    nb_etape3_2

    # 3.3 Vérifier le résultat de ClientOps (audit)

    ## 3.3.1 Virement unique (1 virement = 1 ordre)
    df_restant =  check_with_BO(df_BO_audit,df_restant,df_ClientOps)
    df_restant = df_restant[~df_restant["Subscription_Order_Name_BOClientOps"].isnull()]
    if len(df_restant)> 0 :
        df_restant.loc[:,"Ecart montant"] = abs(df_restant.loc[:,"Amount_BOClientOps"] - df_restant.loc[:,"Crédit"])<=0.5
        df_restant.loc[:,"Ecart date"] = abs((df_restant.loc[:,"SubscriptionDate_BOClientOps"] - df_restant.loc[:,"Valeur"]).dt.days) <= nb_jours_intervalle
        for colonne in ["Motif1","Motif2","Titulaire_clean"]:
            df_restant["score_" + colonne] = df_restant.apply(lambda z : fuzz_score(z[colonne],z["ClientName_BOClientOps"],False), axis=1)

        df_restant["max_score"] = df_restant.apply(lambda x: max(x['score_Titulaire_clean'], x['score_Motif1'],x["score_Motif2"]), axis=1)
        df_restant["Nom similaire"] = df_restant["max_score"] >= 90
        df_restant["Verification ClientOps vs BO"] = df_restant.apply(\
                                                                lambda g : check_Nom_Montant_Date(g["Nom similaire"],\
                                                                                                g["Ecart montant"],\
                                                                                                g["Ecart date"],\
                                                                                                False,\
                                                                                                "Montant exact"), axis=1 )
        df_match = df_restant[df_restant["Verification ClientOps vs BO"] !="N/A"]
        df_BO_audit = fill_BO(df_match,df_BO_audit)
        df_check = pd.concat([df_check,df_match])
        nb_etape3_3_1 = len(df_check)
        nb_etape3_3_1

        ## 3.3.2 Plusieurs virements pour un ordre
        df_restant = df_restant[df_restant["Verification ClientOps vs BO"]=="N/A"]
        colonnes_BO = ["Subscription_Order_Name_BOClientOps","ClientName_BOClientOps","Amount_BOClientOps","SubscriptionDate_BOClientOps","NoteBO_BOClientOps"]
        df_restant_agrege = df_restant.groupby(by=colonnes_BO).\
                            agg({"Id sys" : "|".join,
                                "Ecart date":all,
                                "Nom similaire" : any,
                                "Crédit":"sum",
                                "Valeur":"max"
        })

        list_idsys = list(df_restant_agrege["Id sys"])
        list_plusieurs = [x for x in list_idsys if "|" in x]
        df_restant_agrege = df_restant_agrege[df_restant_agrege["Id sys"].isin(list_plusieurs)].reset_index()
        df_restant_agrege["Ecart montant"] = abs(df_restant_agrege.loc[:,"Amount_BOClientOps"] - df_restant_agrege.loc[:,"Crédit"])<=5
        if len(df_restant_agrege) > 0 :
            df_restant_agrege["Verification ClientOps vs BO"] = df_restant_agrege.apply(\
                                                                    lambda g : check_Nom_Montant_Date(g["Nom similaire"],\
                                                                                                    g["Ecart montant"],\
                                                                                                    g["Ecart date"],\
                                                                                                    True,\
                                                                                                    "Plusieurs virements pour 1 ordre"), axis=1 )

            df_restant_agrege_ok = df_restant_agrege[df_restant_agrege["Verification ClientOps vs BO"]!="N/A"]
            df_restant_agrege_ok["Crédit_total"] = df_restant_agrege_ok["Crédit"]
            df_restant_agrege_ok["Max_Valeur"] = df_restant_agrege_ok["Valeur"]
            df_restant_agrege_ok["Id sys"] = df_restant_agrege_ok["Id sys"].str.split("|")
            df_restant_agrege_ok = df_restant_agrege_ok.explode('Id sys')
            df_restant_agrege_ok = df_restant_agrege_ok[["Id sys","Verification ClientOps vs BO","Ecart montant","Crédit_total","Max_Valeur"]]
            df_restant = df_restant.drop(columns=["Ecart montant","Verification ClientOps vs BO","Crédit_total","Max_Valeur"])
            df_restant = df_restant.merge(df_restant_agrege_ok,on="Id sys", how="outer")
            df_restant["Verification ClientOps vs BO"] = df_restant["Verification ClientOps vs BO"].fillna("N/A")
            df_restant["Ecart montant"] = df_restant["Ecart montant"].fillna(False)
            df_match = df_restant[df_restant["Verification ClientOps vs BO"] !="N/A"]
            df_BO_audit = fill_BO(df_match,df_BO_audit)
            df_check = pd.concat([df_check,df_match])
        nb_etape3_3_2 = len(df_check)
        nb_etape3_3_2

        # 3.3.3 Nom partiel
        df_restant = df_restant[df_restant["Verification ClientOps vs BO"]=="N/A"]
        df_restant.loc[:,"Ecart montant"] = abs(df_restant.loc[:,"Amount_BOClientOps"] - df_restant.loc[:,"Crédit"])<=5

        for colonne in ["Motif1","Motif2","Titulaire_clean"]:
            df_restant["score_" + colonne] = df_restant.apply(lambda z : fuzz_score(z[colonne],z["ClientName_BOClientOps"],True), axis=1)
        df_restant["max_score"] = df_restant.apply(lambda x: max(x['score_Titulaire_clean'], x['score_Motif1'],x["score_Motif2"]), axis=1)
        df_restant["Nom similaire"] = df_restant["max_score"] >= 90
        if len(df_restant) >0 :
            df_restant["Verification ClientOps vs BO"] = df_restant.apply(\
                                                                    lambda g : check_Nom_Montant_Date(g["Nom similaire"],\
                                                                                                    g["Ecart montant"],\
                                                                                                    g["Ecart date"],\
                                                                                                    True,\
                                                                                                    "Montant exact"), axis=1 )
            df_match = df_restant[df_restant["Verification ClientOps vs BO"] !="N/A"]
            df_BO_audit = fill_BO(df_match,df_BO_audit)
            df_check = pd.concat([df_check,df_match])
        nb_etape3_3_3 = len(df_check)
        nb_etape3_3_3

        ## 3.3.4 Un virement pour plusieurs ordres / mouvements
        df_restant = df_restant[df_restant["Verification ClientOps vs BO"]=="N/A"].drop(columns="Ecart montant")
        df_restant = df_restant[~df_restant["Subscription_Order_Name_BOClientOps"].isnull()]
        df_restant_agrege = df_restant.groupby(by=["Id sys","Valeur","Crédit","Fonds_x"])["Amount_BOClientOps"].sum().reset_index()
        df_restant_agrege["Amount_BO_overall"] = df_restant_agrege["Amount_BOClientOps"]
        df_restant_agrege["Ecart montant"] =  abs(df_restant_agrege.loc[:,"Amount_BOClientOps"] - df_restant_agrege.loc[:,"Crédit"])<=5
        if len(df_restant_agrege)>0 :
            df_restant2 = df_restant.merge(df_restant_agrege[["Id sys","Ecart montant","Amount_BO_overall"]], on="Id sys")
            df_restant2["Verification ClientOps vs BO"] = df_restant2.apply(\
                                                                    lambda g : check_Nom_Montant_Date(g["Nom similaire"],\
                                                                                                    g["Ecart montant"],\
                                                                                                    g["Ecart date"],\
                                                                                                    True,\
                                                                                                    "Un vir pour pls ordres"), axis=1 )
            df_match = df_restant2[df_restant2["Verification ClientOps vs BO"]!="N/A"]
            df_BO_audit = fill_BO(df_match,df_BO_audit)
            df_check = pd.concat([df_check,df_match])
        nb_etape3_3_4 = df_check["Id sys"].nunique()
        nb_etape3_3_4

        df_BO_virs_check = pd.concat([df_BO_audit,df_BO_rapprochement_auto])
        list_resultat = [nb_etape1,nb_etape2,nb_etape3_2,nb_etape3_3_1,nb_etape3_3_2,nb_etape3_3_3,nb_etape3_3_4]
    else : 
        df_BO_virs_check = df_BO_rapprochement_auto
        list_resultat = [nb_etape1,nb_etape2,nb_etape3_2]
    df_check["Product_overall"] = df_check["Product"].fillna(df_check["Product_BOClientOps"])
    df_check["Même_compte"] = df_check["Fonds_x"] == df_check["Product_overall"]
    return df_check,df_BO_virs_check,list_resultat

def add_comment(df_check):
    '''Catégoriser les cas de rapprochement (Best Case, TP, MP, Rattrapage,...) et aussi le commentaire sur la façon de rapprochement
    '''
    df_check["SubscriptionDate"] = pd.to_datetime(df_check["SubscriptionDate"], format='%Y/%m/%d %H:%M:%S')
    df_check["Date_BO_overall"]  = df_check["SubscriptionDate"].fillna(df_check["SubscriptionDate_BOClientOps"]) # si le rapprochement automaitiquement ne fonctionne pas, prendre le résultat de ClientOps
    df_check["Amount_BO_overall"]  = df_check["Amount_BO_overall"].fillna(df_check["Amount"]).fillna(df_check["Amount_BOClientOps"])
    df_check["Date_virs_overall"]  = df_check["Max_Valeur"].fillna(df_check["Valeur"]) # plusieurs virs pour le même ordre
    df_check["Amount_virs_overall"]  = df_check["Crédit_total"].fillna(df_check["Crédit"]) # plusieurs virs pour le même ordre
    df_check["Verification ClientOps vs BO"] = df_check["Verification ClientOps vs BO"].fillna(df_check["Moyen_mapping"])


    df_check["Check_date"] = df_check.apply(lambda f:\
                                        check_date(f["Date_BO_overall"],f["Date_virs_overall"]),axis=1)
    df_check["Check_montant"] = df_check.apply(lambda f:\
                                        check_montant(f["Amount_BO_overall"],f["Amount_virs_overall"]),axis=1)
    df_check["Pls_lignes"] = df_check["Verification ClientOps vs BO"].apply(check_pls_lignes)

    df_check["Check_overall"] = df_check.apply(lambda f:\
                                        check_overall(f["Check_date"],\
                                                      f["Check_montant"],\
                                                       f["Même_compte"],\
                                                        f["Pls_lignes"]),axis=1)

    df_check_script = df_check[~df_check["Meme resultat_Script vs ClientOps"].isnull()]
    df_check_clientops = df_check[df_check["Meme resultat_Script vs ClientOps"].isnull()]
    df_check_script["Verification ClientOps vs BO"] = df_check_script["Verification ClientOps vs BO"].fillna(df_check_script["Moyen_mapping"])

    df_check_script["Verification ClientOps vs BO"] = df_check_script.apply(lambda f: \
                                                                            add_comment_script(f["Meme resultat_Script vs ClientOps"],\
                                                                                            f["Verification ClientOps vs BO"]),axis=1)

    df_check_clientops["Verification ClientOps vs BO"] = "Audit ClientOps_"+ df_check_clientops["Verification ClientOps vs BO"]
    df_check = pd.concat([df_check_script,df_check_clientops])
    return df_check


def mapping_un_paiement_pls_ords(df_virements,df_BO_virs,colonne_mapping,moyen_mapping,bol_partial_score,bol_proposition=False):
    '''Cette fonction permet de rapprocher entre les données bancaires et les données de BO pour le cas d'un paiement (virement/cheque) pour plusieurs ordres:
    + Prendre tous les ordres après la date de paiement avec un montant supérieur
    + Agréger les ordres ayant le même ClientName pour voir si le montant match
    Pour les variables : 
    - colonne_mapping (str): colonne pour le nom dans relevé (Titulaire, Motif)
    - moyen_mapping (str): le commentaire /statut pour ajouter dans le résultat afin d'identifer quelle étape du rapprochement
    - bol_partial_score (bolean) : utiliser ou non le score partial du fuzzy matching
    - bol_proposition (bolean): le rapprochement est utlisé pour faire des propositions d'ordre. Si oui, le score de fuzzy matching est 70%, si non 90% 
    '''
    
    #Etape 1 : filtrer sur les lignes qui ne sont pas encore mappées et prendre seulement des colonnes nécessaires
    
    if "Subscription_Order_Name" in df_virements.columns: # si la table est déjà mappée
        df_virements_a_traiter = df_virements[df_virements["Subscription_Order_Name"].isnull()]
    else :
            df_virements_a_traiter = df_virements
    
    if "Id sys" in df_BO_virs.columns: # si la table est déjà mappée
        df_BO_a_traiter = df_BO_virs[df_BO_virs["Id sys"].isnull()].drop(columns="Id sys")
    else :
            df_BO_a_traiter = df_BO_virs
    columns_vir = ['Id sys', 'Valeur', 'Crédit', colonne_mapping,'Product_motif']
    df_virements_a_traiter = df_virements_a_traiter[columns_vir]
    
    #Etape 2 : code SQL pour prendre les transactions qui ont le même montant et les dates proches
    conn = sqlite3.connect(':memory:')
    df_virements_a_traiter.to_sql('vir_releve', conn, index=False)
    df_BO_a_traiter.to_sql('vir_BO', conn, index=False)
    qry = '''
        SELECT  *
        FROM
            vir_releve LEFT JOIN vir_BO ON
            vir_releve.Crédit < vir_BO.Amount
            AND (vir_releve.Valeur BETWEEN vir_BO.Start_Date AND vir_BO.End_Date)

        '''
    df_match = pd.read_sql_query(qry, conn)

    #Etape 3 : calculer le score fuzzy sur le résultat obtenu. 
    # Cette fonction est divisée en 2 étapes permettant d'éliminer les choix pour le fuzzy matching afin de minimiser le temps de traitement
    df_match = df_match[~df_match[colonne_mapping].isnull()]
    df_match = df_match[df_match[colonne_mapping].str.len()>=4]
    if len(df_match) > 0:
        df_match["token_sort_score"] = df_match.apply(lambda x: fuzz.token_sort_ratio(x[colonne_mapping], x['ClientName']), axis=1)
        df_match["token_set_ratio"] = df_match.apply(lambda x: fuzz.token_set_ratio(x[colonne_mapping], x['ClientName']), axis=1)
        df_match["full_score"] = df_match.apply(lambda x: fuzz.ratio(x[colonne_mapping], x['ClientName']), axis=1)
        columns_to_keep = ['Id sys','Moyen_mapping','Subscription_Order_Name','ClientName','SubscriptionDate','Amount','Product','NoteBO',"Crédit"] 
        
        if bol_partial_score:
            df_match["partial_score"] = df_match.apply(lambda x: fuzz.partial_ratio(x[colonne_mapping], x['ClientName']), axis=1)
        else:
            df_match["partial_score"]  = 0
        df_match["max_score"] = df_match.apply(lambda x: max(x['token_sort_score'], x['partial_score'],x["full_score"],x['token_set_ratio']), axis=1)
        if bol_proposition==False: max_score = 90
        else : max_score = 70
        df_match = df_match[df_match["max_score"]>=max_score]
        df_match["Moyen_mapping"] = moyen_mapping
        df_match = df_match.drop_duplicates(subset="Id sys",keep=False)[columns_to_keep] #supprimer tous les doublons qui seront traités après
        df_match_agg = df_match.groupby(by=["Subscription_Order_Name","ClientName","SubscriptionDate","Amount","Product","NoteBO"]).agg({
                                                                                            "Id sys":";".join, "Crédit":"sum" }).reset_index()
        df_match_agg = df_match_agg[abs(df_match_agg["Amount"]-df_match_agg["Crédit"])<=5]
        df_match_agg["Id sys"] = df_match_agg["Id sys"].str.split(";")
        df_match = df_match_agg.drop(columns="Crédit").explode("Id sys")

        # Merge avec virments et BO
        df_virements = df_virements.set_index("Id sys").combine_first(df_match.set_index("Id sys")).reset_index()
        df_BO_virs = df_BO_virs.set_index("Subscription_Order_Name").combine_first(df_match[["Id sys","Subscription_Order_Name"]].set_index("Subscription_Order_Name")).reset_index()
    return df_virements, df_BO_virs