import datetime as dt
import pandas as pd
import numpy as np
import re
from functions.utils import *


def clean_donnees_releve(df_releve_bancaire):
    '''Cette fonction permet de nettoyer les données bancaires. 
    Elle prend en entrée l'extraction initale des relevés bancaires et puis :
    + Formatter les données au bon format
    + Récupérer les informations utiles dans le Libellé (Titulaire du compte, Type de transaction, Motif de virement)
    + Splitter les données en Débit et Crédit
    + Controler les données en entrant et en sortant
    '''
    df_releve_bancaire = df_releve_bancaire[["Id sys","Date","Valeur","Libellé","Ref.","Débit","Crédit","Fonds","IBAN"]].dropna(subset="Id sys")
    df_releve_bancaire["Id sys"] = df_releve_bancaire["Id sys"].astype(int).astype(str)
    
    #Convert to datetime
    df_releve_bancaire.loc[:,"Date"] = pd.to_datetime(df_releve_bancaire["Date"], format='%d/%m/%Y')
    df_releve_bancaire.loc[:,"Valeur"] = pd.to_datetime(df_releve_bancaire["Valeur"], format='%d/%m/%Y')
    df_releve_bancaire.loc[:,"Mois"] = df_releve_bancaire.loc[:,"Valeur"].dt.month
    
    # Splitter en crédit et débit
    df_debit = df_releve_bancaire[df_releve_bancaire["Crédit"].isnull()]
    df_credit = df_releve_bancaire[df_releve_bancaire["Débit"].isnull()]
    
    # Nettoyer le Libellé
    df_credit.loc[:,"Libellé"] = df_credit.loc[:,"Libellé"].str.replace("/M/","/")
    df_credit.loc[:,"Libellé"] = df_credit.loc[:,"Libellé"].str.replace("/NA ME/","/NAME/")
    df_credit.loc[:,"Libellé"] = df_credit.loc[:,"Libellé"].str.replace("/ /","").str.upper()
    
    # créer des paterns regex permettant de récupérer des informations dans le Libellé

    ############# Type de transaction ########
    #les caractères entre un chiffre et le mot CODE
    patern_type_transaction = r'(?<=\d)\/(.*)(?=\/CODE)' 
    ############# Titulaire du compte ########
    #les lettres, espaces, chiffres, et points uniquement après le mot NAME
    #s'il rencontre le caractère spécial \ il va s'arrêter
    patern_name = r'(?<=NAME)\/([a-zA-Z\s\-\.\d]*)'
    ############# Motif de virement ########
    patern_EREF = r'(?<=EREF)\/([a-zA-Z\s\-\.\d]*)'
    patern_REMI = r'(?<=REMI)\/([a-zA-Z\s\-\.\d]*)'

    ############## Prélèvement ########
    patern_PREF = r'(?<=PREF)\/([a-zA-Z\s\-\.\d]*)'
    patern_NBTR = r'(?<=NBTR)\/([a-zA-Z\s\-\.\d]*)'

    # Appliquer sur le regex
    df_credit["Type_Transaction"] = df_credit['Libellé'].str.extract(patern_type_transaction)
    df_credit["Titulaire"] = df_credit['Libellé'].str.extract(patern_name)

    df_credit["EREF"] = df_credit['Libellé'].str.extract(patern_EREF)
    df_credit["EREF"] = df_credit["EREF"].str.upper().astype(str)
    list_to_remove = ['NOTPROVIDED', 'NOT PROVIDED','ACHAT PARTS DE SCPI','ACHAT PARTS SCPI']
    df_credit["EREF"] = df_credit["EREF"].replace(list_to_remove, np.nan)

    df_credit["REMI"] = df_credit['Libellé'].str.extract(patern_REMI).astype(str)
    
    # Cleaner le données récupérées
    # df_credit =  clean_name(df_credit,"Titulaire","Titulaire_clean")
    df_credit.loc[:,"Titulaire_clean"] =  df_credit.loc[:,"Titulaire"].apply(clean_name)

    df_credit.loc[:,"Motif1"] =  df_credit.loc[:,"REMI"].apply(clean_motif)
    df_credit.loc[:,"Motif1"] =  df_credit.loc[:,"Motif1"].apply(clean_name)
    df_credit.loc[:,"Motif1"] = df_credit.loc[:,"Motif1"].apply(remove_duplicated)
    for i in range(0,5): 
        df_credit.loc[:,"Motif1"] = df_credit.loc[:,"Motif1"].apply(remove_de)

    df_credit.loc[:,"Motif2"] =  df_credit.loc[:,"EREF"].apply(clean_motif)
    df_credit.loc[:,"Motif2"] =  df_credit.loc[:,"Motif2"].apply(clean_name)
    df_credit.loc[:,"Motif2"] = df_credit.loc[:,"Motif2"].apply(remove_duplicated)
    for i in range(0,5): 
        df_credit.loc[:,"Motif2"] = df_credit.loc[:,"Motif2"].apply(remove_de)
    
    df_credit.loc[:,"Id sys"] = df_credit.loc[:,"Id sys"].astype(str)
    df_credit.loc[:,"Motif1"] = df_credit.loc[:,"Motif1"].astype(str)
    df_credit.loc[:,"Motif2"] = df_credit.loc[:,"Motif2"].astype(str)
    
    df_credit.loc[:,"Product_motif"] = df_credit.apply(lambda f : get_product_motif(f["REMI"],f["EREF"]), axis=1)
    df_credit = df_credit.drop(columns=["REMI","EREF"])

    df_credit.loc[:,"Nb_transaction"] = df_credit.loc[:,'Libellé'].str.extract(patern_NBTR)
    df_credit.loc[:,"ID_remise"] = df_credit.loc[:,'Libellé'].str.extract(patern_PREF)


    ### Nettoyer Debit ####
    

    df_debit.loc[:,"Type_Transaction"] = df_debit.loc[:,'Libellé'].str.extract(patern_type_transaction)
    
    # Controler le script
    df_control_donnes_bancaires = pd.DataFrame(columns=["etape","nb_colonnes","montant_total"])
    df_control_donnes_bancaires = control_script(df_control_donnes_bancaires,\
                                   "extraction_initiale",df_releve_bancaire["Id sys"].count(),\
                                   df_releve_bancaire["Crédit"].sum())
    df_control_donnes_bancaires = control_script(df_control_donnes_bancaires,\
                                    "total_debit",df_debit["Id sys"].count(),\
                                    df_debit["Débit"].sum())
    df_control_donnes_bancaires = control_script(df_control_donnes_bancaires, \
                                    "total_credit",df_credit["Id sys"].count(),\
                                    df_credit["Crédit"].sum())
    
    return df_credit, df_debit, df_control_donnes_bancaires

def clean_libelle_to_titulaire(df_virements,colonne_titulaire,colonne_a_nettoyer,list_bic,bol_clean_voyelles=True):
    '''Cette fonction permet de nettoyer le libellé : supprimer tous les mots parasites, les chiffres, caractères spéciaux,...
    df_virements : dataframe à traiter
    colonne_titulaire : nom souhaité de la colonne titulaire
    colonne_libelle : nom de la colonne libellé à nettoyer
    list_bic : liste des BIC à enlever
    '''
    #### Supprimer les mots parasites
    df_virements.loc[:,colonne_titulaire] = df_virements[colonne_a_nettoyer].apply(clean_motif)
    df_virements.loc[:,colonne_titulaire] = df_virements[colonne_titulaire].apply(clean_motif)
    df_virements.loc[:,colonne_titulaire] = df_virements.loc[:,colonne_titulaire].apply(remove_duplicated)

    #### Supprimer les BIC dans le libellé
    mask = ~df_virements[colonne_titulaire].isnull()
    ### supprimer les BIC
    df_virements.loc[mask,colonne_titulaire] = df_virements.loc[mask,colonne_titulaire].apply(lambda text : " ".join([x for x in text.split() if x[0:8] not in list_bic])) 
    ## Supprimer les XX
    df_virements.loc[mask,colonne_titulaire] = df_virements.loc[mask,colonne_titulaire].apply(lambda text : " ".join([x for x in text.split() if "XX" not in x])) 
    ## supprimer les mots ne contenant pas de voyelles == des mots qui ne sont pas nom, prénom
    if bol_clean_voyelles == True:
        df_virements.loc[mask,colonne_titulaire] = df_virements.loc[mask,colonne_titulaire].apply(lambda text : " ".join([word for word in text.split() if any(letter in 'AEIOU' for letter in word) ]))
    ### Supprimer Mme Monsieur...
    df_virements.loc[mask,colonne_titulaire] = df_virements.loc[:,colonne_titulaire].apply(clean_name)
    ### supprimer tous les DE, DU, LE, LA, AU à la fin
    for i in range(0,5): 
        df_virements.loc[mask,colonne_titulaire] = df_virements.loc[mask,colonne_titulaire].apply(remove_de)
    return df_virements

def clean_BO(df_BO):
    '''Nettoyer les données de BO
    '''
    df_BO['Amount'] = df_BO['Amount'].replace(r'\,','.',regex=True).astype(float)
    df_BO['SharesNumber'] = df_BO['SharesNumber'].replace(r'\,','.',regex=True).astype(float)
    df_BO['AccountUnitAmount'] = df_BO['AccountUnitAmount'].replace(r'\,','.',regex=True).astype(float)
    
    df_BO["SubscriptionDate"] = pd.to_datetime(df_BO["SubscriptionDate"], format='%Y-%m-%d')
    df_BO["Collectiondate"] = pd.to_datetime(df_BO["Collectiondate"], format='%Y-%m-%d')
    df_BO["MovementDate"] = pd.to_datetime(df_BO["MovementDate"], format='%Y-%m-%d')
    # df_BO["Start_Date"] = df_BO["SubscriptionDate"]+dt.timedelta(days=-30)
    # df_BO["End_Date"] = df_BO["SubscriptionDate"]+dt.timedelta(days=30)
    
    df_BO_initial = df_BO
    df_BO_retraits_annul = df_BO_initial[df_BO_initial["MoveType"]!="Part nouvelle"]
    df_BO = df_BO_initial[df_BO_initial["MoveType"]=="Part nouvelle"].reset_index()
    ######### Supprimer M. Mme dans les noms prénoms ########
    # df_BO =  clean_name(df_BO,"ClientName","ClientName")
    df_BO.loc[:,"ClientName"] = df_BO["ClientName"].apply(clean_name)

    
    df_control_donnes_BO = pd.DataFrame(columns=["etape","nb_records","montant_total"])
    df_control_donnes_BO = control_script(df_control_donnes_BO,\
                                    "extraction_initiale",df_BO_initial["Id"].count(),\
                                    df_BO_initial["Amount"].sum())
    df_control_donnes_BO = control_script(df_control_donnes_BO,\
                                   "retrait_annulation",df_BO_retraits_annul["Id"].count(),\
                                   df_BO_retraits_annul["Amount"].sum())
    df_control_donnes_BO = control_script(df_control_donnes_BO,\
                                   "part_nouvelle",df_BO["Id"].count(),\
                                   df_BO["Amount"].sum())
    
    return df_BO, df_control_donnes_BO

def clean_souscription(df_souscription,bol_matching_columns=False):  
    '''Nettoyer les données de souscription
    '''
    df_souscription['SubscriptionAmount'] = df_souscription['SubscriptionAmount'].replace(r'\,','.',regex=True).astype(float)
    df_souscription['SharesNumber'] = df_souscription['SharesNumber'].replace(r'\,','.',regex=True).astype(float)
    df_souscription['Amount_Bulletin'] = df_souscription['Amount_Bulletin'].replace(r'\,','.',regex=True).astype(float)
    df_souscription['SharesNumber_Dec'] = df_souscription['SharesNumber_Dec'].replace(r'\,','.',regex=True).astype(float)
    df_souscription["Payment_Reception_Date"] = pd.to_datetime(df_souscription["Payment_Reception_Date"], format='%d/%m/%Y %H:%M:%S') 
    df_souscription["SubscriptionDate"] = pd.to_datetime(df_souscription["SubscriptionDate"], format='%Y-%m-%d')    
    ######### Supprimer M. Mme dans les noms prénoms ########
    # df_souscription =  clean_name(df_souscription,"Clientname","Clientname")
    df_souscription.loc[:,"Clientname"] = df_souscription["Clientname"].apply(clean_name)

    souscripton_colonnes = ["Idsubscriptionorder","PaymentMode","Id","Clientname","IDclient","SubscriptionDate","SubscriptionAmount",\
                            "Product","SharesNumber","PropertyType","Status","Payment_Reception_Date","SharesNumber_Dec","Amount_Bulletin"]
    if bol_matching_columns:
        souscripton_colonnes += ["Matching_NP_Ordre","Matching_UF_Ordre","Matching_NP_Ordre_ID","Matching_UF_Ordre_ID"]
    new_name = [x + "_order" for x in souscripton_colonnes] 
    dict_name = dict(zip(souscripton_colonnes,new_name))
    df_souscription = df_souscription[souscripton_colonnes].rename(columns=dict_name)
    return df_souscription

def create_index(df_releve_clean,prefix):
    '''Les fichiers excel convertis ne sont pas propres. Il n'y a pas d'ID, les lignes sont sautées (un libellé peut trouver sur plusieurs rows)
    Cette fonction va créer un id unique pour chaque transaction
    '''
    list_index = []
    for idx,row in enumerate(df_releve_clean.itertuples(),1):
        if row[2]:
            if row[1]=="NAN":
                list_index.append(list_index[len(list_index)-1]) # Prendre la valeur précédente
            else: 
                list_index.append(idx)
    list_index = [prefix + str(x) for x in list_index]
    df_releve_clean["index"] = list_index
    return df_releve_clean
def identify_iban_column(df_releve_pdf,mot_iban = "\bIBAN\b"):
    '''return la dataframe qui contient que les IBAN
    '''
    df_iban = pd.DataFrame()
    for column in range(0,5): ### boucler des colonnes de 0 à 4 pour voir si elle contient l'IBAN
        df_releve_pdf[column] = df_releve_pdf[column].astype(str)
        df_releve_pdf["flag"] = df_releve_pdf[column].str.extract(r"(\bIBAN\b)")
        df_col = df_releve_pdf[~df_releve_pdf["flag"].isnull()]
        df_col["flag"] = df_col[column].str.extract(r"(\bBIC\b)")
        df_col = df_col[~df_col["flag"].isnull()][[column]]
        df_col.columns=[0] ### renommer la colonne pour le pd.concat
        df_iban = pd.concat([df_iban,df_col])
    df_iban = df_iban.reset_index().sort_values(by="index").set_index("index") ### reclasser le résultat par l'ordre de l'index
    return df_iban
def get_iban(text,bol_start_collect = False):
    '''Get le numéro d'IBAN  
    text : la cellule qui contient le numéro d'IBAN
    La fonction va prendre tous les caractères entre IBAN et BIC. Sauf pour les fichiers récents (bol_start_collect=True) ce serait entre Collecte et EUR
    '''
    if bol_start_collect == True: pattern_iban = '(?<=Collecte)\s+(.*)(?=EUR)'
    else : pattern_iban = '(?<=IBAN)\s+\:([\w\d\s\-i]*)'
    iban = re.findall(pattern_iban,text)[0]
    iban = iban.replace("i"," ").replace("BIC","").strip().replace("-"," ")
    return iban

def agreger_lignes_releve_pdf(df_releve_pdf,filename,bol_spec_file,bol_bred):
    '''Les fichiers excel converti ne sont pas exploitable directement. Pour les libellés plusieurs lignes, ça crée plusieurs rows dans Excel pour la même transaction.
    Cette fonction va agréger les lignes de libellé qui appartiennent à la même transaction
    '''
    #### Prendre les lignes contenant l'IBAN
    if bol_bred == False :
        df_iban = identify_iban_column(df_releve_pdf) 
        df_iban["IBAN"] = df_iban[0].apply(get_iban)
        df_iban = df_iban.drop(columns=0).reset_index()
    df_releve_pdf = df_releve_pdf.replace("nan",np.nan)
    # Chercher les entetes car elles sont décalées d'un fichier à l'autre
    dict_column = get_headers(df_releve_pdf,bol_spec_file,bol_bred)
    df_releve_clean = df_releve_pdf[list(dict_column.keys())]
    df_releve_clean = df_releve_clean.rename(columns=dict_column)
    if bol_bred==True :
        columns_bank_bred = ["Date","Référence","Valeur","Débit","Crédit"]
        columns_bank = ["DATE COMPTABLE","NATURE DES OPERATIONS","DATE DE VALEUR","DEBIT","CREDIT"]
        dict_column_commun = dict(zip(columns_bank_bred,columns_bank))
        df_releve_clean = df_releve_clean.rename(columns=dict_column_commun)
    
        # Formatter les données
        df_releve_clean["DATE COMPTABLE"] = df_releve_clean["DATE COMPTABLE"].astype(str).str.upper()
        df_releve_clean["NATURE DES OPERATIONS"] = df_releve_clean["NATURE DES OPERATIONS"].astype(str).str.upper()
        df_releve_clean["DATE DE VALEUR"] = df_releve_clean["DATE DE VALEUR"].astype(str).str.upper()
        df_releve_clean["DEBIT"] = df_releve_clean["DEBIT"].astype(str).str.upper()
        df_releve_clean["CREDIT"] = df_releve_clean["CREDIT"].astype(str).str.upper()

        ### Supprimer les lignes inutiles
        df_releve_clean["delete"] = df_releve_clean.apply(lambda f : exclure_lignes(f["NATURE DES OPERATIONS"],f["DEBIT"],f["CREDIT"]),axis=1)
        df_releve_clean = df_releve_clean[df_releve_clean["delete"]==False].drop(columns="delete")
        df_releve_clean = df_releve_clean[df_releve_clean["DATE COMPTABLE"]!="TOTAL DES MOUVEMENTS"]

        list_values_to_delete = "TOTAL DES OPERATIONS|NATURE DES OPERATIONS|SOLDE PRÉCÉDENT|SOLDE PRECEDENT|SOLDE CRÉDITEUR|SOLDE CREDITEUR|RÉFÉRENCE|NOUVEAU SOLDE"
        df_releve_clean = df_releve_clean[~df_releve_clean["NATURE DES OPERATIONS"].str.contains(list_values_to_delete, na=False)]
        list_values_to_delete = "CENTRE D'AFFAIRE|IBAN|PERIODE DU|BIC|RIB"
        df_releve_clean = df_releve_clean[~df_releve_clean["DATE COMPTABLE"].str.contains(list_values_to_delete, na=False)]
        df_releve_clean = df_releve_clean[~df_releve_clean["DEBIT"].str.contains("CORUM", na=False)]
    else :
        #Supprimer les lignes inutiles
        df_releve_clean = df_releve_clean[~df_releve_clean["NATURE DES OPERATIONS"].isnull()]
        list_values_to_delete = "TOTAL DES OPERATIONS|NATURE DES OPERATIONS|Solde précédent|Solde precedent|Solde créditeur|Solde crediteur"
        df_releve_clean = df_releve_clean[~df_releve_clean["NATURE DES OPERATIONS"].str.contains(list_values_to_delete, na=False)]

        # Formatter les données
        df_releve_clean["DATE COMPTABLE"] = df_releve_clean["DATE COMPTABLE"].astype(str).str.upper()
        df_releve_clean["NATURE DES OPERATIONS"] = df_releve_clean["NATURE DES OPERATIONS"].astype(str).str.upper()
        df_releve_clean["DATE DE VALEUR"] = df_releve_clean["DATE DE VALEUR"].astype(str).str.upper()
        df_releve_clean["DEBIT"] = df_releve_clean["DEBIT"].astype(str).str.upper()
        df_releve_clean["CREDIT"] = df_releve_clean["CREDIT"].astype(str).str.upper()

        list_values_to_delete = "CENTRE D'AFFAIRE|IBAN|PERIODE DU|BIC|RIB"
        df_releve_clean = df_releve_clean[~df_releve_clean["DATE COMPTABLE"].str.contains(list_values_to_delete, na=False)]
        df_releve_clean = df_releve_clean[~df_releve_clean["DEBIT"].str.contains("CORUM", na=False)]
        
        df_releve_clean["delete"] = df_releve_clean.apply(lambda f : exclure_lignes(f["NATURE DES OPERATIONS"],f["DEBIT"],f["CREDIT"]),axis=1)
        df_releve_clean = df_releve_clean[df_releve_clean["delete"]==False].drop(columns="delete")
        #### Merger avec les IBAN
        df_releve_clean = df_releve_clean.reset_index()

        min_index = df_iban.loc[0,"index"]
        max_index = df_releve_clean.tail(1)["index"] + 1
        new_index = [i for i in range(min_index,int(max_index))]
        df_iban = df_iban.set_index("index").reindex(new_index,method='ffill').reset_index()

        df_releve_clean = df_releve_clean.merge(df_iban,on="index").set_index("index")
        df_releve_clean.index.name = None
    ##### Créer l'index et agréger les lignes
    df_releve_clean = create_index(df_releve_clean,filename + "_")
    if "IBAN" not in df_releve_clean.columns : df_releve_clean["IBAN"] = "BRED"
    df_releve_clean_agrege = df_releve_clean.groupby("index").agg({
                                     "DATE COMPTABLE":"first",\
                                     "NATURE DES OPERATIONS":" ".join,\
                                     "DATE DE VALEUR":"first",\
                                     "DEBIT":"first",\
                                     "CREDIT":"first",\
                                     "IBAN" : "first"
                                    })
    return df_releve_clean_agrege

   
def concat_fichiers_clean_with_list(dict_files, list_files,bol_spec_file,bol_bred):
    '''Cette fonction va concaténer les fichiers excel. Elle prend en entrée un dictionnaire comme filename : df_fichier
    '''
    df_releve_concat = pd.DataFrame()
    for filename,df_releve_pdf in dict_files.items():
        if filename in list_files:
            filename = filename.replace(".xlsx","")
            df_releve_agrege = agreger_lignes_releve_pdf(df_releve_pdf,filename,bol_spec_file,bol_bred)
            df_releve_concat = pd.concat([df_releve_concat,df_releve_agrege])
    return df_releve_concat


def clean_releve_pdf(dict_files,date_format = '%Y-%m-%d %H:%M:%S',bol_spec_file=False,bol_bred=False):
    '''Nettoyer les données relevés convertis par des fichiers pdf
    bol_spec_file : il s'agit d'un fichier particulier avec 
    '''
    df_releve_clean = concat_fichiers_clean_with_list(dict_files,dict_files.keys(),bol_spec_file,bol_bred)#.reset_index() # concaténer tous les fichiers à partir du dictionnaire
    df_releve_clean["CREDIT"] = df_releve_clean["CREDIT"].str.replace(",",".").str.replace(" ","")
    df_releve_clean["CREDIT"] = df_releve_clean["CREDIT"].astype(float)
    df_releve_clean["DEBIT"] = df_releve_clean["DEBIT"].str.replace(",",".").str.replace(" ","")
    df_releve_clean["DEBIT"] = df_releve_clean["DEBIT"].astype(float)
    df_releve_clean["DATE DE VALEUR"] = pd.to_datetime(df_releve_clean["DATE DE VALEUR"], format = date_format)
    df_releve_clean["DATE COMPTABLE"] = pd.to_datetime(df_releve_clean["DATE COMPTABLE"], format = date_format)
    dict_columns = {"DATE COMPTABLE" : "Date",
                    "NATURE DES OPERATIONS": "Libellé",
                    "DATE DE VALEUR":"Valeur",
                    "DEBIT":"Débit",
                    "CREDIT":"Crédit"}
    df_releve_clean = df_releve_clean.rename(columns=dict_columns)            
    return df_releve_clean

def splitter_fichier(dict_files,df_releve_pdf,filename) :
    '''cette fonction est pour splitter chaque table au sein d'un fichier Excel en une dataframe.
    Cad, les fichiers Excels convertis sont souvent décalés d'une page à l'autre, donc on peut avoir la Date sur la colonne A, B, C,D
    Cette fonction va détecter les tables les splitter. En sortie, on va avoir un dictionnaire au format : {N° table : df_table}
    Cela permet ensuite d'utiliser la fonction get_headers de détecter la position des colonnes.
    
    dict_files : dictionnaire de données pour ajouter les données de sortie
    df_releve_pdf : dataframe du fichier
    filename : nom du fichier pour crééer la clé
    '''
    ####### flagger les lignes des entetes 
    nb_rows = len(df_releve_pdf)
    nb_columns = len(df_releve_pdf.columns)
    index_headers = []
    ### la liste de différentes tables pouvant exister dans le relevé. 
    #### On les flag pour garder ou supprimer après (car le format de ces tables est complètement différent d'une table à l'autre)
    list_valeurs_a_flagger = ['Date','Date de départ','Compte à terme','Dépôt à terme','Opposition Trésor Public'] 
    for row in range(0,nb_rows):
        for column in range(0,nb_columns):
            if df_releve_pdf.loc[row,column] in list_valeurs_a_flagger : 
                index_headers.append(row)
    ############## splitter la dataframe en dictionnaire pour éviter les colonnes décalées
    filename = filename.replace(".xlsx","")
    list_entetes = ["Date","Référence","Débit","Crédit","Valeur"]
    for idx_list,idx_df in enumerate(index_headers):
        row_values = list(df_releve_pdf.loc[idx_df])
        if all(colonne in row_values for colonne in list_entetes): #### Si la table contient bien des entetes souhaitées, sinon on la prend pas
            inf = idx_df #### le bord inférieur = première ligne à couper, sup = dernière ligne à couper
            if idx_df == index_headers[-1]: sup = len(df_releve_pdf) ### derniere valeur de la liste, il faut prendre jusqu'à la fin de la dataframe
            else : sup = index_headers[idx_list+1]
            dict_files[filename + "_" + str(idx_df)] = df_releve_pdf.loc[inf:sup-1,:]
    return dict_files


def clean_CLientOps(df_ClientOps):
    '''Clean les données de ClientOps
    '''
    df_ClientOps["Référence de l'ordre_origin"] = df_ClientOps["Référence de l'ordre"]
    df_ClientOps["Référence de l'ordre"] = df_ClientOps["Référence de l'ordre"].apply(clean_reference_ordre) # enlever les données qui ne sont pas l'ordre name et splitter les ordres au format List (le cas un vir pour plusieurs ordres) 
    df_ClientOps["Note"] = df_ClientOps["Référence de l'ordre_origin"].apply(note_TP) # Chercher Trop perçu
    df_ClientOps_clean = df_ClientOps.explode("Référence de l'ordre")
    df_ClientOps_clean = df_ClientOps_clean[~df_ClientOps_clean["Référence de l'ordre"].isnull()]
    return df_ClientOps_clean



