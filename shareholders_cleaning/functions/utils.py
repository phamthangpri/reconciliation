import numpy as np
import pandas as pd
import os
import re
# lire fichier excel avec password
import io
import msoffcrypto
from bs4 import BeautifulSoup

df_motsprasites = pd.read_excel("../_config/mots_parasites.xlsx",sheet_name="Sheet1",header=None)
pattern_clean_motif = "|".join(list(df_motsprasites[0])) ### Créer le pattern de regex pour cleanner le motif

def read_list_xlsx(dir_releve_folder,dossier):
    '''Chercher l'ensemble des fichiers excel d'un dossier et sortir un dictionnaire = { filename : df_releve }
    '''
    dir_releve_fond = dir_releve_folder + dossier
    arr = os.listdir(dir_releve_fond)
    list_files = [x for x in arr if ".xlsx" in x]
    dict_files = {}
    for file in list_files:
        df_releve_pdf = pd.read_excel(dir_releve_fond + "/" + file, sheet_name = "Table 1", header=None)
        dict_files[file] = df_releve_pdf
    return dict_files

def read_xlsm(path,fond,passwd ):
    '''Lire fichier xlsm avec mot de passe (fichier ClientOps)
    '''
    decrypted_workbook = io.BytesIO()
    with open(path, 'rb') as file:
        office_file = msoffcrypto.OfficeFile(file)
        office_file.load_key(password=passwd)
        office_file.decrypt(decrypted_workbook)
    dict_ClientOps = pd.read_excel(decrypted_workbook, sheet_name=['A traiter - ' + fond,"Traités - " + fond])
    return dict_ClientOps

def concat_tables_clientOps(dict_to_concat):
    '''Concaténer les 2 onglets A traiter et Traités dans les fichiers ClientOps
    '''
    list_keys = list(dict_to_concat.keys())
    df_1 = dict_to_concat[list_keys[0]] # Onglet A traiter
    df_2 = dict_to_concat[list_keys[1]] # Onglet Traités
    df_concat = pd.concat([df_1,df_2])
    # df_concat = df_concat[["Id Sys","Référence de l'ordre"]]
    df_concat = df_concat.rename(columns={"Id Sys":"Id sys"}).reset_index(drop=True)
    df_concat.loc[:,"Id sys"] = df_concat.loc[:,"Id sys"].astype(str)
    return df_concat   

def get_headers(df_releve_pdf,bol_spec_file = False,bol_bred=False):
    '''Chercher les entêtes dans les fichiers Excels convetis (car les êntetes sont décalées d'un fichier à l'autre)
    bol_spec_file : si le fichier est un fichier particulier : fichier converti via Word (par exemple : des fichiers relevé XL entre 2020 et 2021). Ce fichier est
    assez propre, il n'y a que 5 colonnes, donc on peut forcer le nom des colonnes. Les fichiers convertis directement ne sont pas toujours le cas.
    '''
    # Prendre des lignes qui contiennent headers

    if bol_spec_file == True:
        list_columns_org = list(df_releve_pdf.columns)
        list_new_name = ["DATE COMPTABLE","NATURE DES OPERATIONS","DATE DE VALEUR","DEBIT","CREDIT"]
        dict_column = dict(zip(list_columns_org,list_new_name))

    else : 
        nb_rows = len(df_releve_pdf)
        nb_columns = len(df_releve_pdf.columns)
        list_headers = []
        for i in range(0,nb_rows):
            nb_not_nan = nb_columns - df_releve_pdf.iloc[i,:].isna().sum()
            if nb_not_nan >=5: # prendre seulement des lignes qui ont au moins 5 valeurs d'entêtes (car il y a des décalages, des libellés qui contiennent seulement le mot CREDIT...)
                list_headers.append(i)
        df_headers = df_releve_pdf.iloc[list_headers,:]
        
        # Vérifier le header
        columns_original = df_headers.columns
        if bol_bred == False:  columns_bank = ["DATE COMPTABLE","NATURE DES OPERATIONS","DATE DE VALEUR","DEBIT","CREDIT"]
        else : columns_bank = ["Date","Référence","Débit","Crédit","Valeur"]
        dict_column = {}
        for column in columns_original:
            list_value = list(df_headers[column])
            for column_bk in columns_bank:
                if column_bk in list_value:
                    dict_column[column] = column_bk
    return dict_column
def exclure_lignes(libelle,debit,credit):
    '''Cette fonctione est pour identifier les lignes à supprimer dans le relevé (les lignes inutiles)
    '''
    if str(libelle) == "NAN" and str(debit) == "NAN" and str(credit)== "NAN":
        resultat = True
    elif str(debit) == "DEBIT" :
        resultat = True
    else: resultat = False
    return resultat

def clean_reference_ordre(text_to_clean):
    '''Nettoyr les référence de l'ordre remplies par l'équipe ClientOps : supprimer les caractères non nécessaires
    '''
    text_to_clean = str(text_to_clean).upper()
    text_to_clean = text_to_clean.replace("ODR", "ORD").replace("ORS","ORD").replace("ORDI","ORD").replace("OD","ORD")#.replace(" ","")

    patern_ord = r"\d+"
    list_ord = []
    if text_to_clean: #si la valeur n'est pas vide
        if re.search(patern_ord,text_to_clean): # si elle contient des chiffres
            new_values = re.split(r'\s+|\+|\,|\/|\\|\&|\;',text_to_clean) # splitter les ordres
            for value in new_values:
                if len(value) >=5 and re.search(r'\d+',value): # prendre seulement des texts qui contiennent le n° ordre
                    value = value.upper()
                    if not "ORD" in value:
                        value = "ORD-" + value
                    if "ORD" in value: 
                        if "-" not in value: value = value.replace("ORD","ORD-")
                        value = re.findall(r'(ORD\-[A-Z\d\-]+)',value) # get only order number
                        if value:
                            value = value[0]
                            value = value.replace("--","-")
                            if len(value)<=23: list_ord.append(value)
    if len(list_ord) == 0: list_ord = np.nan
    else : list_ord = list(set(list_ord)) # supprimer les doublons (erreur de coller)
    return list_ord

def note_TP(text_ref):
    '''Chercher le commentaire TP = trop perçu dans le fichier ClientOps
    '''
    text_ref = str(text_ref)
    if re.search(r'\bTP\b',text_ref):
        note = "TP"
    elif re.search(r'\bMP\b',text_ref):
        note = "MP"
    else: note=np.nan
    return note 

def control_script(df_control_script, etape,nb_colonnes,montant_total):
    row_id = len(df_control_script)
    df_control_script.loc[row_id+1] = [etape,nb_colonnes,montant_total]
    return df_control_script

def verification_doublons(df_virements,liste_colonnes_trier,colonne_date,id_column):
    '''Cette fonction permet de vérifier les virements de la même personne pour le même montant
    '''
    df_virements = df_virements[~df_virements["Titulaire_clean"].isnull()] 
    df_virements["Titulaire_clean"] = df_virements["Titulaire_clean"].astype(str)
    df_virements = df_virements[~df_virements["Titulaire_clean"].str.contains("BANQUE")] 

    df_virements = df_virements.sort_values(by=liste_colonnes_trier + [colonne_date]) #Trier la data
    df_virements["diff_days"] = df_virements.groupby(by=liste_colonnes_trier)[colonne_date].diff()\
                                .astype('timedelta64[D]') #calculer le nb jours de différence par rapport à la date précédente
    df_vir_doublons = df_virements[~df_virements["diff_days"].isnull()]\
                .sort_values(by=liste_colonnes_trier + [colonne_date]) # filrer sur les doublons
    df_vir_doublons = df_vir_doublons[df_vir_doublons["diff_days"]<=30][liste_colonnes_trier] # filtrer sur les doublons > 30jours
    df_vir_doublons = df_vir_doublons.merge(df_virements, on = liste_colonnes_trier, how="inner") # join pour récupérer la date 0
    df_vir_doublons["diff_days"] = df_vir_doublons["diff_days"].fillna(0)
    df_vir_doublons = df_vir_doublons.drop_duplicates(subset=id_column)
    return df_vir_doublons


def clean_name(text):
    list_to_replace_name = [# Monsieur ou Madame 
                                 r'(\bM\.OU MME\s+)',r'(\bMRMME)',r'(\bM\.M\s+)',r'(\bM OU MME\s+)',r'(\bMOU ME\s+)',r'(\bMOU MME\s+)', r'(\bMR ET MME\s+)',r'(\bM\. et MME\s+)', r'(\bM\.OUMME\s+)', r'(\bM\+MME\s+)',r'(\bOUMR\s+)',\
                                 # Monsieur
                                r'(\bM\s+)',r'(\s+M\b)',r'(\bDR.\s+)',r'(\bDR\s+)', r'(\bM\.)',r'(\bM\s+)',r'(\bM.\s+)', r'(\bMR\.\s+)',r'(\bMR\s+)',r'(\s+MR\b)',r'(\bMONSIEUR\s+)', r'(\bM\.OU\s+)', r'(\bSR\s+)',r'(\bSIR\s+)',\
                                # Madame
                                 r'(\bMME\s+)',r'(\s+MME\b)', r'(\bMADAME\s+)', r'(\bOU MME\s+)', r'(\bET MME\s+)',r'(\bE SRA\s+)',r'(\bMRS\s+)',\
                                # Mademoiselle
                                 r'(\bML\s+)', r'(\bMLLE\s+)',r'(\bMLE\s+)', r'(\bMELLE\s+)',r'(\bMISS\s+)',  \
                                 r'(\bOU\s+)',r'(\bET\s+)',r'(\bET\.\s+)'
                                 ]  
    text = str(text).upper()
    if text == np.nan or text == "NAN": return np.nan
    for item in list_to_replace_name:
        text = re.sub(item, "",text)
    text = re.sub(r' +', " ",text) # Supprimer double espace
    text = re.sub(r'\.', "",text).strip()
    return text

# def clean_motif(df_to_clean,column_to_clean, output_column_name):
#     '''Cette fonction permet de nettoyer motif par regex 
#     Elle va supprimer tous les chiffres, les mots récurrents, les caractères spéciaux'''
    
#     str_regex = r'\.|\-|\d+|\bEUR\b|\bA\b|ACHAT(S?)|ACTIF(S?)\b|ACQUISITION(S?)|\bACTION(S?)|ADHERENT|\bAN(S?)\b|\bAPPORT|\bAPPROV|\bASS\b|ASSOCIE(E?)|\bAVEC\b|\bAUTRE(S?)\b|BANQUE|BON POUR ACHAT|\bCC\b|\bCFF\b|CHQ|CHEQUE|CEPAFR|\bCOMPL\b|COMPLEMENT\b|COMPLEMENTAIRE|COMPLEME\b|COMPLEMENT SOUSCRIPTION|\bCPI\b|\bCPT\b|\bCON\b|CONTRAT|CONVI\b|CONVIC\b|CONVICT\b|\bCONVICTIO\b|CONVICTION(S?)|\bCONV\b|\bCOR\b|CORU(M?)\b|CORUMX|CORUMEURION|CTS|DANS LE|DE LA|\bDE FRANCE\b|DEMANDE|\bDES\b|DEBLOCAGE|\bDELA\b|DEMEM\b|DEMEMBR\b|DEMEMBRE(E?)(S?)\b|DEMEMBREMENT|DMEMBR\b|DEPOT DE GARANTIE|DEPUIS\b|DEUXIEME|DIVID\b|DIVIDENDE(S?)|DOSSIER|DU\b|EMAIL|E-MAIL|\bEN\b|EUODIA|EURION|EURRIO|\bEURO(S?)|FCPI|FOND(S?)|FORUM|IMMOBILIER|INT\b|INTERNET|INV\b|INVEST\b|INVESTISSEMENT|INVESISSEMENT|INVESTISSEM\b|INVESTMENT|MISE A DISPOSITION|MOBILE|MENSUEL\b|MONTANT|NONREF|NOT(\s?)PROVIDED|\bNP\b|\bNUE PRO(P?)\b|NU(E?) PROPRIETE|NUE ROPRIETE|NUPRO|NU PROP|\bNUP\b|NU PROE\b|OPERATION|\bORIGI\b|ORIGIN(E?)|PAR(S?)\b|\bPART(S?)|PARTEURION|PARTICIPATION|PASSEPORT VERT|PAS DE MOTIF|PLACEMENT|POUR\b|POURACH|PRET|PRÊT|\bPP\b|\bPPT\b|\bPROD\b|REGLEMENTS|RNF|RECU|REGUL\b|REFERENCE|REGULARISATION|REF\b|REJET\b|REMIS(E?)|RETOUR|REMBOURSEMENT|SCPI|SEPA|SOLDE\b|SOIT\b|SCOUSCRIPTION|SOSUCRIPTION|SOSCRIPTION|SOUSCIRPTION|SOUSCRIPTION|SOUSCRIPTEUR|\bSOUSCRIPT\b|SOUSCRIPTIO\b|SOUS(C?)\b|\bSOUSCR\b|\bSOUSCRI\b|SOUSCRIP\b|\bSOUCRIPTION\b|SOUSRIPTION|\bSUR\b|SUITE\b|PR\b|TRANSATLANTIQUE|TRANSFERT|\bTIONS\b|\bUN(E?)\b|\bUS\b|USUFRU\b|USUFRUI(T?)|\bUSU\b|UF\b|UFT|\bVERS\b|VERSEMENT|VIREMENT|VIRMENT|VIRMEMENT|VRT|VIRT|XL|XXXX|\bVIR\b|\bVIRE\b'
    

#     df_to_clean[output_column_name] = df_to_clean[column_to_clean].str.upper().replace(str_regex, "", regex=True)
#     df_to_clean[output_column_name] = df_to_clean[output_column_name].replace(r' +', " ", regex=True).str.strip() # Supprimer double espace
#     return df_to_clean

def clean_motif(text):
    '''Cette fonction ressemble à la fonction clean_motif, sauf que clean_motif est déjà utilisé par pleine d'autres fonctions, donc cette fonction est juste un complément pour ne pas purtuber les fonctions déjà en place'''
    list_replace = ["1\/1","1\/LLE","1\/LME","1\/LONSIEUR","1\/LR"]
    text = str(text)
    if text == np.nan or text == "": return text
    for item in list_replace:
        text = text.replace(item,"")
    text = re.sub(r'\/'," ",text)
    resultat = re.sub(pattern_clean_motif,"",text) ### pattern_clean_motif = liste des mots parasites qui est créée tout en haut
    resultat = resultat.strip()
    resultat = ' '.join(w for w in resultat.split(" ") if len(w) > 1)
    return resultat

def get_words_only(text):
    '''Extraire uniquement les mots (sans caractères spéciaux, ni des mots avec des chiffres) afin de récupérer le nom, prénom
    '''
    if text:
        text = str(text)
        patern_text = r'(\b[^\W\d]+\b)'
        list_result = re.findall(patern_text, text)
        if len(list_result)>0:
            return " ".join(list_result)

def remove_de(text):
    '''A la fin des motifs nettoyés, il y a souvent des mots DE (qui vient de Souscription de corum,...etc). Cette fonction va supprimer uniquement les DE à la fin du nom
    Les DE au début sont gardés (pour les noms de famille DE quelque choses)
    '''
    if text:
        text = str(text)
        if text[:3] == "ET ":
            return text[3:]
        if text[-3:] in [" DE"," DU", " ET", " AU", " LA", " LE"]:
            return text[:-3]
        if text in ["DE", "DU", "ET","AU","LA", "LE"]:
            return np.nan
    return text

def remove_duplicated(text):
    ''''Pour supprimer les noms et prénoms en doublons dans les motifs
    '''
    text = str(text)
    if text == np.nan or text == "" or text == "NAN": return text
    text_list = text.split()
    text_list = sorted(set(text_list), key = text_list.index)
    return " ".join(text_list)

def controle_data_quality(df_releve,fonds):
    df_releve["Periode"] = df_releve["Date"].dt.strftime("%Y-%m")
    df_releve["Annee"] = df_releve["Date"].dt.strftime("%Y")
    df_control_releve = df_releve.groupby(["Annee","Periode"]).agg({\
                                        "Libellé":"count",
                                        "Crédit":"sum",
                                        "Débit":"sum"}).reset_index()
    df_control_releve["Fonds"] = fonds
    return df_control_releve
def merge_with_duplicates(df_vir_doublons,df_ClientOps_doublons,colonne_date):
    '''Merger entre les doublons dans le relevé vs doublons dans ClientOps (données avant 2021):
    Il n'y a pas d'Id sys commun entre 2 bases. Si un client fait 4 virements avec le même montant dans la même journée, le merge normal va créer 16 lignes
    Cette fonction permet de mapper ligne par ligne et puis éliminer cette ligne dans les 2 bases pour continuer à rapprocher.
    '''
    df_match_concat = pd.DataFrame()
    for i in range(0,5): # hypothèse: 5 virements max pour le même montant et le même client par jour
        df1_i = df_vir_doublons.drop_duplicates(subset=["Valeur","Crédit","Titulaire_clean"],keep='first')
        df_vir_doublons = df_vir_doublons[~df_vir_doublons["Id sys"].isin(df1_i["Id sys"])]
        df2_i = df_ClientOps_doublons.drop_duplicates(subset=[colonne_date,"Crédit_ClientOps","Titulaire_clean"],keep='first')
        df_ClientOps_doublons = df_ClientOps_doublons[~df_ClientOps_doublons["Id sys_ClientOps"].isin(df2_i["Id sys_ClientOps"])]
        df_match = df2_i.merge(df1_i,left_on=[colonne_date,"Crédit_ClientOps","Titulaire_clean"],\
                                right_on=["Valeur","Crédit","Titulaire_clean"]
                               ) 
        df_match_concat = pd.concat([df_match_concat,df_match])
    return df_match_concat

def get_type_transaction(libelle):
    libelle = libelle[0:25]
    if bool(re.search(r'REJET',libelle)):
        type_transaction = "REJET"
    elif bool(re.search(r'REMISE|CHEQUE',libelle)):
        type_transaction = "REMISE CHEQUE"
    elif bool(re.search(r'\bPRLV\b|PRELEVEMENT|PR V SEPA',libelle)):
        type_transaction = "PRLV"
    elif bool(re.search(r'VIR(T?) CPTE A CPTE',libelle)):
        type_transaction = "VIRT CPTE A CPTE"
    elif bool(re.search(r'\bVIR\b|VIREMENT|IR TRESO|V R SEPA|V R ETRANGER|V R TRESO|V REMENT SEPA',libelle)):
        type_transaction = "VIREMENT"
    else : type_transaction = "Autre"
    return type_transaction

def get_product_motif(remi,eref=np.nan):
    remi = str(remi)
    eref = str(eref)
    
    patern_xl = r'\bXL|CORUMXL'
    patern_eu = r'EURION'
    patern_cc = r'ORIGIN\b|\bCC\b'
    if re.search(patern_xl,remi):
        result = "XL"
    elif re.search(patern_xl,eref):
        result = "XL"
    elif re.search(patern_eu,remi):
        result = "EU"
    elif re.search(patern_eu,eref):
        result = "EU"
    elif re.search(patern_cc,remi):
        result = "CC"
    elif re.search(patern_cc,eref):
        result = "CC"
    else : result = np.nan
    return result

def prepare_BO_ClientOps(df_BO):
    df_BO = df_BO[["Subscription_Order_Name","ClientName","Amount","SubscriptionDate","Product","NoteBO"]]
    df_BO["NoteBO"] = df_BO["NoteBO"].fillna("N/A")
    df_BO = df_BO.groupby(["Subscription_Order_Name","ClientName","Product"]).agg({\
                                                                                            "Amount":"sum",\
                                                                                            "SubscriptionDate":'min',
                                                                                            "NoteBO":"\n".join}).reset_index() 
    columns_org = df_BO.columns
    if not "_BOClientOps" in columns_org[0]:
        new_columns = [x + "_BOClientOps" for x in columns_org]
        dict_name = dict(zip(columns_org,new_columns))
        df_BO = df_BO.rename(columns=dict_name)
    df_BO["Id sys"] = np.nan
    return df_BO

def read_xml(path_to_xml,folder,filename,encoding='utf-8',bol_pei=False):
    with open(path_to_xml + folder + filename, 'r', encoding=encoding) as f:
        xml_file = f.read()
    bs_data = BeautifulSoup(xml_file, 'xml') # scrapper les données

    #### Récupérer les données selon class ###
    id_remise = bs_data.findAll("MsgId")
    nb_transactions = bs_data.findAll("NbOfTxs")
    montant_total = bs_data.findAll("CtrlSum")
    client_names = bs_data.findAll("Nm")
    ord_names = bs_data.findAll("MndtId")
    date_signs = bs_data.findAll("DtOfSgntr")
    montants = bs_data.findAll("InstdAmt")
    products = bs_data.findAll("EndToEndId")
    

    ### Nettoyer données récupérées ###
    id_remise = id_remise[0].get_text()
    nb_transactions = nb_transactions[0].get_text()
    montant_total = montant_total[0].get_text()
    if bol_pei == True:
        ord_prlv = products
    else:    ord_prlv = products[0].get_text().split()[0]
    

    data = []
    for i in range(0,len(ord_names)):
        rows = [client_names[i+2].get_text(),ord_names[i].get_text(), ## les 2 premiers names dans client_names est CORUM AM à exclure
               date_signs[i].get_text(),montants[i].get_text(),products[i].get_text()[0:2]]
        data.append(rows)

    df_xml_file = pd.DataFrame(data,columns = ['client_name','order_name','date_sign','amount','product'])
    df_xml_file["id_session"] = id_remise
    df_xml_file["nb_transactions"] = nb_transactions
    df_xml_file["total_amount"] = montant_total
    df_xml_file["filename"] = filename
    df_xml_file["ord_prlv"] = ord_prlv

    ### Pour les sessions de PEI, le product est n'est trouvé que sur le filename
    if "SES_Epargne" in filename:
        df_xml_file["product"] = df_xml_file["filename"].apply(lambda x: x.split("_")[2])
    ### Formatting ### 
    df_xml_file["amount"] = df_xml_file["amount"].astype(float)
    df_xml_file["total_amount"] = df_xml_file["total_amount"].astype(float)
    df_xml_file["nb_transactions"] = df_xml_file["nb_transactions"].astype(int)
    df_xml_file["date_sign"] = pd.to_datetime(df_xml_file["date_sign"],format='%Y-%m-%d')
    return df_xml_file

def read_xml_remboursement(path_to_xml,filename,bol_fichier_rachat=False):
    with open(path_to_xml + filename, 'r',encoding='utf-8') as f:
        xml_file = f.read()
    bs_data = BeautifulSoup(xml_file, 'xml') # scrapper les données

    date_transaction = bs_data.findAll("CreDtTm")
    name = bs_data.findAll("Nm")
    montant = bs_data.findAll("InstdAmt")
    iban = bs_data.findAll("IBAN")
    code_compte = bs_data.findAll("PmtInfId")
    if bol_fichier_rachat :
        
        motif_virement = bs_data.findAll("Ustrd")
        li = bs_data.findAll('Othr')
        code_client = [li[i].get_text() for i in range(2,len(li))]

    date_transaction = date_transaction[0].get_text()
    compte = name[0].get_text() 

    df_xml_file = []
    for i in range(0,len(montant)):
        rows = [name[i+2].get_text(),montant[i].get_text(), ## les 2 premiers names dans client_names est CORUM AM à exclure
               iban[i+1].get_text()]
        if bol_fichier_rachat:
            rows += [motif_virement[i].get_text(), code_client[i]]
        df_xml_file.append(rows)
    if bol_fichier_rachat: 
        df_xml_file = pd.DataFrame(df_xml_file,columns = ['Nom Associé','Montant','IBAN',"Motif_virement","Code_client"])
        

    else: df_xml_file = pd.DataFrame(df_xml_file,columns = ['Nom Associé','Montant','IBAN'])
    df_xml_file["code_transac"] = code_compte[0].get_text()
    df_xml_file["Date"] = date_transaction[0:10]
    df_xml_file["Fonds"] = compte.strip()
    df_xml_file["IBAN_Corum"] = iban[0].get_text()
    

    df_xml_file["Montant"] = df_xml_file["Montant"].astype(float)
    df_xml_file["Date"] = pd.to_datetime(df_xml_file["Date"],format = "%Y-%m-%d")
    return df_xml_file