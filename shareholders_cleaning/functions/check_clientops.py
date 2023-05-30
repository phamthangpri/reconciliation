
import datetime as dt
from fuzzywuzzy import fuzz, process # Fuzzy match
from  itertools import product
import pandas as pd
import sqlite3
from functions.utils import *

def fuzz_score(x,y,bol_partial_score):
    '''Calculer le fuzzy matching score entre x et y avec l'option partial_score True/False
    '''
    x = str(x)
    y = str(y)
    if len(x)>0 and len(y) > 0 : 
        token_score = fuzz.token_sort_ratio(x,y)
        full_score = fuzz.ratio(x,y)
        if bol_partial_score:
            partial_score = fuzz.partial_ratio(x,y)
        else:
            partial_score = 0
        max_score = max(token_score,full_score,partial_score)
    else : max_score = 0
    return max_score

def check_with_BO(df_BO,df_virs_a_vérifier,df_ClientOps):
    '''Merger le relevé avec données de ClientOps pour récupérer l'ordre et puis rapprocher avec BO pour récupérer les données de BO
    '''
    df_BO_a_traiter = df_BO[df_BO["Id sys"].isnull()].drop(columns="Id sys")
    df_check = df_virs_a_vérifier.merge(df_ClientOps, on="Id sys",how="outer",indicator=True)
    df_check = df_check[df_check["_merge"]!="right_only"].drop(columns="_merge")
    df_check = df_check.merge(df_BO_a_traiter,left_on = ["Référence de l'ordre"],right_on=["Subscription_Order_Name_BOClientOps"],how="outer",indicator=True)
    df_check = df_check[df_check["_merge"]!="right_only"].drop(columns="_merge")
    return df_check

def fill_BO(df_match,df_BO_copy):
    '''Remplir le Id sys dans BO après chaque rapprochement afin de ne pas réutilier l'ordre
    '''
    df_BO_checked = df_BO_copy[~df_BO_copy["Id sys"].isnull()]
    df_BO_a_traiter = df_BO_copy[df_BO_copy["Id sys"].isnull()].drop(columns="Id sys")
    df_ord_matched = df_match.groupby(by="Référence de l'ordre").agg({"Id sys":"|".join}).reset_index()
    df_BO_a_traiter = df_BO_a_traiter.merge(df_ord_matched,\
                                    left_on = 'Subscription_Order_Name_BOClientOps',\
                                    right_on="Référence de l'ordre",how="outer", indicator = True)
    df_BO_a_traiter = df_BO_a_traiter[df_BO_a_traiter["_merge"]!="right_only"].drop(columns=["Référence de l'ordre","_merge"])
    df_BO_copy = pd.concat([df_BO_checked,df_BO_a_traiter])
    return df_BO_copy

def check_Nom_Montant_Date(bol_nom,bol_montant,bol_date,bol_nom_partiel,motif_check):
    '''Ajouter la notation en cas de l'audit sur le résultat de ClientOps
    '''
    if bol_nom and bol_montant and bol_date :
        if bol_nom_partiel: result = motif_check + "_OK_nom mappé partiellement"
        else: result = motif_check + "_OK"
    else:
        if bol_nom and bol_montant:
            result = motif_check + "_Plus de 30 jours entre Date BO et Date Releve"
        elif bol_nom_partiel:
            if bol_montant and bol_date:
                result  = motif_check + "_Montant et date ok, à vérifier le nom du client"
            else :result = "N/A"
        else : result = "N/A"
    return result

def check_date(date_souscription,date_virs):
    '''distinguer entre Best case, Erreur ou Rattrapage
    '''
    if date_souscription < date_virs:
        commentaire = "Argent reçu après la création des parts"
    elif date_virs.strftime("%Y-%m") == date_souscription.strftime("%Y-%m"):
        commentaire = "Best case"
    else : commentaire = "Rattrapage"
    return commentaire

def check_montant(montant_bo,montant_virs):
    '''Check Moins Perçu ou Trop perçu
    '''
    if abs(montant_bo - montant_virs) <= 0.01:
        result = "Best case"
    elif montant_bo - montant_virs > 0.01:
        result = "MP"
    else:result="TP"
    return result

def check_meme_compte(fonds,product):
    product = str(product)
    if product != 'nan':
        if product == fonds: return True
        return False
    return np.nan
def check_pls_lignes(commentaire):
    if "Plusieurs virs pour un ordre" in commentaire or "Plusieurs virements pour 1 ordre" in commentaire:
        resultat = "Plusieurs virs pour un ordre"
    elif "Un vir pour pls ordres" in commentaire:
        resultat = "Un vir pour pls ordres"
    else: resultat = "virement unique"
    return resultat 
def check_overall(check_date,check_montant,meme_compte,pls_lignes):
    if check_date == "Best case" and check_montant == "Best case" and meme_compte == True and pls_lignes == "virement unique" : # Best case
        check_overall = "Best case"
    elif check_date == "Best case" and check_montant == "Best case" and pls_lignes == "virement unique": # Mauvais compte
        check_overall = "Mauvais compte"
    elif meme_compte == True and check_montant == "Best case" and pls_lignes == "virement unique": # Erreur ou Rattrapge
        check_overall = check_date
    elif meme_compte == True and check_date == "Best case" and pls_lignes == "virement unique" : # MP ou TP
        check_overall = check_montant
    elif check_date == "Best case" and check_montant == "Best case" and meme_compte == True : # Pls virements pour un ordre / 1 vir pour pls ordres
        check_overall = pls_lignes
    else: check_overall = "Mixte"
    return check_overall

def add_comment_script(bol_script_vs_clientops,verification_clientops_bo):
    '''Pour les rapprochements automatiques par script, il y a un check entre le résultat du script vs résultat de ClientOps. Cette fonction est juste ajouter ce commentaire dans le commentaire global.
    '''
    if bol_script_vs_clientops :
        return verification_clientops_bo + "_OK, même résultat avec ClientOps"
    return verification_clientops_bo + "_Different entre Script vs ClientOps"