import datetime

import oracledb
import pandas as pd

dictionary = {"A": "annullati", "S": "scaduti", "C": "da_compilare", "P": "parzialmente_firmati",
              "D": "da_firmare", "F": "firmati"}

months_dict = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"}

current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# DB parameter
user = "UFAFIEAVA_OWN"
password = "@c1cc10P45t1cc10#"
dsn = "169.51.51.164:31521/ORCLCDB.localdomain"


def connection_up():
    try:
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        print("Connessione al database avvenuta con successo.")
        return conn
    except oracledb.Error as e:
        print(f"Errore durante la connessione al database: {e}")


def execute_query(query):
    conn = connection_up()
    df = pd.read_sql(query, conn)
    if 'MONTH' in df.columns:
        df['MONTH'] = df['MONTH'].map(months_dict)
    conn.close()
    return df


def count_documents():
    return "select count(*) as documenti from TFA1005_DOCUMENTO"


def count_signed_documents():
    return "select count(*) as firmati from TFA1005_DOCUMENTO d where d.COD_STA_DOC='F'"


def count_to_sign_documents():
    return "select count(*) as da_firmare from TFA1005_DOCUMENTO d where d.COD_STA_DOC='D'"


def count_partial_signed_documents():
    return "select count(*) as parzialmente_firmati from TFA1005_DOCUMENTO d where d.COD_STA_DOC='P'"


def count_annulled_documents():
    return "select count(*) as annullati from TFA1005_DOCUMENTO d where d.COD_STA_DOC='A'"


def count_to_compile_documents():
    return "select count(*) as da_compilare from TFA1005_DOCUMENTO d where d.COD_STA_DOC='C'"


def count_expired_documents():
    current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return ("select count(*) as scaduti from TFA1005_DOCUMENTO d where d.COD_STA_DOC in ('P','C','D') and "
            f"d.DAT_SCA_DOC < TO_DATE('{current_date}','YYYY-MM-DD HH24:MI:SS')")

def uploaded_from_stato():
    return (
        "select client, year, "
        "coalesce(max(case when stato='F' then uploaded end), 0) as Firmati, "
        "coalesce(max(case when stato='P' then uploaded end), 0) as Parzialmente_firmati, "
        "coalesce(max(case when stato='D' then uploaded end), 0) as Da_firmare, "
        "coalesce(max(case when stato='A' then uploaded end), 0) as Annullati, "
        "coalesce(max(case when stato='C' then uploaded end), 0) as Da_compilare, "
        "coalesce(max(case when (stato in ('P','C','D') and "
        f"data_scadenza < TO_DATE('{current_date}','YYYY-MM-DD HH24:MI:SS')) then uploaded end), 0) as Scaduti, "
        "SUM(uploaded) AS Totale "
        "from ("
        "select d.COD_PGM_ULT_MOV as client, extract(YEAR from d.DAT_UPD_DOC) as year, "
        "d.COD_STA_DOC as stato, d.DAT_SCA_DOC as data_scadenza, "
        "count(*) as uploaded from TFA1005_DOCUMENTO d "
        "group by d.COD_PGM_ULT_MOV, extract(YEAR from d.DAT_UPD_DOC), d.COD_STA_DOC, d.DAT_SCA_DOC "
        ") "
        "group by client, year "
        "order by year, client")



def separate_counted_from_year(state):
    if (state != "S"):
        return (
            f"select extract(YEAR from d.DAT_UPD_DOC) as year, count(*) as {dictionary.get(state)} from TFA1005_DOCUMENTO d "
            f"where d.COD_STA_DOC='{state}' group by extract(YEAR from d.DAT_UPD_DOC) order by year")
    else:
        current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return (
            f"select extract(YEAR from d.DAT_UPD_DOC) as year, count(*) as {dictionary.get(state)} from TFA1005_DOCUMENTO d where "
            f"d.COD_STA_DOC in ('P','C','D') and d.DAT_SCA_DOC < TO_DATE('{current_date}','YYYY-MM-DD HH24:MI:SS') "
            "group by extract(YEAR from d.DAT_UPD_DOC) order by year")


def separate_counted_from_month(state):
    if (state != "S"):
        return (
            "select extract(YEAR FROM d.DAT_UPD_DOC) AS year, "
            "extract(MONTH FROM d.DAT_UPD_DOC) AS month, "
            f"count(*) as {dictionary.get(state)} from TFA1005_DOCUMENTO d "
            f"where d.COD_STA_DOC='{state}' "
            "group by extract(YEAR from d.DAT_UPD_DOC), extract(MONTH from d.DAT_UPD_DOC) "
            "order by year, month")
    else:
        return (
            "select extract(YEAR from d.DAT_UPD_DOC) as year, "
            "extract(MONTH FROM d.DAT_UPD_DOC) AS month, "
            f"count(*) as {dictionary.get(state)} from TFA1005_DOCUMENTO d where "
            f"d.COD_STA_DOC in ('P','C','D') and d.DAT_SCA_DOC < TO_DATE('{current_date}','YYYY-MM-DD HH24:MI:SS') "
            "group by extract(YEAR from d.DAT_UPD_DOC), extract(MONTH from d.DAT_UPD_DOC), d.COD_PGM_ULT_MOV "
            "order by year, month")


def find_clients():
    return ("select d.COD_PGM_ULT_MOV as client FROM TFA1005_DOCUMENTO d "
            "group by d.COD_PGM_ULT_MOV order by client")

def uploaded_from_clients():
    return (
        "select client, year, "
        "coalesce(max(case when month=1 then uploaded end), 0) as Gennaio, "
        "coalesce(max(case when month=2 then uploaded end), 0) as Febbraio, "
        "coalesce(max(case when month=3 then uploaded end), 0) as Marzo, "
        "coalesce(max(case when month=4 then uploaded end), 0) as Aprile, "
        "coalesce(max(case when month=5 then uploaded end), 0) as Maggio, "
        "coalesce(max(case when month=6 then uploaded end), 0) as Giugno, "
        "coalesce(max(case when month=7 then uploaded end), 0) as Luglio, "
        "coalesce(max(case when month=8 then uploaded end), 0) as Agosto, "
        "coalesce(max(case when month=9 then uploaded end), 0) as Settembre, "
        "coalesce(max(case when month=10 then uploaded end), 0) as Ottobre, "
        "coalesce(max(case when month=11 then uploaded end), 0) as Novembre, "
        "coalesce(max(case when month=12 then uploaded end), 0) as Dicembre, "
        "SUM(uploaded) AS Totale "
        "from ("
        "select d.COD_PGM_ULT_MOV as client, extract(YEAR from d.DAT_UPD_DOC) as year, "
        "extract(MONTH from d.DAT_UPD_DOC) as month, "
        "count(*) as uploaded from TFA1005_DOCUMENTO d "
        "group by d.COD_PGM_ULT_MOV, extract(YEAR from d.DAT_UPD_DOC), extract(MONTH from d.DAT_UPD_DOC) "
        ") "
        "group by client, year "
        "order by year, client")


def uploaded_from_client_and_state_in_year(state, client):
    if (state != "S"):
        return (
            f"select d.COD_PGM_ULT_MOV, extract(YEAR from d.DAT_UPD_DOC) as year, count(*) as '{dictionary.get(state)}' from TFA1005_DOCUMENTO d "
            f"where d.COD_STA_DOC = {state} d.COD_PGM_ULT_MOV = '{client}' "
            f"group by extract(YEAR from d.DAT_UPD_DOC) order by year")
    else:
        return (
            f"select d.COD_PGM_ULT_MOV, extract(YEAR from d.DAT_UPD_DOC) as year, count(*) as {dictionary.get(state)} from TFA1005_DOCUMENTO d where "
            f"d.COD_STA_DOC in ('P','C','D') and d.DAT_SCA_DOC < TO_DATE('{current_date}','YYYY-MM-DD HH24:MI:SS') "
            f"d.COD_PGM_ULT_MOV = '{client}' "
            "group by extract(YEAR from d.DAT_UPD_DOC) order by year")


def uploaded_from_client_and_state_in_month(state, client):
    if (state != "S"):
        return (
            "select d.COD_PGM_ULT_MOV as client, extract(YEAR FROM d.DAT_UPD_DOC) AS year, "
            "extract(MONTH FROM d.DAT_UPD_DOC) AS month, "
            f"count(*) as {dictionary.get(state)} from TFA1005_DOCUMENTO d "
            f"where d.COD_STA_DOC = '{state}' and d.COD_PGM_ULT_MOV = '{client}' "
            "group by d.COD_PGM_ULT_MOV, extract(YEAR from d.DAT_UPD_DOC), extract(MONTH from d.DAT_UPD_DOC) "
            "order by year, month")
    else:
        return (
            "select d.COD_PGM_ULT_MOV, extract(YEAR from d.DAT_UPD_DOC) as year, "
            "extract(MONTH FROM d.DAT_UPD_DOC) AS month, "
            f"count(*) as {dictionary.get(state)} from TFA1005_DOCUMENTO d where "
            f"d.COD_STA_DOC in ('P','C','D') and d.DAT_SCA_DOC < TO_DATE('{current_date}','YYYY-MM-DD HH24:MI:SS') "
            f"and d.COD_PGM_ULT_MOV = '{client}' "
            "group by d.COD_PGM_ULT_MOV, extract(YEAR from d.DAT_UPD_DOC), extract(MONTH from d.DAT_UPD_DOC) "
            "order by year, month")


def execute_all_queries():
    queries_count_state = [count_signed_documents(),
                           count_partial_signed_documents(),
                           count_to_compile_documents(),
                           count_annulled_documents(),
                           count_expired_documents(),
                           count_to_sign_documents()]

    queries_count_time_for_states = []
    for state in dictionary.keys():
        queries_count_time_for_states.append(separate_counted_from_year(state))
        queries_count_time_for_states.append(separate_counted_from_month(state))

    queries_count_upload_for_client = []
    queries_count_upload_for_client.append(uploaded_from_clients())

    dataframes = []
    try:
        for query in queries_count_state:
            dataframes.append(execute_query(query))
        for query in queries_count_time_for_states:
            dataframes.append(execute_query(query))
        for query in queries_count_upload_for_client:
            dataframes.append(execute_query(query))

        return dataframes

    except Exception as e:
        print(f"Errore durante la connessione al database: {e}")
        return None
