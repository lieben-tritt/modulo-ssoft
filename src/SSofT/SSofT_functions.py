import io
import json
import yaml
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import gspread

from google.cloud import storage

parent_parent = os.path.abspath(__file__ + "../../../")
current_dir = os.path.dirname(os.path.abspath(__file__))

def get_config(json_file=None, abspath=True, relpath=None):

    cnf = current_dir + '/SSofT_config.json'
    with open(cnf, encoding='utf-8') as conf:
        config = json.load(conf)

    return config


def get_credential(credential):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential

def do_data_ssoft(df):
    """ Insere um campo no dataframe com a data agora
    """        
    df['data_ssoft'] = datetime.now()

def dir_base():
    return parent_parent +'/'

def get_config_yaml(_file=None, abspath=True, relpath=None):
    
    
    if _file:
        if abspath is True:
            cnf = os.path.abspath(_file)
        else:
            cnf = _file
    else:
        cnf = current_dir + '/SSofT_config.yaml'
        #cnf = Path('config/config.json')

    with open(cnf, encoding='utf-8') as conf:
        config = yaml.safe_load(conf)

    return config

#!=============================================================================
#config = get_config()
config = get_config_yaml()
#!=============================================================================

def get_str_agora(data=False):
    
    if data is True:
        ret = datetime.now()
    else:
        ret = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return ret

def do_log(txt, dtini=None, dtfim=None):

    concat = f'{get_str_agora()} | --> {txt}'

    if (dtini and dtfim):

        diff_seconds = round((dtfim-dtini).total_seconds(),2)
        diff_fmt = str(timedelta(seconds=diff_seconds))

        concat = f'{concat} - duração: {diff_fmt}'

    return (
        logging.info(concat),
        print(concat)
    )

def diff_datas(dtfim, dtini):

    diff_seconds = round((dtfim-dtini).total_seconds(),0)
    diff_fmt = str(timedelta(seconds=diff_seconds))
    
    return diff_fmt

def insere_coluna(df, col_name: str, after: bool=True):
    """ Função para inserção de uma coluna pos ou antes de outra coluna"""

    plus_minus = 1
    if after == False:
        plus_minus = -1

    index_col = df.columns.get_loc(col_name) + plus_minus
    return index_col

def download_blob(nome, bucket, prefix_snappy='df', compressao='snappy'):
    """Download um blob de um bucket."""

    #config = get_config()

    """ Aponta para o cache SuperSSofT - depois tem que ver se faz-se uma
        mais generica
    """
    local_cache = config['super_ssoft']['local_cache']

    storage_client = storage.Client()
    source_blob_name = '.'.join([prefix_snappy, nome.upper(), compressao])
    destination_file_name = local_cache + '/' + source_blob_name
    bucket_client = storage_client.bucket(bucket)

    do_log(f'Carregando {source_blob_name} e enviando para: {destination_file_name}...')

    getblob = bucket_client.get_blob(source_blob_name)
    getblob.download_to_filename(destination_file_name)

    return getblob

def download_blob_simples(nome, bucket, local_cache):
    """Download um blob de um bucket."""
    
    do_log(f'Download {nome} - bucket: {bucket}')
        
    storage_client = storage.Client()
    destination_file_name = local_cache + '/' + nome
    bucket_client = storage_client.bucket(bucket)

    do_log(f'Carregando {nome}... Enviando para: {destination_file_name}...')

    getblob = bucket_client.get_blob(nome)
    getblob.download_to_filename(destination_file_name)

    return getblob

def checa_existe_arquivo(arq):

    """Esta função checa se o arquivo da tabela em questão existe e se está
    válido dentro do tempo definido na config

    Caso esteja válido, usa o arquivo. Caso não esteja válido ou não exista, 
    faz o download do repositório de cache atualizado pela importação do
    totvs_hot
    """
    #config = get_config()
    arqcache = os.path.exists(arq)
    txt_arqcache = arq.split('.')[1]

    if arqcache is True:

        timeout = config['super_ssoft']['gcp']['arquivo_pqt']['timeout_minutes']
        file_mod_time = datetime.fromtimestamp(os.stat(arq).st_ctime)
        agora = datetime.today()
        max_delay = timedelta(minutes=timeout)
        
        if agora - file_mod_time > max_delay:
         
            do_log(f'{txt_arqcache} - existe mas invalidado pelo timeout')
            """
            Apaga o arquivo para garantir o "frescor dos daados" :-)
            """
            os.remove(arq)
            return False

        else:
         
            do_log(f'{txt_arqcache} - Arquivo de cache válido')
            arqcache = None
            return True
    else:

        do_log(f'{txt_arqcache} - Não existe o arquivo no cache. Carregando...')
        arqcache = None
        return False

def get_sql(Filesql: str, Index: int):
    """ Acessa o arquivo contendo scripts SQL e seleciona pelo índice criado a
    partir da separação de scripts com `;`  """

    fsql = open(Filesql, 'r', encoding="utf8")
    fsql = fsql.read().split(';')
    sql_script = fsql[Index]

    return sql_script

def df_info_to_json(df: pd.DataFrame, arq_saida: str, dir_saida: str):

    """Cria um arquivo json a partir da função pandas df.info()
    Args:
        df (pd.DataFrame): O dataframe a ser analisado
        arq_saida (str): o nome do arquivo de saida
        dir_saida (str): o diretorio onde este arquivo será armazenado
    """

    buffer = io.StringIO()

    df.info(buf=buffer, memory_usage='deep', max_cols=500)
    s = buffer.getvalue()

    arqinfo = '_temp.json'
    dir_saida_olha = dir_saida
    arqfinal = '/'.join([dir_saida_olha, arq_saida])

    with open(arqinfo, "w", encoding="utf-8") as f:
        f.write(s)

    schema_info = {
        'Int64Index': {},
        'data_columns': None,
        'columns': {},
        'dtypes': {},
        'memory_usage': None
    }

    arqopen = open(arqinfo, 'r')
    lnumerate = enumerate(arqopen)

    line_count = sum(1 for line in open(arqinfo))
    linha_fim = line_count-3

    cols = np.arange(5, linha_fim, 1).tolist()
    list_cols = []
    list_dtypes = []

    for lnum, linha in lnumerate:
        if lnum > 0:
            if lnum == 1:
                str_lin1 = linha.strip().split(None, 6)

                schema_info.update({
                    'Int64Index': {
                        'total': str_lin1[1],
                        'from': str_lin1[3],
                        'to': str_lin1[5],
                        }
                    
                    })
                
            elif lnum == 2:
                str_lin2 = linha.replace('(', '').replace(')', '')
                str_lin2 = str_lin2.split(None, 5)

                schema_info.update({'data_columns': str_lin2[3]})

            elif lnum in cols:
                nl = linha.split(None, 5)
                try:
                    list_cols.append(
                        {
                            'ord': int(nl[0]) + 1,
                            'column': nl[1],
                            'non_null': nl[2],
                            'dtype': nl[4]
                        }
                    )

                except IndexError as ie:
                    
                    print(ie)
                    pass
                    
            elif lnum == linha_fim + 1:
                str_lin4 = linha.replace('(', '_').replace(')', '').replace('\n','')
                str_lin4 = str_lin4.split(None, 5)
                str_lin4 = [lin.split('_') for lin in str_lin4]
                
                range_dtypes = str_lin4[1:]
                for um_dtype in range_dtypes:
                    list_dtypes.append({
                        um_dtype[0]: um_dtype[1].replace(',', '')
                        })   

            elif lnum == linha_fim + 2:
                str_lin5 = linha.split(None, 4)

                schema_info.update(
                    {'memory_usage': {
                        'memory': str_lin5[2],
                        'unit': str_lin5[3]
                        }
                    }
                    )

    schema_info.update({'columns': list_cols})
    schema_info.update({'dtypes': list_dtypes})

    with open(arqfinal, 'w') as af:
        json.dump(schema_info, af)

    arqopen.close()
    os.remove(arqinfo)

def get_heatmap(
    df,
    figsizeout=(20,6),
    nomearq=None,
    pallete='rocket'
    ):
    
    fig, ax = plt.subplots(figsize=figsizeout)
    ax = plt.subplots_adjust(bottom=0.25, left=0.08, top=0.99, right=0.99)

    #sns.set_palette("pastel")
    cmap = sns.color_palette(pallete, as_cmap=True)
    sns.heatmap(
        df.isnull(),
        yticklabels=False,
        cbar=False,
        cmap=cmap
        );
    
    if nomearq:
        fig.savefig(nomearq)

def pd_debug_set_ops():

    pd.set_option("max_rows", 250)
    pd.set_option("max_columns", 120)
    #pd.options.display.max_columns = 50  
    # to see max 50 columns
    pd.options.display.max_colwidth = 100 
    pd.options.display.precision = 2     
    #floating point precision upto 3 decimal    
    idx = pd.IndexSlice
    return idx

def gera_gsheet(df_sheet, credential, titulo):

    df_sheet.fillna('', inplace=True)
    #df_sheet.data.fillna('').astype(str).str.strip()
    
    gc = gspread.service_account(credential)
    agora = datetime.now()
    cabtitles = df_sheet.columns
    fim = len(cabtitles) + 1
    cols_padrao = len(df_sheet.columns)
    folga = 2
    plan = gc.create('{} - {}'.format(titulo, agora))

    plan_pq = plan.add_worksheet(
        title=titulo,
        rows=df_sheet.shape[0] + folga,
        cols=cols_padrao,
        index=0
        )

    plan_pq.update(
        [df_sheet.columns.values.tolist()
            ] + df_sheet.values.tolist())

    plan.share('mauro.braga@teiaescolar.com.br',
            perm_type='user', role='writer')