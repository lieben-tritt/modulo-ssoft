# %%
import os
import sys
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, current_dir)

from SSofT_functions import (
    get_config,
    checa_existe_arquivo,
    download_blob_simples,
    get_heatmap
    )

"""
Carrega o json de configuração do processo
"""

# %%
config = get_config()

"""
Cria variáveis para uso interno e global:
"""

remote_cache = config['super_ssoft']['gcp']['cache_super_super']
local_cache = config['super_ssoft']['local_cache']

"""
Montagem da classe geral "GetTotvs" e das classes específicas das tabelas
do repositrio CACHE - SSofT
Para pegar as dados "naturais", quando conveniente ou necessários, recorre-
-se à classe GetTotvs. Normalmente serão usadas as classes individuais

A class individual é necessária para as transoformações, de "de-para" por
exemplo, que cada tabela por ter.
"""

"""
Classe geral para todas as tabelas do repositório. É o estado V1 so SSofT
Houve uma primeira "interpretação" python a partir da extração do SQL Server
"""


class GetSSofT:
    """
    Classe base para tabelas TOTVS

    Classe base para tabelas TOTVS com todos os methods comuns
    Defnição ainda por vir - métodos úteis comuns
    """

    def __init__(self, snappy):

        self.snappy = snappy

    #@newrelic.agent.background_task(name='snappy', group='SuperSuperSSofT')
    def df_base(self, hm=False, columns=None):

        """ Função principal: montagem do dataframe a partir do arquivo já em estado
        SSofT via parquet local (local_cache) ou remoto (remote_cache - bucket).
        """

        nome_arquivo = self.snappy
     
        """ Procura primeiro localmente nome diretorio cache + nome do arquivo:
        """

        arq_cache = local_cache + '/' + nome_arquivo
        
        if checa_existe_arquivo(arq_cache) == False:

            cache_a_usar = remote_cache
            blob = download_blob_simples(self.snappy, cache_a_usar, local_cache)

            """ Depois de fazer o download define o nome do arquivo com
            o diretorio do cache compondo
            """
            arq_cache = local_cache + '/' + blob.name

        df_original = pd.read_parquet(arq_cache, engine='auto')
        
        if hm:
            nome_arquivo_hm = nome_arquivo + '.png'
            get_heatmap(
                df_original
                (30,10),
                local_cache + '/heatmap/' + nome_arquivo_hm
            )
    
        #for col in df_original.columns:
        #    setattr(self, col, col)
    
        return df_original


# %%
    
"""
Classes específicas de cada tabela do repositórioo CACHE
"""

class GetBoaFull(GetSSofT):
    
    def __init__(self):
        super().__init__('df.BolsaAluno_full.snappy')

    def df(self, columns=None):

        dfb = super().df_base()
 
        return dfb 


class GetBoaAtiva(GetSSofT):
    
    def __init__(self):
        super().__init__('df.BolsaAluno_ativa.snappy')

    def df(self, columns=None):

        dfb = super().df_base()
 
        return dfb
    
    
class GetStatus(GetSSofT):
    
    def __init__(self):
        super().__init__('df.StatusStatusMovimento_mpl_last.snappy')

    def df(self, columns=None):

        dfb = super().df_base()
 
        return dfb


class GetFluxo(GetSSofT):
    
    def __init__(self):
        super().__init__('df.StatusStatusMovimento_fluxo.snappy')

    def df(self, columns=None):

        dfb = super().df_base()
 
        return dfb


class GetLog(GetSSofT):
    
    def __init__(self):
        super().__init__('df.StatusStatusMovimento_lpl_log.snappy')

    def df(self, columns=None):

        dfb = super().df_base()
 
        return dfb


class GetFullLog(GetSSofT):
    
    def __init__(self):
        super().__init__('df.StatusStatusMovimento_full.snappy')

    def df(self, columns=None):

        dfb = super().df_base()
 
        return dfb


class GetParMov(GetSSofT):

    def __init__(self):
        super().__init__('df.Parcela.snappy')

    def df(self, columns=None):

        dfb = super().df_base()
 
        return dfb
