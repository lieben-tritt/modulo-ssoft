# %%
# Importa o que importa!
import hashlib
import sys
import os
import os.path
from collections import defaultdict

import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, current_dir)

from SSofT_functions import (
    get_config, 
    checa_existe_arquivo, 
    df_info_to_json,
    download_blob, 
    get_heatmap
    )

"""
Carrega o json de configuração do processo
"""
config = get_config()
ano_letivo_base_corte_global = config['super_ssoft']['gcp']['ano_letivo_base']
"""
Cria variáveis para uso interno e global:
"""
ambiente = config['super_ssoft']
credential = ambiente['gcp']['credential']

ssoft_credential = f'{current_dir}.{credential}')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ssoft_credential

remote_cache = ambiente['gcp']['cache_bucket']
remote_cache_st = ambiente['gcp']['cache_bucket_st']
local_cache = ambiente['local_cache']
compressao = ambiente['gcp']['arquivo_pqt']['compressao']
prefixo = ambiente['gcp']['arquivo_pqt']['prefixo']
project_id_dp = config['totvs_hot']['totvs_hot_dp']['project_id']
dataset_id_dp = config['totvs_hot']['totvs_hot_dp']['dataset_id']
prefixo_dp =  config['totvs_hot']['totvs_hot_dp']['arquivo_pqt_dp']['prefixo']
remote_cache_dp = remote_cache = ambiente['gcp']['cache_bucket']
local_cache_dp = local_cache = ambiente['local_cache']


def tree():
    return defaultdict(tree)
dirs = tree()

""" Cria a estrutura de diretorios para a operação SuperSSofT
"""
if os.path.isdir(local_cache)==False:

    print('Criando a estrutura inicial de diretorios...')
    dirs[local_cache]["heatmap"]
    dirs[local_cache]["metadados"]
    dirs[local_cache]["ssoft_n1"]["metadados"]
    dirs[local_cache]["ssoft_n1"]["heatmap"]
    dirs[local_cache]["ssoft_n2"]["metadados"]
    dirs[local_cache]["ssoft_n2"]["heatmap"]
    dirs[local_cache]["ssoft_n3"]["metadados"]
    dirs[local_cache]["ssoft_n3"]["heatmap"]

    def cria_dirs(directory, current_path):
        if len(directory):
            for direc in directory:
                cria_dirs(directory[direc], os.path.join(current_path, direc))
        else:
            os.makedirs(current_path)

    cria_dirs(dirs, "")


class GetTotvs:
    
    """ Montagem da classe geral "GetTotvs" e das classes específicas das tabelas
    do repositrio CACHE - SSofT
    Para pegar as dados "naturais", quando conveniente ou necessários, recorre-
    -se à classe GetTotvs. Normalmente serão usadas as classes individuais

    A class individual é necessária para as transoformações, de "de-para" por
    exemplo, que cada tabela pode necessitar, como é o caso de `SSTATUS`.

    Classe geral para todas as tabelas do repositório. É o estado V1 so SSofT
    Houve uma primeira "interpretação" python a partir da extração do SQL Server
    """

    def __init__(self, tabela, strict=False, idx=None, proj_id=None, dataset_id=None):

        self.tabela = tabela.upper()
        self.strict = strict
        self.proj_id = None
        self.dataset_id = None
        self.idx = idx

    def df_base(self, hm=False, columns=None):
        """
        Função principal: montagem do dataframe a partir do arquivo 
        parquet local (local_cache) ou remoto (remote_cache - bucket).

        Nome do arquivo (convenção):
        <prefixo>.<TABELA>.<compressao>

        Exemplo:
        "df.SALUNO.snappy" o "O destino é um [d]ata[f]rames da
        tabela totvs [SALUNO] com a compressão [snappy]
        """
        nome_arquivo = '.'.join([prefixo, self.tabela, compressao])
        nome_arquivo_json = '.'.join([prefixo, self.tabela, 'json'])

        """
        Procura primeiro localmente nome diretorio cache + nome do arquivo:
        """
        arq_cache = local_cache + '/' + nome_arquivo

        if checa_existe_arquivo(arq_cache) == False:

            if self.strict:
                cache_a_usar = remote_cache_st
            else:
                cache_a_usar = remote_cache

            blob = download_blob(self.tabela, cache_a_usar,
                                 prefixo, compressao)

            """ 
            Depois de fazer o download define o nome do arquivo com
            o diretorio do cache compondo
            """
            arq_cache = local_cache + '/' + blob.name

        try:
            self.df_original = pd.read_parquet(arq_cache, engine='auto')
            self.df_original.dropna(axis=1, how='all', inplace=True)
            self.df_original.columns = self.df_original.columns.str.lower()

        except Exception as e:
            print(f'Erro ao ler o arquivo {nome_arquivo}')

        if columns:
            filtro = list(columns.replace(' ', '').split(','))
        else:
            filtro = list(self.df_original.columns)

        """ Regsitra o metadado da tabela processada
        """
        df_info_to_json(
            self.df_original,
            nome_arquivo_json,
            local_cache + '/metadados'
        )

        if hm:
            nome_arquivo_hm = '.'.join([prefixo, self.tabela.lower(), 'png'])
            get_heatmap(
                self.df_original.filter(filtro),
                (30, 10),
                local_cache + '/ssoft_n1/heatmap/' + nome_arquivo_hm
            )

        return self.df_original.filter(filtro)


class PpessoaSt(GetTotvs):

    def __init__(self):
        super().__init__('ppessoa_st', True)

    def df(self, columns=None, thdp: bool = False):

        dfb = super().df_base(columns)
        dfb.rename(columns={'codigo': 'codpessoa'}, inplace=True)

        return dfb


class FcfoSt(GetTotvs):

    def __init__(self):
        super().__init__('fcfo_st', True)

    def df(self, columns=None, thdp: bool = False):

        dfb = super().df_base(columns)

        return dfb


class Slogpletivo(GetTotvs):

    def __init__(self):
        super().__init__('slogpletivo')

    def df(self, columns=None, thdp: bool = False):

        dfb = super().df_base(columns)
        filtro = list(dfb.columns)

        if thdp is True:
            self.do_totvs_hot_dp(dfb.filter(filtro), 'slogpletivo')

        return dfb.filter(filtro)


class Smatricpl(GetTotvs):

    def __init__(self):
        super().__init__('smatricpl')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        filtro = list(dfb.columns)

        return dfb.filter(filtro)


class Smatricula(GetTotvs):

    def __init__(self):
        super().__init__('smatricula')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        filtro = list(dfb.columns)

        return dfb.filter(filtro)


class Shabilitacaofilial(GetTotvs):

    def __init__(self):
        super().__init__('shabilitacaofilial')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        hbl = Shabilitacao().df()[
            ['codcoligada', 'codcurso', 'codhabilitacao', 'nome_habilitacao']
        ]
        dfb = dfb.merge(
            hbl, 'left', ['codcoligada', 'codcurso', 'codhabilitacao'])

        dfb['codcurso_dp'] = dfb.apply(
            lambda df: self.de_para_codcurso(
                df.codcoligada,
                df.codfilial,
                df.codcurso,
                df.codhabilitacao
            ),
            axis=1
        )

        dfb['codcurso_dp_sub'] = dfb.apply(
            lambda df: self.de_para_codcurso_sub(
                df.codcoligada,
                df.codfilial,
                df.codcurso,
                df.codhabilitacao
            ),
            axis=1
        )

        dfb['serie_dp'] = dfb.codcurso_dp_sub.str[4]

        """ trata "EXTRAS" como não tendo série e portanto -1 """

        """ tem que cercar BIS e Bureau """
        dfb.loc[dfb.serie_dp.str.isalpha() == True, 'serie_dp'] = '-'
        dfb.loc[dfb.serie_dp == '-', 'serie_dp'] = -1

        """ trata a não existência de série ainda """

        dfb.loc[dfb.serie_dp.isnull(), 'serie_dp'] = -1

        """ Trata a Ordem do CURSO  """

        dfb['codcurso_dp_sub_ord'] = dfb.apply(
            lambda df: self.de_para_ordem(
                df.codcurso_dp,
                df.codcurso_dp_sub,
            ),
            axis=1
        ).fillna(-1).astype(int)

        """ Trata a nome do CURSO  """

        dfb['codcurso_dp_nome'] = dfb.apply(
            lambda df: self.de_para_nome_txt(df.codcurso_dp,), axis=1
        )

        """Cria a coluna codcurso_sub_turno"""

        tno = self.codcurso_sub_turno_df()
        _temp = dfb.merge(tno, how='left', on=['codcoligada', 'codturno'])

        dfb['codcurso_sub_turno'] = _temp.codcurso_dp_sub + \
            '-' + _temp.turno_dp.str[0]
        dfb['turno_dp'] = _temp.turno_dp

        colunas = [
            'codcoligada', 'codfilial', 'nome_habilitacao', 'codcurso_dp',
            'codcurso_dp_sub', 'serie_dp', 'codcurso_dp_sub_ord',
            'codcurso_dp_nome', 'idhabilitacaofilial', 'codturno', 'turno_dp',
            'codcurso', 'codtipocurso', 'codcurso_sub_turno', 'codhabilitacao',
            'codgrade', 'codccusto', 'ativo'
        ]

        return dfb[colunas]

    def codcurso_sub_turno_df(self):
        """Faz a extração básica de turno para a criação da coluna 
        codcurso_sub_turno"""

        tno = Sturno()
        tno = tno.df()[['codcoligada', 'codturno', 'turno_dp']]
        return tno

    def de_para_ordem(self, codcurso_dp, codcurso_dp_sub):

        try:
            if codcurso_dp == '0EI':
                return codcurso_dp_sub[4]
            elif (codcurso_dp == '1EF') | (codcurso_dp == '2EF'):
                return int(codcurso_dp_sub[4]) + 5
            elif codcurso_dp == '3EM':
                return int(codcurso_dp_sub[4]) + 14
        except TypeError as e:
            pass

    def de_para_nome_txt(self, codcurso_dp):

        if codcurso_dp == '0EI':
            return 'Educação Infantil'
        elif codcurso_dp == '1EF':
            return 'Ensino Fundamental 1'
        elif codcurso_dp == '2EF':
            return 'Ensino Fundamental 2'
        elif codcurso_dp == '3EM':
            return 'Ensino Médio'
        else:
            return 'Extras'

    def de_para_codcurso(self, codcoligada, codfilial, codcurso, codhabilitacao):

        if (codcoligada==6) & (codfilial==5):
            return codcurso[0:3]
        else:   
            if (codcoligada in [1, 2]) & (codcurso == 'EI') & (codhabilitacao in ['1', 'VD']):
                return '1EF'
            elif codcurso in ['1', 'EI', '0EI']:
                return '0EI'
            elif codcurso in ['4', 'F1', '1EF']:
                return '1EF'
            elif codcurso in ['5', 'F2', '2EF']:
                return '2EF'
            elif codcurso in ['2', 'EM', '3EM']:
                return '3EM'
            else:
                return 'Extras'

    def de_para_codcurso_sub(self, codcoligada, codfilial, codcurso, codhabilitacao):

        if codcoligada not in [1, 2, 4, 5]:
            if (codcoligada==6) & (codfilial==5):
                return '-'.join([codcurso[0:3], codhabilitacao])
            else:
                return '-'.join([codcurso, codhabilitacao])
        else:
            if codcoligada in [1, 2]:
                if codcurso == 'EI':

                    if codhabilitacao == 'AI':
                        return '0EI-1'
                    elif codhabilitacao == 'AO':
                        return '0EI-2'
                    elif codhabilitacao == 'LJ':
                        return '0EI-3'
                    elif codhabilitacao == 'AZ':
                        return '0EI-4'
                    elif codhabilitacao == 'VM':
                        return '0EI-5'
                    elif codhabilitacao == 'VM':
                        return '0EI-5'
                    elif (codhabilitacao == 'VD') | (codhabilitacao == '1'):
                        return '1EF-1'

                elif codcurso == 'F1':

                    if codhabilitacao == 'VD':
                        return '1EF-1'
                    elif codhabilitacao == '1':
                        return '1EF-1'
                    elif codhabilitacao == '2':
                        return '1EF-2'
                    elif codhabilitacao == '3':
                        return '1EF-3'
                    elif codhabilitacao == '4':
                        return '1EF-4'
                    elif codhabilitacao == '5':
                        return '1EF-5'

                elif codcurso == 'F2':

                    if codhabilitacao == '6':
                        return '2EF-6'
                    elif codhabilitacao == '7':
                        return '2EF-7'
                    elif codhabilitacao == '8':
                        return '2EF-8'
                    elif codhabilitacao == '9':
                        return '2EF-9'

                elif codcurso == 'EM':

                    if codhabilitacao == '1':
                        return '3EM-1'
                    elif codhabilitacao == '2':
                        return '3EM-2'
                    elif codhabilitacao == '3':
                        return '3EM-3'

            elif codcoligada in [4, 5]:

                if codcurso == '1':

                    if codhabilitacao == '1':
                        return '0EI-3'
                    elif codhabilitacao == '2':
                        return '0EI-4'
                    elif codhabilitacao == '3':
                        return '0EI-5'
                    elif codhabilitacao == '11':
                        return '0EI-1'
                    elif codhabilitacao == '12':
                        return '0EI-2'
                    elif codhabilitacao == '13':
                        return '0EI-3'
                    elif codhabilitacao == '14':
                        return '0EI-4'
                    elif codhabilitacao == '15':
                        return '0EI-5'

                if codcurso == '4':

                    if codhabilitacao == '1':
                        return '1EF-1'
                    elif codhabilitacao == '2':
                        return '1EF-2'
                    elif codhabilitacao == '3':
                        return '1EF-3'
                    elif codhabilitacao == '4':
                        return '1EF-4'
                    elif codhabilitacao == '5':
                        return '1EF-5'

                if codcurso == '5':
                    if codhabilitacao == '6':
                        return '2EF-6'
                    elif codhabilitacao == '7':
                        return '2EF-7'
                    elif codhabilitacao == '8':
                        return '2EF-8'
                    elif codhabilitacao == '9':
                        return '2EF-9'

                if codcurso == '2':

                    if codhabilitacao == '1':
                        return '3EM-1'
                    elif codhabilitacao == '2':
                        return '3EM-2'
                    elif codhabilitacao == '3':
                        return '3EM-3'

                if (codcoligada == 4) & (codcurso == '1'):

                    if codhabilitacao == '2':
                        return '0EI-2'
                    elif codhabilitacao == '6':
                        return '0EI-4'
                    elif codhabilitacao == '7':
                        return '0EI-5'

                if (codcoligada == 5) & (codcurso == '1'):

                    if codhabilitacao == '6':
                        return '0EI-2'
                    elif codhabilitacao == '7':
                        return '0EI-2'
                    elif codhabilitacao == '8':
                        return '0EI-4'
                    elif codhabilitacao == '9':
                        return '0EI-5'


class Shabilitacao(GetTotvs):

    def __init__(self):
        super().__init__('shabilitacao')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        dfb.rename(columns={'nome': 'nome_habilitacao'}, inplace=True)
        dfb.drop(columns=[
            'descricao', 'prioridade', 'reccreatedby', 'reccreatedon',
            'recmodifiedby', 'recmodifiedon'
        ], inplace=True)

        return dfb


class Spletivo(GetTotvs):

    def __init__(self):
        super().__init__('spletivo')

    def df(self, columns=None):
        df = super().df_base(columns)

        df['anoletivo'] = df.codperlet.str[: 4].astype(int)

        return df


class Stipomatricula(GetTotvs):

    def __init__(self):
        super().__init__('stipomatricula')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        dfb.loc[dfb.descricao.str.lower().isin(['matrícula', 'matricula']),
                'descricao'] = 'Matrícula'

        dfb.loc[dfb.descricao.str.lower() == 'rematrícula',
                'descricao'] = 'Rematrícula'

        dfb.loc[dfb.descricao.str.lower() == 'transferido',
                'descricao'] = 'Transferido'

        dfb.loc[dfb.descricao.str.lower() == 'ex-aluno',
                'descricao'] = 'Ex-aluno'

        dfb.loc[dfb.descricao.str.lower() == 'candidato',
                'descricao'] = 'Candidato'

        dfb.loc[dfb.descricao.str.lower() == 'Transferido',
                'descricao'] = 'Transferido'

        dfb.rename(columns={'descricao': 'tipomat'}, inplace=True)

        dfb = dfb.assign(
            codtipomat_dp=-1, tipomat_dp=None, tipomat_dp_val=0, tipomat_nome_col=None
        )

        lista_campos = ['codtipomat_dp', 'tipomat_dp',
                        'tipomat_dp_val', 'tipomat_nome_col']

        dfb.loc[
            dfb.tipomat.str.lower().isin(['matrícula']), lista_campos
        ] = [1, 'Matrícula', 1, 'matricula']
        dfb.loc[
            dfb.tipomat.str.lower().isin(['rematrícula']), lista_campos
        ] = [2, 'Rematrícula', 1, 'rematricula']

        dfb.loc[
            dfb.tipomat.str.lower().isin(['candidato']), lista_campos
        ] = [3, 'Candidato', 0, 'candidato']

        dfb.loc[
            dfb.tipomat.str.lower().isin(['ex-aluno']), lista_campos
        ] = [4, 'Ex-aluno', 1, 'ex_aluno']

        dfb.loc[
            dfb.tipomat.str.lower().isin(['transferido']), lista_campos
        ] = [5, 'Transferido', -1, 'ex_aluno']

        #filtro = ['codcoligada', 'codtipomat', 'tipomat', 'tipomat_nome_col']

        return dfb  # .filter(filtro)


class Stipocurso(GetTotvs):

    def __init__(self):
        super().__init__('stipocurso')

    def df(self, columns=None):
        
        dfb = super().df_base(columns)
        dfb = dfb[['codcoligada', 'codtipocurso', 'nome', 'apresentacao']]
        
        dfb['codtipocurso_dp'] = dfb.apresentacao.astype(int)
        dfb['tipocurso_dp'] = 'Curricular'
        
        dfb.loc[dfb.codtipocurso_dp > 0, 'tipocurso_dp'] = 'Extracurricular'
        dfb.rename(columns={'nome': 'tipocurso'}, inplace=True)

        return dfb


class Sstatus(GetTotvs):

    def __init__(self):
        super().__init__('sstatus')

    def df(self, columns=None):
        """De - PARA - status de cada par mesmo status"""
        
        dfb = super().df_base(columns)

        df = dfb.assign(codstatus_dp=0, status_dp=None, status_dp_ord=None,
                        status_dp_val=0)

        lista_campos = ['codstatus_dp', 'status_dp', 'status_dp_val',
                        'status_dp_ord', 'status_dp_nome_col', 'status_dp_alg']

        df.loc[
            df.descricao.str.contains('Pré-Mat', case=False),
            lista_campos
        ] = [1, 'Pré-Matrícula', 0, 1, 'pre_matriculado', .25]

        df.loc[
            df.descricao.str.lower().isin(['reserva de vaga']),
            lista_campos
        ] = [2, 'Reserva de Vaga', 0, 2, 'reserva_de_vaga', .25]

        list_mat = ['matriculado', 'cursando']
        df.loc[
            df.descricao.str.contains('|'.join(list_mat), case=False),
            lista_campos
        ] = [3, 'Matriculado', 1, 3, 'matriculado', 1]

        lista_mov_int = ['Transferência interna',
                         'Transferido de Turma', 'Remanejado']
        df.loc[
            df.descricao.str.lower().isin(x.lower() for x in lista_mov_int),
            lista_campos
        ] = [4, 'Remanejado', -1, 4, 'remanejado', -1]

        lista_tran = ['Transferido', 'Transferido de Instituição']
        df.loc[
            df.descricao.str.lower().isin(x.lower() for x in lista_tran),
            lista_campos
        ] = [5, 'Transferido de Instituição', -1, 5, 'transf_instituicao', -1]

        lista_tran_teia = [
            'Transferido p/ Escola do Grupo', 'Transferência Teia']
        df.loc[
            df.descricao.str.lower().isin(x.lower() for x in lista_tran_teia),
            lista_campos
        ] = [6, 'Transferido p/ Escola do Grupo', -1, 5, 'transf_escola_grupo', -1]

        list_can = ['Cancelado', 'Não Cursou']
        df.loc[
            df.descricao.str.contains('|'.join(list_can), case=False),
            lista_campos
        ] = [7, 'Cancelado', -1, 2, 'cancelado', -1]

        df.loc[
            df.descricao.str.contains('desistente', case=False),
            lista_campos
        ] = [8, 'Desistente', -1, 2, 'desistente', -1]

        df.loc[
            df.descricao.str.contains('aprovado', case=False),
            lista_campos
        ] = [9, 'Aprovado', 0, 4, 'aprovado', .25]

        lista_repr = ['Retido', 'Reprovado', 'Reprovado por Falta']
        df.loc[
            df.descricao.str.lower().isin(x.lower() for x in lista_repr),
            lista_campos
        ] = [10, 'Reprovado', 0, 4, 'reprovado', 0]

        df.loc[
            df.descricao.str.contains('recupera', case=False),
            lista_campos
        ] = [11, 'Recuperação', 0, 4, 'recuperacao', 0]

        df.loc[
            df.descricao.str.contains('regress', case=False),
            lista_campos
        ] = [12, 'Regressão', 0, 4, 'regressao', 0]

        df.loc[
            df.descricao.str.lower().isin(['apc', 'conselho']),
            lista_campos
        ] = [13, 'Aprovado pelo Conselho', 0, 4, 'aprovado_conselho', .25]

        df.loc[
            df.descricao == 'Reclassificado', lista_campos
        ] = [14, 'Reclassificado', -1, 4, 'reclassificado', .25]

        df.loc[
            df.descricao == 'Intercâmbio', lista_campos
        ] = [15, 'Intercâmbio', -1, 4, 'intercambio', -.5]

        list_conc = ['Concluiu']
        df.loc[
            df.descricao.str.contains('|'.join(list_conc), case=False),
            lista_campos
        ] = [30, 'Formado', -1, 5, 'formado', -1]

        #df.loc[(df.codcoligada==6) & (df.codtatus==16)]       
        
        return df


class Sstatus_ext(GetTotvs):

    def __init__(self):
        super().__init__('sstatus')

    def df(self, columns=None):

        s = Sstatus()
        df_ = s.df()
        return df_


class Sturno(GetTotvs):

    def __init__(self):

        super().__init__('sturno')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        dft = dfb.assign(turno_dp=None, turno_dp_nome_col='nao_def')
        dft.loc[dft.tipo == 'M', [
            'turno_dp', 'turno_dp_nome_col']] = ['Manhã', 'manha']
        dft.loc[dft.tipo == 'V', [
            'turno_dp', 'turno_dp_nome_col']] = ['Tarde', 'tarde']
        dft.loc[dft.tipo == 'I', [
            'turno_dp', 'turno_dp_nome_col']] = ['Integral', 'integral']
        dft.loc[dft.tipo == 'N', [
            'turno_dp', 'turno_dp_nome_col']] = ['Noite', 'noite']

        filtro = ['codcoligada', 'codfilial', 'codturno', 'turno_dp',
                  'horini', 'horfim', 'tipo', 'turno_dp_nome_col']

        return dft.filter(filtro)


class Smotivoaltmat(GetTotvs):

    def __init__(self):
        super().__init__('smotivoaltmat')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        dfb['descricao'] = dfb.descricao.str.lstrip('(INATIVO)')
        dfb.rename(columns={'descricao': 'motivo'}, inplace=True)

        return dfb


class Smotivoaltmat_ext(GetTotvs):

    def __init__(self):
        super().__init__('smotivoaltmat')

    def df(self, columns=None):

        mam_ext = Smotivoaltmat()
        df_ = mam_ext.df()
        return df_


class Sinstituicao(GetTotvs):

    def __init__(self):
        super().__init__('sinstituicao')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        dfb = dfb.rename(columns={
            'nome': 'instituicao',
            'nomefantasia': 'instituicao_nf'
        })

        filtro = ['codinst', 'instituicao', 'instituicao_nf', 'cidade', 'uf',
                  'diretor', 'conveniada', 'tipoinst']

        return dfb.filter(filtro)


class Sturma(GetTotvs):

    def __init__(self):
        super().__init__('sturma')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        cols_ren = {'nome': 'nometurma', 'nomered': 'nometurma_red'}
        dfb.rename(columns=cols_ren, inplace=True)
        filtro = list(dfb.columns)

        return dfb.filter(filtro)


class Gcoligada(GetTotvs):

    def __init__(self):
        super().__init__('gcoligada')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        filtro = list(dfb.columns)

        return dfb.filter(filtro)

    def get_model(self):

        return super().get_model_schema(self.df)


class Gfilial(GetTotvs):

    def __init__(self):
        super().__init__('gfilial')

    def df(self, columns=None):

        dfb = super().df_base(columns)

        cols = [
            'codcoligada',
            'codfilial',
            'cgc',
            'nome',
            'nomefantasia',
            'telefone',
            'email',
            'rua',
            'numero',
            'complemento',
            'bairro',
            'cidade',
            'estado',
            'pais',
            'cep'
        ]

        return dfb[cols]


class Sparcela(GetTotvs):

    def __init__(self):
        super().__init__('sparcela')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        filtro = list(dfb.columns)

        return dfb.filter(filtro)


class Slan(GetTotvs):

    def __init__(self):
        super().__init__('slan')

    def df(self, columns=None):
        dfb = super().df_base(columns)
        filtro = list(dfb.columns)

        return dfb.filter(filtro)


class Flan(GetTotvs):

    def __init__(self):
        super().__init__('flan')

    def df(self, columns=None):

        dfb = super().df_base(columns)

        dfb.insert(
            4, 'statuslan_desc_dp',
            dfb.apply(
                lambda x: self.depara_statuslan(x.statuslan), axis=1
            )
        )
        _cols_op = [
            'valorop1',
            'valorop2',
            'valorop3',
            'valorop4',
            'valorop5',
            'valorop6',
            'valorop7',
            'valorop8',
        ]
        dfb['valorop_sum'] = dfb[_cols_op].sum(axis=1)

        return dfb

    def depara_statuslan(self, x):
        if x == 0:
            txt = 'Em Aberto'
        elif x == 1:
            txt = 'Baixado'
        elif x == 2:
            txt = 'Cancelado'
        elif x == 3:
            txt = 'Baixado por Acordo'
        elif x == 4:
            txt = 'Baixado parcialmente'
        elif x == 5:
            txt = 'Borderô'
        else:
            txt = 'Não definido'
        return txt


class Fboleto(GetTotvs):

    def __init__(self):
        super().__init__('fboleto')

    def df(self, columns=None):

        dfb = super().df_base(columns)

        dfb.insert(
            13, 'cnabstatus_desc_dp',
            dfb.apply(
                lambda x: self.depara_cnabstatus(x.cnabstatus),
                axis=1
            )
        )

        return dfb

    def depara_cnabstatus(self, x):
        if x == 0:
            txt = 'Não Remetido'
        elif x == 1:
            txt = 'Remetido'
        elif x == 2:
            txt = 'Registrado'
        elif x == 3:
            txt = 'Recusado'
        elif x == 4:
            txt = 'Baixado'
        elif x == 5:
            txt = 'Registrado Online'
        elif x == 6:
            txt = 'Cancelado'
        elif x == 7:
            txt = 'Pendente Remessa'
        return txt


class Flanboleto(GetTotvs):

    def __init__(self):
        super().__init__('flanboleto')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        filtro = list(dfb.columns)

        return dfb.filter(filtro)


class Flanbaixa(GetTotvs):

    def __init__(self):
        super().__init__('flanbaixa')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        """ para sincronizar com a FLAN """
        dfb = dfb.rename(columns={'valorbaixa': 'valorbaixado'})
        return dfb


class Fxcx(GetTotvs):

    def __init__(self):
        super().__init__('fxcx')

    def df(self, columns=None):

        dfb = super().df_base(columns)
        return dfb


class Sservico(GetTotvs):

    def __init__(self):
        super().__init__('sservico')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        dfb['tdo_dp'] = dfb.apply(
            lambda df: self.cria_tdo_dp(df.codtdo), axis=1
        )
        dfb = dfb.rename(columns={'nome': 'servico'})

        return dfb

    def cria_tdo_dp(self, codtdo):
        if (codtdo == 'MENSALIDAD') | (codtdo == '2021 MENS'):
            return 'Mensalidade'
        elif (codtdo == 'EXTRA') | (codtdo == '2021 EXTRA'):
            return 'Extras'
        elif (codtdo == 'CHQDEV'):
            return 'Devolução de Cheque'
        elif (codtdo == 'ACORDO'):
            return 'Acordo'
        elif (codtdo == 'TXMAT'):
            return 'Taxa de Material'
        elif (codtdo == 'FIN.COVID'):
            return 'Financiamento COVID'
        elif (codtdo == 'DANPATR'):
            return 'Danos Patrimoniais'
        elif (codtdo == 'OPTATIVA'):
            return 'Optativa'
        elif (codtdo == 'ALUGUEL'):
            return 'Aluguel'
        elif codtdo is None:
            return 'Não definido'
        else:
            return 'Outros'


class Splanopgto(GetTotvs):

    def __init__(self):
        super().__init__('splanopgto')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        dfb.rename(columns={
            'descricao': 'descplanpgto',
            'nome': 'nomeplanpgto',
            'dtinicio': 'datainiciopgto',
            'dtfim': 'datafimpgto'
        }, inplace=True)

        return dfb


class Scontrato(GetTotvs):

    def __init__(self):
        super().__init__('scontrato')

    def df(self, columns=None):

        dfb = super().df_base(columns)
        return dfb


class Saluno(GetTotvs):

    def __init__(self):
        super().__init__('saluno')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        filtro = list(dfb.columns)

        return dfb.filter(filtro)


class Ppessoa(GetTotvs):

    def __init__(self):
        super().__init__('ppessoa')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        _ren = {'codigo': 'codpessoa', 'nome': 'nompessoa'}
        dfb.rename(columns=_ren, inplace=True)
        filtro = list(dfb.columns)

        return dfb.filter(filtro)


class Fcfo(GetTotvs):

    def __init__(self):
        super().__init__('fcfo')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        filtro = list(dfb.columns)

        return dfb.filter(filtro)


class Tmov(GetTotvs):

    def __init__(self):
        super().__init__('tmov')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        return dfb


class Tmovlan(GetTotvs):

    def __init__(self):
        super().__init__('tmovlan')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        return dfb


class Ftdo(GetTotvs):

    def __init__(self):
        super().__init__('ftdo')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        dfb['codtdo_desc_dp'] = dfb.descricao
        return dfb


class Zmdgrupo(GetTotvs):

    def __init__(self):
        super().__init__('zmdgrupo')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        return dfb


class Zmdgrupocoligada(GetTotvs):

    def __init__(self):
        super().__init__('Zmdgrupocoligada')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        zgc = dfb[['codgrupo', 'codcoligada']]

        gc = Gcoligada().df()[['codcoligada', 'nomefantasia', 'nome']]
        gc.rename(
            columns={
                'nome': 'coligada_rs',
                'nomefantasia': 'coligada'
            },
            inplace=True
        )

        gf = Gfilial().df()[
            ['codcoligada', 'codfilial', 'nome', 'nomefantasia']]
        gf.rename(
            columns={
                'nome': 'filial_rs',
                'nomefantasia': 'filial'
            },
            inplace=True
        )

        zg = Zmdgrupo().df()[['codgrupo', 'nomegrupo']]

        df_grupo = zg.merge(zgc, on=['codgrupo'])

        df_grupo['escola'] = df_grupo.nomegrupo
        df_grupo['codescola'] = df_grupo.codgrupo

        df_grupo = df_grupo.merge(gc, on=['codcoligada'])
        df_grupo = df_grupo.merge(gf, on=['codcoligada'])

        """ Trata o codunidade """
        df_grupo = df_grupo.assign(
            unidade=-1,
            nomeunidade=df_grupo.nomegrupo.str.replace(
                'Grupo\s', '', regex=True
            )
        )

        df_grupo.loc[
            df_grupo.codcoligada.isin([1, 2, 3, 9, 10, 12, 13, 14, 19, 20]
                                      ), 'unidade'] = 1

        # VILA BUTANTA
        df_grupo.loc[(df_grupo.codcoligada == 4) & (df_grupo.codfilial == 1),
                     ['unidade', 'nomeunidade']
                     ] = [1, 'Escola da Vila - Butantã']

        # VILA GRANJA
        df_grupo.loc[(df_grupo.codcoligada == 4) & (df_grupo.codfilial == 2),
                     ['unidade', 'nomeunidade']
                     ] = [2, 'Escola da Vila - Granja Viana']

        # VILA MORUMBI
        df_grupo.loc[
            (df_grupo.codcoligada == 5), ['unidade', 'nomeunidade']
        ] = [3, 'Escola da Vila - Morumbi']

        # PARQUE
        df_grupo.loc[(df_grupo.codcoligada == 15), ['unidade', 'nomeunidade']
                     ] = [1, 'Unidade Gávea']

        df_grupo.loc[(df_grupo.codcoligada == 16), ['unidade', 'nomeunidade']
                     ] = [2, 'Unidade Barra']
        # BIS
        df_grupo.loc[df_grupo.codcoligada.isin([17, 18]), 'unidade'] = 1

        # Autonomia
        df_grupo.loc[
            (df_grupo.codcoligada == 6) &
            (df_grupo.codfilial == 2), [
                'codgrupo', 'nomegrupo', 'codescola', 'escola', 'unidade',
                'filial', 'nomeunidade'
            ]
        ] = [
            62, 'Escola Autonomia', 62, 'Escola Autonomia', 1,
            'Escola Autonomia', 'Escola Autonomia'
        ]

        df_grupo.loc[
            (df_grupo.codcoligada == 6) &
            (df_grupo.codfilial == 5), [
                'codgrupo', 'nomegrupo', 'codescola', 'escola', 'unidade',
                'filial', 'nomeunidade'
            ]
        ] = [
            65, 'Colégio Apoio', 65, 'Colégio Apoio', 1,
            'Colégio Apoio', 'Colégio Apoio'
        ]

        """
        hash entra depois das mascaras de nome grupo etc
        """

        df_grupo['codmd5'] = [
            hashlib.md5(
                val.encode('UTF-8')
            ).hexdigest() for val in df_grupo['nomegrupo']
        ]

        df_grupo = df_grupo[[
            'codgrupo',
            'nomegrupo',
            'codescola',
            'escola',
            'unidade',
            'nomeunidade',
            'codcoligada',
            'coligada',
            'coligada_rs',
            'codfilial',
            'filial_rs',
            'filial',
            'codmd5',
        ]]

        df_grupo = df_grupo.drop_duplicates(keep='first')

        return df_grupo

    def dict_grupocoligada(self, dict_orient='records'):
        return self.df().to_dict(orient=dict_orient)


class Zmdfichamedicaquestionario(GetTotvs):

    def __init__(self):
        super().__init__('zmdfichamedicaquestionario')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        return dfb


class Zmdfichamedicagrupopergunta(GetTotvs):

    def __init__(self):
        super().__init__('zmdfichamedicagrupopergunta')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        return dfb


class Zmdfichamedicapergunta(GetTotvs):

    def __init__(self):
        super().__init__('zmdfichamedicapergunta')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        return dfb


class Zmdfichamedicaresposta(GetTotvs):

    def __init__(self):
        super().__init__('zmdfichamedicaresposta')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        return dfb


class Cpartida(GetTotvs):

    def __init__(self):
        super().__init__('cpartida')

    def df(self, columns=None):
        dfb = super().df_base(columns)
        df19 = dfb.loc[dfb.data >= ano_letivo_base_corte_global]

        return df19


class Cconta(GetTotvs):

    def __init__(self):
        super().__init__('cconta')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        return dfb


class Gccusto(GetTotvs):

    def __init__(self):
        super().__init__('gccusto')

    def df(self, columns=None):
        dfb = super().df_base(columns)

        return dfb


class Sbolsa(GetTotvs):

    def __init__(self):
        super().__init__('sbolsa')

    def df(self, columns=None):
        df = super().df_base(columns)
        df.rename(columns={
            'nome': 'nomebolsa',
            'valor': 'valorbolsa',
            'tipodesc': 'tipodesconto'
        }, inplace=True)

        return df


class Sbolsaaluno(GetTotvs):

    def __init__(self):
        super().__init__('sbolsaaluno')

    def df(self, columns=None):
        df = super().df_base(columns)

        _pl = Spletivo().df()[['codcoligada', 'idperlet', 'anoletivo']]

        df = df.merge(_pl, 'left', on=['codcoligada', 'idperlet'])

        _serv = Sservico().df()[
            ['codcoligada', 'codservico', 'servico', 'codtdo']]

        df = df.merge(_serv, 'left', on=['codcoligada', 'codservico'])

        _bolsa = Sbolsa().df()[['codcoligada', 'codbolsa', 'nomebolsa']]

        df = df.merge(_bolsa, 'left', on=['codcoligada', 'codbolsa'])

        df.rename(columns={
            'dtinicio': 'datainiciobolsaaluno',
            'dtfim': 'datafimbolsaaluno',
            'datacancelamento': 'datacancelamentobolsaaluno',
            'ativa': 'bolsaativa'
        }, inplace=True)

        df = df.loc[df.codcontrato.notnull()]

        return df

    def re_order_cols(self):

        re_order = [
            'idbolsaaluno',
            'codbolsa',
            'nomebolsa',
            'codcoligada',
            'codfilial',
            'idperlet',
            'anoletivo',
            'ra',
            # 'bolsa100',
            # 'data_ini_ra',
            'datainiciobolsaaluno',
            'datafimbolsaaluno',
            'duracao_bolsa',
            # 'duracao_ra',
            'parcelainicial',
            'parcelafinal',
            'desconto',
            'tipodesc',
            'codcontrato',
            'dtcontrato',
            'dtassinatura',
            'codservico',
            'servico',
            'codtdo',
            'ordembolsa',
            'dataconcessao',
            'dataautorizacao',
            'datacancelamentobolsaaluno',
            'tetovalor',
            'ativa',
            'motivocancelamento',
            'bolsaretroativa',
            'idbolsaalunoorigem',
            'ordemaplicdescantecipacao',
            'afetabasecalculo',
            'obs',
        ]
        return re_order

    def trata_periodo_parcela(self, p1, p2):

        if (p1 != None) & (p2 != None):
            if p1 == p2:
                per = 1
            else:
                per = p2-p1+1
        else:
            per = 0
        return per


class Sbolsalan(GetTotvs):

    def __init__(self):
        super().__init__('sbolsalan')

    def df(self, columns=None):
        df = super().df_base(columns)

        df.rename(columns={
            'valor': 'valorbolsalan',
            'valorbaixa': 'valorbaixabolsalan'
        }, inplace=True)

        return df


class Sbolsapletivo(GetTotvs):

    def __init__(self):
        super().__init__('sbolsapletivo')

    def df(self, columns=None):
        df = super().df_base(columns)

        return df


class Sbolsacompl(GetTotvs):

    def __init__(self):
        super().__init__('sbolsacompl')

    def df(self, columns=None):
        df = super().df_base(columns)

        return df


class Sbolsafilial(GetTotvs):

    def __init__(self):
        super().__init__('sbolsafilial')

    def df(self, columns=None):
        df = super().df_base(columns)

        return df


class Gusuario(GetTotvs):

    def __init__(self):
        super().__init__('gusuario')

    def df(self, columns=None):
        df = super().df_base(columns)

        return df


class Gdic(GetTotvs):

    def __init__(self):
        super().__init__('gdic')

    def df(self, columns=None):
        df = super().df_base(columns)

        return df


class Setapas(GetTotvs):
    
    def __init__(self):
        super().__init__('setapas')

    def df(self, columns=None):
        df = super().df_base(columns)

        return df


class Snotas(GetTotvs):
    
    def __init__(self):
        super().__init__('snotas')

    def df(self, columns=None):
        df = super().df_base(columns)

        return df


class Snotaetapa(GetTotvs):
    
    def __init__(self):
        super().__init__('snotaetapa')

    def df(self, columns=None):
        df = super().df_base(columns)

        return df


class Sprovas(GetTotvs):
    
    def __init__(self):
        super().__init__('sprovas')

    def df(self, columns=None):
        df = super().df_base(columns)

        return df


class Sfrequencia(GetTotvs):

    def __init__(self):
        super().__init__('sfrequencia', True)

    def df(self, columns=None, thdp: bool = False):

        dfb = super().df_base(columns)

        return dfb


# *-----------------------------------------------------------------------------

# * Classes Join - Compostas

# *-----------------------------------------------------------------------------


class SmatricplStipomatricula(GetTotvs):
    """ Classe dedicada para um join cartesiano entre a SMATRICPL->CODTIPOMAT
    com a SLOGPLETIVO para que os dados _LPL sejam atribuidos do tipo de 
    matrícula corretamente
    """
    def __init__(self):
        super().__init__('smatricpl')

    def df(self, columns=None):
    
        _list_codcurso = ['1','2','3','4','5','EI','F1','F2','EM','0EI','1EF',
            '2EF','3EM','0EI.1','1EF.1', '1EF.2', '2EF.1', '2EF.2']

        cols_tm = [
            'codcoligada',
            'codfilial',
            'idperlet',
            'ra',
            'codtipomat',
            ]
        
        df_mpl = super().df_base()
        df_tmt = Stipomatricula().df()
        df_hbf = Shabilitacaofilial().df()
        df_mpl = df_mpl.merge(df_hbf, 'inner', on=['codcoligada', 'idhabilitacaofilial', 'codfilial'])
        df_mpl = df_mpl[df_mpl.codcurso.isin(_list_codcurso)]

        _df = df_mpl.merge(df_tmt, 'inner', on=['codcoligada', 'codtipomat'])
        
        return _df[cols_tm]


class SlogpletivoSmatricpl(GetTotvs):

    def __init__(self):
        super().__init__('slogpletivo')

    def df(self, columns=None):

        # _cols = [
        #    'codcoligada','codfilial','idhabilitacaofilial','idperlet','ra',
        #    'codstatus','codturma'
        # ]

        _cols = [
            'codcoligada',
            'idperlet',
            'idhabilitacaofilial',
            'ra',
            'codfilial',
            'codstatus'
        ]

        #_cols_lpl = _cols + ['dtalteracao', 'codstatusant', 'operacao']

        _cols_lpl = _cols + [
            'idlogpletivo',
            'dtalteracao',
            'codmotivo',
            'operacao',
            'codturma',
            'codstatusres',
            'periodo',
            'codstatusant',
            'codinstdestino',
            'codmotivotransferencia',
            'reccreatedby',
            'reccreatedon',
            'recmodifiedby',
            'recmodifiedon'
        ]

        #_cols_mpl = _cols + ['recmodifiedon']
        _cols_mpl = _cols + [
            'codtipomat',
            'dtmatricula',
            'dtmatriculaencerra',
            'numaluno',
            'reccreatedby',
            'reccreatedon',
            'recmodifiedby',
            'recmodifiedon'
        ]


        _cols_join = [
            'codcoligada',
            'codfilial',
            'codstatus',
            'idperlet',
            'ra',
            'idhabilitacaofilial'
        ]

        df_lpl = super().df_base(columns)
        df_lpl = df_lpl[_cols_lpl]
        
        df_mpl_tmt = SmatricplStipomatricula().df()
        df_lpl = df_lpl.merge(df_mpl_tmt,
                              how='inner',
                              on=['codcoligada', 'codfilial', 'idperlet', 'ra']
                              )
        

        df_lpl['data_key'] = df_lpl.dtalteracao.apply(
            lambda x: x.strftime('%Y%m%d%H%M')[:-1]).astype('int64')

        df_mpl = Smatricpl().df()
        df_mpl = df_mpl[_cols_mpl]

        df_mpl['data_key'] = df_mpl.recmodifiedon.apply(
            lambda x: x.strftime('%Y%m%d%H%M')[:-1]).astype('int64')

        _how_join = 'outer'
        _on_join = ['data_key'] + _cols_join

        _df = df_lpl.merge(
            df_mpl, how=_how_join, on=_on_join, indicator=True, suffixes=('_lpl', '_mpl'))

        _map_merge = {"left_only": "_LPL",
                      "right_only": "MPL_", "both": "_JOIN_"}

        _df['merge_str'] = _df._merge.map(_map_merge)

        """ Traz dados de grupos e coligadas par nascer ordenado já com dados
        não TOTVS como escola e unidade 
        """
        _gc_cols = [
            'codgrupo',
            'nomegrupo',
            'codescola',
            'escola',
            'unidade',
            'nomeunidade',
            'codcoligada',
            'coligada',
            'codfilial',
            'filial',
            'codmd5'
        ]
        gc = Zmdgrupocoligada().df()
        _df = _df.merge(gc, 'inner', ['codcoligada', 'codfilial'])

        """ Merge com Spletivo para trazer o ano letivo e calcular ano letivo
        inicial do RA
        """

        _cols_ple = ['codcoligada', 'codfilial', 'idperlet']
        ple = Spletivo().df()[_cols_ple + ['anoletivo', 'codtipocurso']]
        _df = _df.merge(ple, 'inner', _cols_ple)

        """ Merge com a status para o status principal. Depois haverá de se 
        fazer os status anterior e resultante 
        """

        _cols_sts = ['codcoligada', 'codstatus', 'codstatus_dp', 'status_dp',
                     'status_dp_ord', 'status_dp_val', 'status_dp_nome_col',
                     'status_dp_alg']
        sts = Sstatus().df()[_cols_sts]
        _df = _df.merge(sts, 'inner', ['codcoligada', 'codstatus'])


        """ Colunas que tratam o primeiro e o último ano letivo do aluno nos 
        diversos contextos. Esta análise tem que ser adequada, corrijida para 
        usar o codpessoa e não o RA para refletir uma relidade de captação
        ou retenção
        """
        
        _df = _df.set_index(['ra'])
        _df['anoletivo_min'] = _df.groupby(
            level=[0], dropna=False).agg({'anoletivo': 'min'})
        _df['anoletivo_max'] = _df.groupby(
            level=[0], dropna=False).agg({'anoletivo': 'max'})

        _df = _df.reset_index().set_index(['ra', 'codgrupo'])
        _df['anoletivo_min_g'] = _df.groupby(
            level=[0, 1], dropna=False).agg({'anoletivo': 'min'})
        _df['anoletivo_max_g'] = _df.groupby(
            level=[0, 1], dropna=False).agg({'anoletivo': 'max'})

        _df = _df.reset_index().set_index(['ra', 'codgrupo', 'unidade'])
        _df['anoletivo_min_gu'] = _df.groupby(
            level=[0, 1, 2], dropna=False).agg({'anoletivo': 'min'})
        _df['anoletivo_max_gu'] = _df.groupby(
            level=[0, 1, 2], dropna=False).agg({'anoletivo': 'max'})

        _df = _df.reset_index().set_index(['codcoligada', 'ra'])
        _df['anoletivo_min_c'] = _df.groupby(
            level=[0, 1], dropna=False).agg({'anoletivo': 'min'})
        _df['anoletivo_max_c'] = _df.groupby(
            level=[0, 1], dropna=False).agg({'anoletivo': 'max'})

        _df = _df.reset_index().set_index(['codcoligada', 'codfilial', 'ra'])
        _df['anoletivo_min_cf'] = _df.groupby(
            level=[0, 1, 2], dropna=False).agg({'anoletivo': 'min'})
        
        _df['anoletivo_max_cf'] = _df.groupby(
            level=[0, 1, 2], dropna=False).agg({'anoletivo': 'max'})

        _df = _df.reset_index()

        """Faz o bfill do codtipo mat para os registro que vem da LPL"""

        #_df = _df.sort_values(
        #    ['ra', 'idperlet', 'anoletivo', 'data_key', 'codtipomat'], na_position='first')
        
        _df['codtipomat_lpl'] = _df.codtipomat_lpl.fillna(_df.codtipomat_mpl)
        _df.rename(columns={'codtipomat_lpl': 'codtipomat'}, inplace=True)
        
        _df['tipomat_dp_anoletivo'] = _df.apply(
            lambda z: 'Matrícula' if z.anoletivo_min_g == z.anoletivo else 'Rematrícula', axis=1)
        
        
        re_order = [
            'data_key',
            'codgrupo',
            'nomegrupo',
            'codescola',
            'escola',
            'unidade',
            'nomeunidade',
            'coligada',
            'codcoligada',
            'coligada_rs',
            'codfilial',
            'filial_rs',
            'filial',
            'ra',
            'dtalteracao',
            'anoletivo',
            'codtipomat',
            'codtipomat_mpl',
            'tipomat_dp_anoletivo',
            'codtipocurso',
            'codstatus_dp',
            'status_dp',
            'status_dp_ord',
            'status_dp_val',
            'codmotivo',
            'codturma',
            'codstatusres',
            'periodo',
            'codstatusant',
            'codinstdestino',
            'codmotivotransferencia',
            'dtmatricula',
            'dtmatriculaencerra',
            'numaluno',
            'status_dp_alg',
            'anoletivo_min',
            'anoletivo_max',
            'anoletivo_min_g',
            'anoletivo_max_g',
            'anoletivo_min_gu',
            'anoletivo_max_gu',
            'anoletivo_min_c',
            'anoletivo_max_c',
            'anoletivo_min_cf',
            'anoletivo_max_cf',
            'reccreatedby_lpl',
            'reccreatedon_lpl',
            'recmodifiedby_lpl',
            'recmodifiedon_lpl',
            'reccreatedby_mpl',
            'reccreatedon_mpl',
            'recmodifiedby_mpl',
            'recmodifiedon_mpl',
            'status_dp_nome_col',
            'idperlet',
            'idhabilitacaofilial',
            'codstatus',
            'operacao',
            'idlogpletivo',
            '_merge',
            'merge_str',
            'codmd5',
        ]

        return _df[re_order]


class SalunoPpessoa(GetTotvs):

    def __init__(self):
        super().__init__('saluno')

    def df(self, columns=None):

        dfb = super().df_base()[[
            'codpessoa',
            'ra',
            'codcoligada',
            'codtipoaluno',
            'codinstorigem',
            'codcolcfo',
            'codcfo',
            'codparentcfo',
            'codpessoaraca',
            'codparentraca',
            'anoingresso',
        ]]

        return dfb
