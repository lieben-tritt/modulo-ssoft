class Coligada:
    
    def __init__(self, _df):
        
        self.df = _df
    
    def get_coligada_by_cod(
        self, codcoligada, codfilial=None, anoletivo=None, colunas=None
        ):
        """Carrega o dataframe da coligada [e filial] especificada

        Args:
            codcoligada (int ou list): codigo do(s) grupos(s)
            codfilial (int ou list): codigo da filial da coligada
            anoletivo (opcional): [list de anos letivos] ou anoletivo int.
            colunas (opcional): define quais as colunas serã expostas

        Returns:
            dataframe: retorna o dataframe do código da coligada especificada
        """
        _df = self.df

        if isinstance(codcoligada, list):
            _df = _df[_df.codcoligada.isin(codcoligada)]        
        elif isinstance(codcoligada, tuple):
            _df = _df[_df.codcoligada.isin(list(codcoligada))]
        else:
            _df = _df[_df.codcoligada==codcoligada]

        if isinstance(codfilial, list):
            _df = _df[_df.codcoligada.isin(codfilial)]        
        elif isinstance(codfilial, tuple):
            _df = _df[_df.codcoligada.isin(list(codfilial))]
        else:
            _df = _df[_df.codcoligada==codfilial]

        if anoletivo:
            if isinstance(anoletivo, list):
                _df = _df[_df.anoletivo.isin(anoletivo)]
            else:
                _df = _df[_df.anoletivo==anoletivo]
        
        _filtro_cols = _df.columns.to_list() 

        if colunas:
            if isinstance(colunas, list):
                _filtro_cols = colunas
            elif isinstance(colunas, tuple):
                _filtro_cols = list(colunas)
            else:
                pass

        return _df.filter(_filtro_cols)

class Escola:

    def __init__(self, _df):
        
        self.df = _df
    
    def get_escola_by_cod(self, codescola, anoletivo=None, colunas=None):
        """Carrega o dataframe da escola especificada

        Args:
            codescola (int ou list): codigo do(s) grupos(s)
            anoletivo (opcional): [list de anos letivos] ou anoletivo int.

        Returns:
            dataframe: retorna o dataframe do código da escola especificada
        """
        _df = self.df

        if isinstance(codescola, list):
            _df = _df[_df.codescola.isin(codescola)]        
        elif isinstance(codescola, tuple):
            _df = _df[_df.codescola.isin(list(codescola))]
        else:
            _df = _df[_df.codescola==codescola]

        if anoletivo:
            if isinstance(anoletivo, list):
                _df = _df[_df.anoletivo.isin(anoletivo)]
            else:
                _df = _df[_df.anoletivo==anoletivo]
        
        _filtro_cols = _df.columns.to_list() 

        if colunas:
            if isinstance(colunas, list):
                _filtro_cols = colunas
            elif isinstance(colunas, tuple):
                _filtro_cols = list(colunas)
            else:
                pass

        return _df.filter(_filtro_cols)
    
    def get_grupo_by_ra(self, ra, anoletivo=None, colunas=None):
        """Carrega o dataframe do RA com o(s) grupo(s) ao qual esteve vinculado
        ou está vicnulado

        Args:
            ra (str ou list): codigo do registro academico
            anoletivo (optional): [list de anos letivos] ou anoletivo int.

        Returns:
            dataframe: retorna o dataframe do código da escola especificada
        """
        _df = self.df
        
        if isinstance(ra, list):
            _df = _df[_df.ra.isin(ra)]        
        elif isinstance(ra, tuple):
            _df = _df[_df.ra.isin(list(ra))]
        else:
            _df = _df[_df.ra==ra]

        if anoletivo:
            if isinstance(anoletivo, list):
                _df = _df[_df.anoletivo.isin(anoletivo)]
            else:
                _df = _df[_df.anoletivo==anoletivo]
        
        _filtro_cols = _df.columns.to_list() 

        if colunas:
            if isinstance(colunas, list):
                _filtro_cols = colunas
            elif isinstance(colunas, tuple):
                _filtro_cols = list(colunas)
            else:
                pass

        return _df.filter(_filtro_cols)
    
    def get_grupo_status(self, grouped: bool=False, anoletivo=None):
        """Retorna uma tabela com os o estoque de matriculados em cada grupo no
        ano letivo corrente

        Args:
            grouped (bool, optional): Se verdadeiro retorna uma tabela agrpada.
            anoletivo (list, tuple ou int): o(s) ano(s) letivo(s) a ser(em) 
            considerado(s)
        Returns:
            dataframe: retorna um dataframe
        """
        _base_key = ['codcoligada', 'codfilial', 'idperlet', 'ra']

        _df = self.df

        if anoletivo:
            if isinstance(anoletivo, list):
                _df = _df[_df.anoletivo.isin(anoletivo)]
            if isinstance(anoletivo, tuple):
                _df = _df[_df.anoletivo.isin(list(anoletivo))]
            else:
                _df = _df[_df.anoletivo==anoletivo]
        else:
            # pega o ano correntE como ano letivo
            from datetime import datetime
            anoletivo = datetime.now().year
            _df = _df[_df.anoletivo==anoletivo]

        _df = _df.loc[
            (_df.codtipocurso_dp == 0)
            & (_df.operacao != 'E') 
            & (_df.codstatus_dp != 4) #! Não pega remanejados
            ]

        _df.insert(4, 'max_data', _df.groupby(
            _base_key).data_lpl.transform('max')==_df.data_lpl)

        # Para anos letivos anteriores ao ano atual, pegas os status de 
        # reprovados e aprovados

        _df_md = _df.loc[
            (_df.max_data==True) 
            & (_df.codstatus_dp.isin([3, 9, 10]))
            #& (_df.anoletivo==anoletivo)
            ]

        if grouped:
            _df_md = _df_md.groupby([
                'codescola', 'escola'],
                as_index=False
                ).agg(ra_log=('ra','nunique'))

        return _df_md.sort_values('escola')        
