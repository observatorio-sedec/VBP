from ETL_VBP import df_vbp
import psycopg2
from conexao import conexao

def executar_sql():
    
    cursor = conexao.cursor()
    cursor.execute('SET search_path TO vbp, public')
    
    criando_tabela_estadual = f'''
        CREATE TABLE IF NOT EXISTS vbp.vbp_producao (
            Estado TEXT,
            Produto TEXT, 
            Data_att DATE,
            Valor_VBP NUMERIC,
            Categoria TEXT, 
            Regiao TEXT
            );
        '''
    cursor.execute(criando_tabela_estadual)
    
    verificando_existencia_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_name='vbp_producao';
    '''
    
    cursor.execute(verificando_existencia_estadual)
    resultado_estadual = cursor.fetchone()
    

    if resultado_estadual[0] == 1:
        dropando_tabela_estadual = '''
        TRUNCATE vbp.vbp_producao;
        '''
        cursor.execute(dropando_tabela_estadual)
        
    inserindo_dados_estadual = '''
    INSERT INTO vbp.vbp_producao (Estado, Produto, Data, Valor_VBP, Categoria, Regiao)
    VALUES (%s, %s, %s, %s, %s, %s);
    '''
    try:
        for idx, i in df_vbp.iterrows():
            dados = (
                i['UF'],
                i['PRODUTO'],
                i['Data'],
                i['Valor'],
                i['CATEGORIA'],
                i['REGIÃO']
            )
            cursor.execute(inserindo_dados_estadual, dados)
            conexao.commit()

    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")


    conexao.commit()
    conexao.close()
