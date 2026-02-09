import datetime
import requests as rq  
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple


def verificar_url(ano: int, mes: int) -> Optional[Tuple[str, int, int]]:
    url = f"https://www.gov.br/agricultura/pt-br/assuntos/politica-agricola/arquivos-vbp/DASHBOARDVBP{ano}{mes:02d}.xlsx"
    try:
        response = rq.get(url, timeout=5)
        if response.status_code == 200:
            print(f"✓ URL encontrada: {ano}/{mes:02d}")
            return (url, ano, mes)
        else:
            print(f"✗ URL não disponível: {ano}/{mes:02d} (Status: {response.status_code})")
            return None
    except Exception as e:
        print(f"✗ Erro ao verificar {ano}/{mes:02d}: {e}")
        return None


def encontrar_ultima_url() -> Optional[Tuple[str, int, int]]:
    """
    Encontra a última URL disponível testando em paralelo múltiplas combinações.
    
    """
    ano_atual = datetime.datetime.now().year
    mes_atual = datetime.datetime.now().month
    
    # Gerar lista de candidatos (últimos 24 meses)
    candidatos = []
    for i in range(24):
        data_teste = datetime.datetime(ano_atual, mes_atual, 1) - datetime.timedelta(days=30 * i)
        candidatos.append((data_teste.year, data_teste.month))
    
    print(f"\n🔍 Procurando última versão disponível dos dados VBP...")
    print(f"📅 Testando {len(candidatos)} URLs em paralelo...\n")
    
    # Verificar URLs em paralelo
    urls_encontradas = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(verificar_url, ano, mes): (ano, mes) for ano, mes in candidatos}
        
        for future in as_completed(futures):
            resultado = future.result()
            if resultado:
                urls_encontradas.append(resultado)
    
    if not urls_encontradas:
        print("\n❌ Nenhuma URL disponível encontrada!")
        return None
    
    # Ordenar por ano e mês (mais recente primeiro)
    urls_encontradas.sort(key=lambda x: (x[1], x[2]), reverse=True)
    url_final, ano_final, mes_final = urls_encontradas[0]
    
    print(f"\n✅ Última versão encontrada: {ano_final}/{mes_final:02d}")
    print(f"🔗 URL: {url_final}\n")
    
    return url_final, ano_final, mes_final


def baixar_dados(url: str) -> bool:
    try:
        response = rq.get(url, timeout=30)
        
        if response.status_code == 200:
            with open('dados_vbp.xlsx', 'wb') as file:
                file.write(response.content)
            print(f"✅ Arquivo baixado com sucesso!")
            return True
        else:
            print(f"❌ Erro ao baixar o arquivo: Status {response.status_code}")
            return False
            
    except rq.exceptions.RequestException as e:
        print(f"❌ Erro ao baixar o arquivo: {e}")
        return False


def processar_dados() -> pl.DataFrame:
    
    df_vbp = pl.read_excel('dados_vbp.xlsx', sheet_name='BASE')
    
    df_vbp = df_vbp.filter(pl.col('COD UF') != 'BR')
    
    df_vbp = df_vbp.with_columns(
        pl.concat_str([
            pl.lit('01/01/'),
            pl.col('Ano').cast(pl.Utf8)
        ]).str.strptime(pl.Date, format='%d/%m/%Y').alias('Ano')
    )
    
    df_vbp = df_vbp.rename({
        'COD UF': 'UF',
        'Ano': 'Data',
        'UF REGIÕES.NOME UF': 'ESTADO',
        'UF REGIÕES.REGIÃO': 'REGIÃO'
    })
    
    df_vbp = df_vbp.with_columns(
        (pl.col('milhões R$') * 1000000).alias('milhões R$')
    )
    
    df_vbp = df_vbp.drop(['ESTADO', 'Valor'])
    
    df_vbp = df_vbp.rename({'milhões R$': 'Valor'})
    
    print(f"✅ Dados processados: {df_vbp.shape[0]} linhas, {df_vbp.shape[1]} colunas")
    
    return df_vbp


def main():
    
    resultado = encontrar_ultima_url()
    if not resultado:
        print("❌ Não foi possível encontrar dados disponíveis.")
        return
    
    url, ano, mes = resultado
    
    if not baixar_dados(url):
        print("❌ Falha no download dos dados.")
        return
    
    df_vbp = processar_dados()
    
    print(f"\n📋 Preview dos dados:")
    print(df_vbp.head(10))
    
    print("✅ ETL concluído com sucesso!")
    
    return df_vbp


df_vbp = main()
if __name__ == "__main__":
    try:
        from sql import executar_sql
        executar_sql()
    except ImportError:
        print("\n⚠️  Módulo 'sql' não encontrado. Pulando execução SQL.")