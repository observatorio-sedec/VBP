import datetime
import pandas as pd
import requests as rq  



ano_atual = datetime.datetime.now().year
mes_atual = datetime.datetime.now().month
url = f"https://www.gov.br/agricultura/pt-br/assuntos/politica-agricola/arquivos-vbp/DASHBOARDVBP{ano_atual}07.xlsx"

response = rq.get(url)
if response.status_code == 200:
    with open('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\VBP\\dados_vbp.xlsx', 'wb') as file:
        file.write(response.content)
else:
    print(f"Erro ao baixar o arquivo: {response.status_code}")
    
df_vbp = pd.read_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\VBP\\dados_vbp.xlsx', sheet_name='BASE')
df_vbp = df_vbp[df_vbp['COD UF'] != 'BR']
df_vbp['Ano'] = pd.to_datetime('01' + '/' + '01' + '/' + df_vbp['Ano'].astype(str) , format='%d/%m/%Y', utc=False)
df_vbp['Ano'] = df_vbp['Ano'].dt.date
df_vbp = df_vbp.rename(columns={
    'COD UF': 'UF',
    'Ano':'Data',
    'UF REGIÕES.NOME UF': 'ESTADO',
    'UF REGIÕES.REGIÃO': 'REGIÃO'})
df_vbp['milhões R$'] = df_vbp['milhões R$'] * 1000000
df_vbp. drop(columns=['ESTADO', 'Valor'], inplace=True)
df_vbp = df_vbp.rename(columns={'milhões R$': 'Valor'})
print(df_vbp.info())

df_vbp.to_excel("ajustado.xlsx", index=False)
print(df_vbp.head())
if __name__ == "__main__":
    from sql import executar_sql
    executar_sql()