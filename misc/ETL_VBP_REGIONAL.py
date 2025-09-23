import pandas as pd 
import openpyxl
from ajustar_planilha import ajustar_bordas, ajustar_colunas
from Drive import add_arquivos_a_pasta
from Google import Create_Service

df_Regional = pd.read_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\VBP\\202312VBPREGIONAL1.xlsx', sheet_name='VBP UFs')

for linhas in range(0, 433):
    df_Regional = df_Regional.drop(index=linhas)

for linhas in range(465, 548):
    df_Regional = df_Regional.drop(index=linhas)
     
for i in range(1, 12):
    df_Regional.drop(columns=f'Unnamed: {i}', inplace=True)


df_Regional.drop(columns="VALOR BRUTO DA PRODUÇÃO AGROPECUÁRIA", inplace=True)
df_Regional = df_Regional.drop(index=433)

valores_colunas = df_Regional.iloc[0].tolist()    
coluna_original = 'VALOR BRUTO DA PRODUÇÃO AGROPECUÁRIA.1'
indice_coluna = df_Regional.columns.get_loc(coluna_original)
df_Regional = df_Regional.rename(columns={coluna_original: valores_colunas[indice_coluna]})

for i in range(0, 11):
    df_Regional.rename(columns={df_Regional.columns[i]: f'Unnamed: {i}'}, inplace=True)

for i in range(0, 11):
    df_Regional.rename(columns={f'Unnamed: {i}': valores_colunas[i]},  inplace=True)
    
df_Regional = df_Regional.drop(index=[434, 442, 443, 457, 463])
for linhas in df_Regional.columns:
    df_Regional[linhas] = df_Regional[linhas].replace('-', 0).replace('Café Total', 'Café').replace('Total Lav.+ Pec.', 'VBP TOTAL')
    
colunas_numericas = df_Regional.select_dtypes(include='number' )
df_Regional[colunas_numericas.columns] = colunas_numericas.round(decimals=2)
pd.set_option('display.float_format', lambda x: '%.2f' % x) 

df_Regional.rename(columns={'LAVOURAS': 'PRODUTOS'},inplace=True)
for colunas in df_Regional.columns:
    df_Regional[colunas] = df_Regional[colunas].astype(str)

df_Regional = df_Regional.drop(index=464)
df_Regional = df_Regional.melt(id_vars='PRODUTOS', var_name='Data', value_name='Valor')
df_Regional.reset_index(drop=True, inplace=True)
lista_pecuaria = ['Frango', 'Bovinos','Ovos', 'Leite', 'Suínos']
for indice, valor in df_Regional.iterrows():
    for pecu in lista_pecuaria:
        if valor['PRODUTOS'] == pecu:
            df_Regional.at[indice, 'Categoria'] = 'Pecuária'
            
df_Regional['Categoria'] = df_Regional['Categoria'].fillna('Lavouras')
df_Regional['Data'] = '01/01/' + df_Regional['Data'].astype(str)
# print(df_Regional)
df_Regional.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\VBP\\VBP COMPLETO MATO GROSSO.xlsx', index=False, header=1)
wb_VBP = openpyxl.load_workbook('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\VBP\\VBP COMPLETO MATO GROSSO.xlsx')

ws_VBP = wb_VBP.active
ajustar_colunas(ws_VBP)
ajustar_bordas(wb_VBP)
wb_VBP.save('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\TABELAS\\TABELAS EM CSV\\VBP COMPLETO MATO GROSSO.xlsx')

# Faz autenticação do google drive para jogar os arquivos gerados
CLIENT_SECRET_FILE = 'credencials.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ["https://www.googleapis.com/auth/drive"]

service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

#PASSA O ARQUIVO PARA O DRIVE
file_id = "1e2yihre6trC07ai7IayhrvLrFP4c57za"
FILE_NAMES = ["VBP COMPLETO MATO GROSSO.xlsx"]
MIME_TYPES = ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]

add_arquivos_a_pasta(FILE_NAMES, MIME_TYPES, service, file_id)