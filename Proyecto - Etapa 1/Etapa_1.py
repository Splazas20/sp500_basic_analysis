"""
ETAPA_1

Miembros del Equipo
Claudia Marín, cc: 1016032591
Juan Sebastián Plazas, cc: 1072649946
Stefan Giraldo, cc: 1012394217

"""


#Importar las librerías
import pandas as pd
import yfinance as yf
import os
import logging

#Carpetas para guardar los datos y los logs en local
log_dir = './logs'
data_dir = './data'

#Crear las carpetas si no existen
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
if not os.path.exists(log_dir):
    os.makedirs(log_dir)


#Log crea un archivo que guarda los loggins información del sistema
log_filename = os.path.join(log_dir, 'etl_process.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

#Función que extrae datos de wikipedia S&P 500 y crea el ticker
def extract_sp500(url):
    try:
        logging.info(f'Extrayendo datos de: {url}')    
        #Leer datos de la red
        table_sp500 = pd.read_html(url)[0]
        sp500 = table_sp500[["Seguridad" , "Símbolo"]]
        logging.info('Los datos han sido extraidos exitosamente')    
        

        #guardar el dataframe en un archivo csv
        filename = os.path.join(data_dir, f'S&P_500_nombres.csv')
        logging.info("Guardando el dataframe en un archivo csv")
        sp500.to_csv(filename, encoding="utf-8-sig", index=False, header=True)
        logging.info("Archivo csv guardado exitosamente")


        #Símbolos a lista para usarlo como ticker
        logging.info("Crear lista de Símbolos para usar como ticker")
        sp_symbols = table_sp500["Símbolo"].tolist()
        logging.info("Lista creada correctamente")

        return  sp_symbols
 
    except Exception as e:
        logging.error(f'Error extrayendo datos de {url}: {e}')
        return None
    

#Función para extraer los datos
def extract_data(ticker, start_date, end_date):
    try:
        logging.info(f'Extrayendo datos para {ticker} desde {start_date} hasta {end_date}')
        data = yf.download(ticker, start=start_date, end=end_date)
        logging.info(f'Datos extraídos exitosamente para {ticker}')
        return data
    except Exception as e:
        logging.error(f'Error extrayendo datos para {ticker}: {e}')
        return None

#Función para transformar los datos
def transform_data(data):
    
    try:     
        logging.info('Transformando datos')
        transform_data = data['Adj Close'].reset_index().melt(id_vars=['Date'], value_vars=tickers, var_name='Symbol', value_name='Close')
        transform_data = transform_data.dropna()
        logging.info('Datos transformados exitosamente')        
        return transform_data

    except Exception as e:
        logging.error(f'Error transformando datos: {e}')
        return None


#Función para cargar los datos
def load_data(df):
    try:
        filename = os.path.join(data_dir, f'S&P500_prices_proccessed.csv')
        logging.info(f'Guardando datos transformados en {filename}')
        df.to_csv(filename, index=False)
        logging.info('Datos guardados exitosamente')
    except Exception as e:
        logging.error(f'Error guardando datos: {e}')

#Esta función unifica todas las funciones anteriores
def etl_process(ticker, start_date, end_date):
    data = extract_data(ticker, start_date, end_date)
    if data is not None:
        transformed_data = transform_data(data)
        if transformed_data is not None:
            load_data(transformed_data)
            return transformed_data
    return None


url_sp = "https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500"
tickers = extract_sp500(url_sp)
start_date = '2024-01-01' 
end_date = '2024-03-31'
etl_process(tickers, start_date, end_date)


#   /\_/\  (
#  ( ^.^ ) _)
#    \"/  (
#  ( | | )
# (__d b__)