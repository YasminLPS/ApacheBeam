import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
    'id',
    'data_iniSE',
    'casos',
    'ibge_code',
    'cidade',
    'uf',
    'cep',
    'latitude',
    'longitude'
]

def list_for_dicionary(element, colunas):
    """
    Recebe duas listas
    Retorna um dicionario    
    """
    
    return dict(zip(colunas, element))
    

def text_for_list(element, delimitador='|'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador    
    """
    
    return element.split(delimitador)
 
def trata_data(element):
    """
    Recebe um dicionario e cria um novo campo com ANO-MES
    Retorna o mesmo dicionario com novo campo  
    """   
    #"2016-08-01" >> ['2016', '08'] >> "2016-08"
    element['ano_mes'] = '-'.join(element['data_iniSE'].split('-')[:2])
    return element

def chave_uf(element):
    """
    Recebe um dicionario
    Retorna uma tupla com estadio e o elemento (UF, dicionario)  
    """   
    chave = element['uf']
    return (chave, element)

def casos_dengue(element):
    """
    Recebe uma tupla ('UF', [{}, {}])
    Retorna uma tupla ('UF-ANO-MES', quantidade de casos de dengue) 
    """   
    #varivel uf recebeu o ('UF') e a variavel registros recebeu o [{}, {}])
    uf, registros = element
    for registro in registros:
        #verifica se no campo 'casos' possui um float
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)
            
def chave_chuva(element):
    """
    Recebe uma lista de elementos
    Retorna uma tupla ('UF-ANO-MES', valor da chuva em mm) 
    """ 
    
    data, mm, uf = element
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f"{uf}-{ano_mes}"
    
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
        
    return chave, mm

def arredonda(element):
    """
    Recebe uma tupla ('RO-2019-10', 445.3999999999997)
    Retorna uma tupla com o valor arredondado ('RO-2019-10', 445.3)
    """ 
    chave, mm = element
    return (chave, round(mm, 1))

def filtra_campos_vazios(elemento):
    """
    Remove elementos que tenham chaves vazias
    Recebe uma tupla ex:('CE-2015-01',{'chuvas': [85.8], 'dengue': [17.0]})
    Retorna uma tupla ('CE-2015-01',{'chuvas': [85.8], 'dengue': [17.0]})
    """
    chave, dados = elemento
    if all([
        dados['chuvas'],
        dados['dengue']
    ]):
        return True
    return False

def descompactar_elementos(elemento):
    """
    Receber uma tupla ('CE-2015-01',{'chuvas': [85.8], 'dengue': [17.0]})
    Retorna uma tupla ('CE','2015','01','85.8','17.0')
    """
    chave, dados = elemento
    chuva = dados['chuvas'][0] #Pega o elemento  sem os '[]'
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    
    return uf, ano, mes, str(chuva), str(dengue)

def preparar_csv(elemento, delimitador=','):
    """
    Receber uma tupla ('CE',2015,01,85.8,17.0)
    Retorna uma string delimitada "CE,2015,01,85.8,17.0"
    """
    return f"{delimitador}".join(elemento)

dengue = (
    pipeline 
    | "Leitura do dataset de dengue" >> 
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(text_for_list)
    | "De lista para dicionario" >> beam.Map(list_for_dicionary, colunas_dengue)
    | "Criar campo ano-mes" >> beam.Map(trata_data)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue) #flatmap permite que use o yield
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    #| "Mostrar resultados" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> 
        ReadFromText('chuvas.csv', skip_header_lines=1)
    | "De texto para lista (chuvas)" >> beam.Map(text_for_list, delimitador=',')
    | "Criar chave uf-ano_mes" >> beam.Map(chave_chuva)
    | "Soma do total de chuvas pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar resultados de chuvas" >> beam.Map(arredonda)
    #| "Mostrar resultados de chuvas" >> beam.Map(print)
)

resultado = (
    #(chuvas,dengue)
    #| "Empilha as pcols" >> beam.Flatten() #Pega as pcolletions e faz o empilhamento, um em cima da outra
    #| "Agrupa as pcols " >> beam.GroupByKey()
    ({'chuvas': chuvas, 'dengue':dengue})
    | "Mesclar pcols" >> beam.CoGroupByKey()
    | "Filtrar dados vazios " >>  beam.Filter(filtra_campos_vazios)
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    | "Preparar CSV" >>  beam.Map(preparar_csv)
    #| "Mostrar resultados da união" >> beam.Map(print)
)

header = 'UF,ANO,MES,CHUVA,DENGUE'

resultado | 'Criar arquivo CSV' >> WriteToText('resultado', file_name_suffix='.csv', header=header)

pipeline.run()