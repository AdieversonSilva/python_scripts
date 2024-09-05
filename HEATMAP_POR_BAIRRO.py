# IMPORT NECESSARIOS PARA TRABALHAR
import psycopg2 as psgt # CONEXÃO COM BANCO POSTGRESQL
import pandas as pd # TRATAMENTO DE DADOS
import seaborn as sns # MANIPULAÇÃO DE GRAFICOS
import matplotlib as mpl # MANIPULAÇÃO DE GRAFICOS
import matplotlib.pyplot as plt # MANIPULAÇÃO DE GRAFICOS
from datetime import date, datetime # MANIPULAÇÃO DE DATA E HORA
import os # PARA CRIAR CAMINHOS 
# print('Import Successful')


def gerar_heatmaps():
    def heat_map(data, titulo_plot, nome_img):
        
        hoje = datetime.today().strftime('%d-%m-%Y') # ESTABELECENDO VARIAVEL DATA DE HOJE
        directory = f"{titulo_plot}//{hoje}" # DIRETORIO A SER CRIADO COM DATA DE HOJE
        parent_dir = "" # LOCAL DA PASTA A SER CRIADA
        caminho =  os.path.join(parent_dir, directory) # CONCATENANDO O CAMINHO 
        os.makedirs(caminho) # CRIANDO AS PASTAS
        
        sns.set_theme() # TEMA DO GRAFICO

        # TRANSFORMANDO AS INFORMAÇÕES DO DATASET EM DATA LONGFORM
        datap = data.pivot(index="numerodia",columns="hora",values="qtd")
        
        # FAZENDO O HEATMAP TER EM CADA CELULA UM NÚMERO EM QUANTIDADE
        f, ax = plt.subplots(figsize=(15, 10))
        graph = sns.heatmap(datap, annot=True, fmt=".0f", linewidths=1.5, ax=ax, cmap="rocket_r", cbar=False)
        plt.title(titulo_plot)
        plt.xticks(rotation = "horizontal")
        plt.ylabel("DIA DA SEMANA")
        plt.yticks(ticks= [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5], labels=['SEG', 'TER', 'QUA', 'QUI', 'SEX', 'SAB', 'DOM'])
        plt.yticks(rotation = "horizontal")
        plt.xlabel("HORA DO DIA")
        plt.savefig(f"{caminho}/{nome_img}-{hoje}.svg", dpi = 300, bbox_inches='tight')
        plt.close('all');
   
    def show_psycopg2_exception(err):
        
        # COLETA OS DETALHES DA EXCEÇÃO
        err_type, err_obj, traceback = sys.exc_info()
        
        # COLETA O NÚMERO DA LINHA QUANDO HÁ EXCEÇÃO
        line_n = traceback.tb_lineno
        
        # print O ERRO DA FUNÇÃO DE CONEXÃO(CONEXAO)
        print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
        print ("psycopg2 traceback:", traceback, "-- type:", err_type)
        
        # psycopg2 extensions.Diagnostics object attribute - DIAGNOSTICOS DE ERRO
        print ("\nextensions.Diagnostics:", err.diag)
       
        # print O pgcode E O pgerror exceptions
        print ("pgerror:", err.pgerror)
        print ("pgcode:", err.pgcode, "\n")
    
    def conexao(host, user, dbname, password):
        conn = None
        try:
            print(f'Conectando com o banco {dbname}-{host}......')
            conn_string = f'host={host} user={user} dbname={dbname} password={password}'
            conn = psgt.connect(conn_string)
            print(f"Conexão efetuada com sucesso com o banco {dbname}-{host}......")
        
        except psgt.OperationalError as err:
            # COLOCANDO A EXCEÇÃO NA FUNÇÃO
            show_psycopg2_exception(err)
            # EM CASO DE ERRO SETANDO CONN PARA NONE
            conn = None
            
        return conn
    ##########################################################################################################
    # ESTABAELCENDO PARAMETROS COM A FUNÇÃO DE CONEXÃO COM INFORMAÇÕES DO BANCO
    conn = conexao(host="", user="", dbname="", password="")
    
    # SE CONEXÃO DIFERENTE DE NONE RODAR QUERY
    if conn != None:
        try:
            
            # SALVANDO QUERY EM VARIAVEL
            sql_query = """ WITH cte_dias
            AS (
                SELECT CASE 
                        WHEN dia_semana_fato = 'SEG'
                            THEN '01'
                        WHEN dia_semana_fato = 'TER'
                            THEN '02'
                        WHEN dia_semana_fato = 'QUA'
                            THEN '03'
                        WHEN dia_semana_fato = 'QUI'
                            THEN '04'
                        WHEN dia_semana_fato = 'SEX'
                            THEN '05'
                        WHEN dia_semana_fato = 'SAB'
                            THEN '06'
                        WHEN dia_semana_fato = 'DOM'
                            THEN '07'
                        ELSE '-'
                        END AS dia_ord
                    ,dbr.dia_semana_fato AS dia_s
                FROM sisp.dim_bop_reg dbr
                GROUP BY 1
                    ,2
                ORDER BY 1
                )
                ,cte_hora
            AS (
                SELECT dbr.hora_fato AS hora
                FROM sisp.dim_bop_reg dbr
                GROUP BY 1
                )
                ,cte_bai
            AS (
                SELECT dbr.ds_bairro_fato || ' - ' || dbr.localidade_fato AS bai
                FROM sisp.dim_bop_reg dbr
                WHERE DBR.risp_fato = '02ª RISP'
                    --AND dbr.ds_bairro_fato in ('UNA', 'STA.CLARA') --filtro para teste
                GROUP BY 1
                )
                ,cte_master
            AS (
                SELECT cd.dia_ord
                    ,cd.dia_s
                    ,ch.hora
                    ,cb.bai
                FROM cte_dias cd
                LEFT JOIN cte_hora ch ON 1 = 1
                LEFT JOIN cte_bai cb ON 1 = 1
                )
                ,cte_metrica
            AS (
                SELECT dbr.ds_bairro_fato || ' - ' || dbr.localidade_fato AS bairro
                    ,dbr.dia_semana_fato AS dia
                    ,dbr.hora_fato AS hora
                    ,count(DISTINCT dbr.n_bop) AS qtd
                FROM sisp.dim_bop_reg dbr
                WHERE dbr.ano_fato = '2023'
                    AND dbr.crime = 'ROUBO'
                    AND DBR.risp_fato = '02ª RISP'
                GROUP BY 1
                    ,2
                    ,3
                ORDER BY 1
                )
            SELECT cm.bai
                ,cm.dia_ord
                ,cm.dia_s
                ,cm.hora
                ,coalesce(cm2.qtd, '0') AS qtd
            FROM cte_master cm
            LEFT JOIN cte_metrica cm2 ON cm.dia_s = cm2.dia
                AND cm.hora = cm2.hora
                AND cm.bai = cm2.bairro"""
           
            # SETANDO CONN A CURSOR PARA EXECUTAR QUERY
            cur = conn.cursor()
            
            # EXECUTANDO QUERY
            cur.execute(sql_query)
            print(f"Query efetuada com sucesso......")

        # SE CONEXÃO DER NONE ENTÃO VAI SETAR ERRO E QUERY NÃO VAI RODAR    
        except psgt.OperationalError as err:
            # COLOCANDO A EXCEÇÃO NA FUNÇÃO
            show_psycopg2_exception(err)
            # EM CASO DE ERRO SETANDO CONN PARA NONE
            conn = None
    
    ##########################################################################################################
    # TRANSFORMANDO EM DATAFRAME A RESPOSTA DA QUERY
    df = pd.DataFrame(cur.fetchall())
    
    # FECHANDO CONEXÃO E CURSOR
    cur.close()
    conn.close()
    
    # RENOMEANDO COLUNAS
    df.columns = ["bairro", "numerodia", "dia", "hora", "qtd"]
    print("Tabela carregada com sucesso......")
    ##########################################################################################################
    # GERANDO TUPLA COM NOMES UNICOS DOS BAIRROS (GROUP BY)
    print("Gerando lista de bairros......")
    bai = df.bai.unique()
    print("Lista de bairros gerada com sucesso......")
    
    ##########################################################################################################
    # LENDO A TUPLA COM NOME DOS BAIRROS
    print("Criando tabela com heatmaps......")
    for b in bai:
        
        # SALVANDO EM UM DATAFRAME COM AS ULTIMAS 4 COLUNAS
        a = df.loc[df.bai == b, 'numerodia':'qtd']
        print(f"Gerando tabela com heatmaps do bairro {b}......")
        
        # GERANDO PLOT PARA CADA BAIRRO
        heat_map(data = a, titulo_plot = b, nome_img = b)
        print(f"Tabela gerada com sucesso da {b}......")
    print("Finalizado com sucesso todos os passos......")  
if __name__ == "__main__": # RODAR O .py COMO EXECUTAVEL
    gerar_heatmaps()
