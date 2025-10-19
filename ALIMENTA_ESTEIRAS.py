import pyodbc
import json
import requests
import sys
from datetime import date, timedelta

# ==============================================================================
# --- FUNÇÕES AUXILIARES ---
# ==============================================================================

def carregar_config(caminho_arquivo='config.json'):
    """Lê as configurações do arquivo JSON."""
    try:
        with open(caminho_arquivo, 'r') as f:
            print(f"Lendo configurações de '{caminho_arquivo}'...")
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO CRÍTICO: Arquivo de configuração '{caminho_arquivo}' não encontrado.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"ERRO CRÍTICO: O arquivo '{caminho_arquivo}' não é um JSON válido.")
        sys.exit(1)

def enviar_notificacao_teams(webhook_url, titulo, mensagem, sucesso=True):
    """Envia uma notificação para um canal do Microsoft Teams."""
    if not webhook_url or webhook_url == "URL_DO_SEU_WEBHOOK_DO_TEAMS_AQUI":
        print("AVISO: Webhook do Teams não configurado. Notificação não enviada.")
        return
    cor_tema = "00FF00" if sucesso else "FF0000"
    payload = {"@type": "MessageCard", "@context": "http://schema.org/extensions", "themeColor": cor_tema, "summary": titulo, "sections": [{"activityTitle": titulo, "activitySubtitle": "Automação de Procedures Diárias", "facts": [{"name": "Status", "value": mensagem}], "markdown": True}]}
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        print("Notificação enviada para o Teams com sucesso.")
    except requests.exceptions.RequestException as e:
        print(f"ERRO ao enviar notificação para o Teams: {e}")

def executar_procedure(cursor, nome_procedure, data_execucao: date):
    """Executa UMA stored procedure para uma data específica e retorna o status."""
    try:
        print(f"   -> Tentando executar {nome_procedure} para {data_execucao}...")
        
        # Envia a data como parâmetro tipado
        cursor.execute(
            f"EXEC {nome_procedure} @DataExecucao = ?",
            data_execucao
        )
        
        print(f"      SUCESSO: {nome_procedure} executada.")
        return True
    except pyodbc.Error as ex:
        print(f"      FALHA ao executar {nome_procedure} para {data_execucao}. SQLSTATE: {ex.args[0]}")
        print(f"      Mensagem: {ex}")
        return False

# ==============================================================================
# --- LÓGICA PRINCIPAL DO SCRIPT ---
# ==============================================================================

def main():
    """
    Função principal que orquestra a execução.
    Verifica dias faltantes para CADA processo definido no config
    e executa a procedure correspondente.
    """
    config = carregar_config()
    db_config = config['conexao_banco']
    lista_processos = config['objetos_banco'].get('processos', [])
    teams_webhook = config.get('notificacoes', {}).get('teams', {}).get('webhook_url')

    enviar_notificacao_teams(teams_webhook, "Início do Processo", "Iniciando verificação de dias faltantes para múltiplos processos.")

    conn = None
    # Resultados agora armazenam tuplas (Processo, Dia)
    resultados = {"sucesso": [], "falha": [], "erros_gerais": ""}
    
    if not lista_processos:
        msg_erro = "ERRO CRÍTICO: Nenhuma 'processo' definido em 'objetos_banco' no config.json."
        print(msg_erro)
        resultados["erros_gerais"] = msg_erro
        enviar_notificacao_teams(teams_webhook, "Processo Falhou", msg_erro, sucesso=False)
        sys.exit(1)

    try:
        conn_str = (f"DRIVER={{{db_config['driver']}}};SERVER={db_config['servidor']};DATABASE={db_config['banco_de_dados']};UID={db_config['usuario']};PWD={db_config['senha']};")
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        print("Conexão com o banco de dados estabelecida.")

        # 1. Definir o intervalo de datas para verificar (comum a todos)
        hoje = date.today()
        data_inicio_verificacao = hoje - timedelta(days=7)
        intervalo_total = [(data_inicio_verificacao + timedelta(days=i)) for i in range(7)]
        print(f"Verificando datas no intervalo: {data_inicio_verificacao.strftime('%Y-%m-%d')} até {hoje.strftime('%Y-%m-%d')}")

        # --- LOOP PRINCIPAL POR PROCESSO ---
        for processo in lista_processos:
            nome_proc = processo.get('nome_processo', 'N/A')
            proc_sql = processo.get('procedure_executar')
            tabela_check = processo.get('tabela_verificacao')
            coluna_check = processo.get('coluna_data_verificacao')

            print(f"\n=======================================================")
            print(f"Iniciando Processo: {nome_proc} (Tabela: {tabela_check})")
            print(f"=======================================================")

            if not all([proc_sql, tabela_check, coluna_check]):
                print(f"   AVISO: Ignorando processo '{nome_proc}' por falta de configuração (procedure, tabela ou coluna).")
                continue

            # 2. Obter as datas que JÁ existem na tabela de destino DESTE PROCESSO
            query_datas_existentes = f"SELECT DISTINCT {coluna_check} FROM {tabela_check} WHERE {coluna_check} >= ?"
            cursor.execute(query_datas_existentes, data_inicio_verificacao)
            datas_existentes = {row[0] for row in cursor.fetchall()}
            
            # 3. Identificar as datas que estão faltando para ESTE PROCESSO
            datas_faltantes = [dia for dia in intervalo_total if dia not in datas_existentes]

            # 4. Executar a procedure específica para cada data faltante
            if not datas_faltantes:
                print(f"   Nenhum dia faltante encontrado para '{nome_proc}'. A tabela está atualizada.")
            else:
                print(f"   Datas faltantes para '{nome_proc}': {[d.strftime('%Y-%m-%d') for d in datas_faltantes]}")
                for dia in datas_faltantes:
                    if executar_procedure(cursor, proc_sql, dia):
                        resultados["sucesso"].append(f"{nome_proc} ({dia.strftime('%Y-%m-%d')})")
                    else:
                        resultados["falha"].append(f"{nome_proc} ({dia.strftime('%Y-%m-%d')})")

    except pyodbc.Error as ex:
        resultados["erros_gerais"] = f"ERRO CRÍTICO de banco de dados: {ex}"
        print(resultados["erros_gerais"])
    except Exception as e:
        resultados["erros_gerais"] = f"Ocorreu um erro inesperado no script: {e}"
        print(resultados["erros_gerais"])

    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

        # Monta e envia notificação final
        sucesso_final = not resultados["erros_gerais"] and not resultados["falha"]
        titulo_final = "Processo Finalizado"
        if not sucesso_final:
            titulo_final = "Processo Finalizado com ERROS"

        msg_final = ""
        if resultados["sucesso"]:
            msg_final += f"**Execuções com Sucesso:** {', '.join(resultados['sucesso'])}.  \n"
        if resultados["falha"]:
            msg_final += f"**Execuções com Falha:** {', '.join(resultados['falha'])}.  \n"
        if resultados["erros_gerais"]:
            msg_final += f"**Erro Geral:** {resultados['erros_gerais']}"
        
        if not msg_final:
            msg_final = "Nenhuma ação foi necessária ou nenhum processo foi configurado."

        enviar_notificacao_teams(teams_webhook, titulo_final, msg_final, sucesso=sucesso_final)
        print("Script finalizado.")

if __name__ == "__main__":
    main()