from flask import Flask, render_template, url_for, redirect, jsonify
from flask_mysqldb import MySQL
from flask import request, Response
import json
import yaml
from wtforms import *
from wtforms.validators import *
from flask_cors import CORS

# IMPORTES NECESSARIOS PARA A COMPARACAO TEXTUAL DO SQL
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Parenthesis
from sqlparse.tokens import Keyword, DML

# IMPORTES NECESSARIOS PARA A COMPARACAO ATRAVES DE DATASET DOS COMANDOS DE SQL
import sqlparse
import MySQLdb._mysql
import pandas
import datacompy
from mysql.connector import Error

# SIMPLESMENTE CORES
from colorama import Fore, Style
from termcolor import colored

app = Flask(__name__, instance_relative_config=True)
app.config.from_object(__name__)

db = yaml.load(open('db.yaml'))
app.config['MYSQL_HOST'] = db['mysql_host']
app.config['MYSQL_USER'] = db['mysql_user']
app.config['MYSQL_PASSWORD'] = db['mysql_password']
app.config['MYSQL_DB'] = db['mysql_db']

mysql = MySQL(app)
print("MYSQL: ",mysql)
CORS(app)

# ACRESCENTAR OS NOMES DAS BASE DE DADOS
# ACRESCENTAR OS NOMES DAS TABELAS DA BASE DE DADOS MUSICAS

jg_teste_cur_init = MySQLdb.connect(host='localhost',
                                         database='jg_teste',
                                         user='root',
                                         password='user')
cursor = jg_teste_cur_init.cursor()
# print("MYSQL: ",mysql)
# print("MYSQL CONNECTION: ",mysql.connection)
with app.app_context():
    mydb_cur = mysql.connection.cursor()
    query = "SHOW TABLES;"
    cursor.execute(query)
    data_show_tables = cursor.fetchall()
    # print(data_show_tables)
    if(len(data_show_tables) > 0):
        for each in data_show_tables:
            # print("EACH IN DATA_SHOW_TABLES:",each[0].upper())
            query = "SELECT TABELA_ID, NOME FROM TABELA WHERE NOME = %s;"
            mydb_cur.execute(query, (each[0],))
            data = mydb_cur.fetchall()
            if(len(data) == 0):
                query = "INSERT INTO tabela (NOME) VALUES (%s);"
                mydb_cur.execute(query, (each[0],))
                mysql.connection.commit()

def acrescentar_id(query, id_):
        queryparsed = sqlparse.parse(query)
        # print(queryparsed[0])
        lista = list()
        for each in queryparsed[0]:
            if each.value == "SELECT":
                lista.append(each.value)
                lista.append(" ")
                lista.append(id_+",")
                print(''.join(lista))
            else:
                lista.append(each.value)
        print(lista)
        lista = ''.join(lista)
        # print(lista)
        return lista


def is_subselect(parsed):
    #print(type(parsed),parsed)
    if not parsed.is_group:
        return False
    #print("is_subselect",parsed, " ", not parsed.is_group, )#parsed.tokens)
    if(isinstance(parsed,Identifier) or isinstance(parsed,Parenthesis)):
        if(parsed.value.upper().find("(SELECT") == 0):
            return True
        else:
    #for item in parsed[0]:
        #print("Item",item, item.ttype, item.value.upper())
        #print()
        #print(item.ttype)
        #print(item.value.upper())
        #if item.ttype is DML and item.value.upper() == 'SELECT':
            #return True
            return False

# parsed = sqlparse.parse(sql)[0]
def extract_from_part(parsed):
    from_seen = False
    #print(parsed.value)
    if(parsed.value.upper().find("(SELECT") == 0):
        parsed.value = parsed.value[parsed.value.find("(")+1:parsed.value.find(")")]
        parsed = sqlparse.parse(parsed.value)[0]
    #print(parsed.value)
    for item in parsed.tokens:
        if from_seen:
            #print(item.ttype)
            if is_subselect(item):
                #print(item)
                for x in extract_from_part(item):
                    #print(x)
                    yield x
                #print("something")
            elif item.ttype is Keyword and (item.value.upper() == 'INNER JOIN' or item.value.upper() == 'JOIN'):
                print("continue", item.value)
                continue
            elif item.ttype is Keyword:#and item.value.upper() != 'INNER JOIN' and item.value.upper() != 'JOIN':
                print("break", item.value)
                break
                #raise StopIteration
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_table_identifiers(token_stream):
    
    for item in token_stream:
        #print(type(item).__name__)
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_real_name()#.get_name()
        elif isinstance(item, Identifier):
            yield item.get_real_name()#.get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            
            yield item.value#.value


def extract_tables(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    #print(dir(stream))
    #print()
    #print(sqlparse.parse(sql)[0][0])
    return list(extract_table_identifiers(stream))


class AlunoForm(Form):
    nome = StringField('nome', [validators.Length(min=3, max=45)])
    numero = IntegerField('numero', [validators.required()])
    password = PasswordField('password', [validators.DataRequired()])


class PerguntaForm(Form):
    pergunta = StringField('pergunta', [validators.Length(min=15, max=1000)])
    pergunta_sql = StringField(
        'pergunta_sql', [validators.Length(min=12, max=1000)])
    query_id = StringField('query_id', [validators.Length(min=2, max=45)])


class RespostaForm(Form):
    resposta_sql = StringField(
        'resposta_sql', [validators.Length(min=0, max=1000)])
    aluno_aluno_id = IntegerField('aluno_aluno_id', [validators.required()])
    pergunta_pergunta_id = IntegerField('pergunta_pergunta_id', [validators.required()])

@app.route('/v1.0/alunos/', methods=['POST', 'GET'])
def alunos():
    form = AlunoForm(request.form)
    print("ENTROU DENTRO DE /v1.0/alunos/")
    print("IMPRIMR O FORMULARIO")
    print(dir(form))
    print("IMPRIMIR O FORM VALIDATE:")
    print(form.validate())
    print("IMPRIME OS ERROS")
    print(form.errors)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute('SELECT * FROM aluno;')
        data = cur.fetchall()
        cur.close()

        alunos = []

        for elm in data:
            aluno = {
                'aluno_id': elm[0],
                'nome': elm[1],
                'numero': elm[2]  # ,
                # 'Password': elm[3]
            }
            alunos.append(aluno)
        # print(data.row0)
        # return render_template('/alunos/alunos.html', students=data)
        js = json.dumps(alunos)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/alunos/'
        return resp
    elif request.method == 'POST' and form.validate():
        # RECEBE OS DADOS DO REQUEST
        nome = request.form["nome"]
        numero = request.form["numero"]
        password = request.form["password"]
        # INSERE UM NOVO ALUNO DENTRO DA TABELA ALUNO
        cur = mysql.connection.cursor()
        query = "INSERT INTO aluno (NOME, NUMERO, PASSWORD) VALUES (%s,%s,%s);"
        cur.execute(query, (nome, numero, password))
        mysql.connection.commit()
        # SELECIONA O ULTIMO INDEX INSERIDO
        cur.execute("SELECT LAST_INSERT_ID()")
        data_last_inserted_id = cur.fetchall()
        last_inserted_id = data_last_inserted_id[0][0]
        query = "SELECT NOME,NUMERO FROM ALUNO WHERE ALUNO_ID = %s;"
        cur.execute(query, (last_inserted_id,))
        data = cur.fetchall()
        cur.close()

        aluno = {
            'nome': data[0][0],
            'numero': data[0][1]
        }

        js = json.dumps(aluno)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/alunos/'
        return resp
    elif request.method == 'POST' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/alunos/'
        return resp


@app.route('/v1.0/aluno/<int:aluno_id>', methods=['GET'])
def aluno(aluno_id):
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        query = "SELECT NOME, NUMERO, PASSWORD FROM aluno WHERE NUMERO=%s"
        cur.execute(query, (aluno_id,))
        data = cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:
            aluno = {
                'nome': data[0][0],
                'numero': data[0][1],
                'password': data[0][2]
            }
            js = json.dumps(aluno)
            resp = Response(js, status=200, mimetype='application/json')
            resp.headers['Links'] = 'http://127.0.0.1/aluno'
            return resp


@app.route('/v1.0/aluno/delete/<int:aluno_id>', methods=['POST'])
def aluno_delete(aluno_id):
    if request.method == 'POST' and request.form['_method'] == 'delete':
        query = "DELETE FROM aluno WHERE NUMERO = %s"
        cur = mysql.connection.cursor()
        cur.execute(query, (aluno_id,))
        mysql.connection.commit()
        cur.fetchall()
        cur.close()
        return Response(status=200)


@app.route('/v1.0/aluno/update/<int:aluno_id>', methods=['POST'])
def aluno_update(aluno_id):
    form = AlunoForm(request.form)
    if request.method == 'POST' and form.validate():
        nome = request.form["nome"]
        numero = request.form["numero"]
        password = request.form["password"]
        cur = mysql.connection.cursor()
        query = "UPDATE aluno SET NOME=%s, NUMERO=%s, PASSWORD=%s WHERE NUMERO = %s"
        cur.execute(query, (nome, numero, password, aluno_id))
        mysql.connection.commit()
        cur.execute(
            "SELECT NOME, NUMERO FROM aluno WHERE NUMERO = %s", (aluno_id,))
        data = cur.fetchall()
        cur.close()

        print(" * DATA ")
        print(data)

        aluno = {
            'nome': data[0][0],
            'numero': data[0][1]
        }

        js = json.dumps(aluno)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/aluno'
        return resp
    elif request.method == 'POST' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/aluno'
        return resp


@app.route('/v1.0/respostas/', methods=['GET'])
def respostas():
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute(
            'SELECT RESPOSTA_ID, RESPOSTA_SQL, ALUNO_ALUNO_ID, PERGUNTA_PERGUNTA_ID, NUMERO_LINHAS_IGUAIS, NUMERO_LINHAS_TOTAIS, TABELAS_TOTAIS, TABELAS_IGUAIS FROM resposta;')
        data = cur.fetchall()
        cur.close()
        respostas = []

        for elm in data:
            resposta = {
                'resposta_id': elm[0],
                'resposta_sql': elm[1],
                'aluno_aluno_id': elm[2],
                'pergunta_pergunta_id': elm[3],
                'numero_linhas_iguais': elm[4],
                'numero_linhas_totais': elm[5],
                'tabelas_totais': elm[6],
                'tabelas_iguais': elm[7]
            }
            respostas.append(resposta)

        # return render_template('/respostas/respostas.html', respostas=data)
        return jsonify(respostas)


@app.route('/v1.0/resposta/<int:pergunta_id>/<int:aluno_id>',  methods=['POST'])
def resposta_insert(pergunta_id, aluno_id):
    form = RespostaForm(request.form)
    print("IMPRIMR O FORMULARIO")
    print(dir(form))
    print("IMPRIMIR O FORM VALIDATE:")
    print(form.validate())
    print("IMPRIME OS ERROS")
    print(form.errors)
    if request.method == 'POST' and form.validate():
        print("ENTROU AQUI:")
        print("if request.method == 'POST' and form.validate():")
        # VERIFICA SE OS FORMULARIOS ESTAO BEM FEITOS
        # Caso nao estejam bem feito levanta uma excepcao
        resposta_sql = request.form["resposta_sql"]

        resposta_sql_alun = resposta_sql
        # INSERE NA BASE DE DADOS A NOVA RESPOSTA
        mydb_cur = mysql.connection.cursor()
        jg_teste_cur = MySQLdb.connect(host='localhost',
                                         database='jg_teste',
                                         user='root',
                                         password='user')
        print("MYDB_CUR")
        print(mydb_cur)
        print("JG_TESTE_CUR")
        print(jg_teste_cur)
        
        # Verifica a qualidade do SQL
        ###############################################################
        #resposta_sql_alun = resposta_sql_alun.upper()

        print("RESPOSTA SQL ALUN")
        print(resposta_sql_alun)
        ############ E QUE TAL FAZER ISTO TUDO DENTRO DE UMA FUNCAO ????? ############
        # Verifica se as tabelas utilizadas são as mesmas
        # Caso não seja manda uma excepção
        query = "EXPLAIN " + resposta_sql_alun
        
        print("QUERY EXPLAIN")
        print(query)
        # Verificia se o cursor pode ser criado
        # Caso não seja manda uma excepção
        cursor = jg_teste_cur.cursor()
        print("CURSOR")
        print(cursor)
        # Verifica se o comando pode ser executado 
        # Caso não seja levanta uma excepção
        try:

            cursor.execute(query)

        except Exception as error:

            print(error)
            print("dir error ", dir(error))
            print("error something", error.args)
            erro = {
                'erro': error.args
            }
            js = json.dumps(erro)
            print(js)
            print("ERROR SOMETHING!!!")
            return Response(js, status=406, mimetype='application/json')

        # Verifica se o comando pode ser executado 
        # Caso não seja levanta uma excepção
        ###########################################################################
        # explain_alun = cursor.fetchall()
        # print("EXPLAIN ALUN")
        # print(explain_alun)
        # alun_tabela = []
        # for each_tabela in explain_alun:
        #     alun_tabela.append(each_tabela[2])
        # for each in alun_tabela:
        #     each.upper()
        # print("#########################")
        # print("## TABELAS ENCONTRADAS ##")
        # print("#########################")
        # print("TABELAS ENCONTRADAS NA QUERY ALUNO")
        # print(alun_tabela)
        ###########################################################################
        # print("TABELAS ENCONTRADAS NA QUERY ALUNO - SEM REPETICAO")
        # alun_tabela = list(dict.fromkeys(alun_tabela))
        # print(alun_tabela)
        # PESQUISA O QUERY MYSQL DA PERGUNDA DO PROFESSOR

        alun_tabela = extract_tables(resposta_sql_alun)
        # for each_elm in alun_tabela:
        #     each_elm.upper()


        query = "SELECT PERGUNTA_SQL,QUERY_ID FROM PERGUNTA WHERE PERGUNTA_ID = %s"
        mydb_cur.execute(query, (pergunta_id,))
        data_prof = mydb_cur.fetchall()
        print("DATA_PROF")
        print(data_prof)
        # VERIFICA TAMANHO data_prof 
        # Levanta Excepção
        resposta_sql_prof = data_prof[0][0]
        #resposta_sql_prof.upper()
        # query = "EXPLAIN " + resposta_sql_prof#.upper()
        # print("QUERY EXPLAIN PROF")
        # print(query)
        # ########################################################################
        # # Noutro ponto da API a pergunta do professor tem que ser testada contra erros
        # cursor.execute(query)
        # explain_prof = cursor.fetchall()
        # print("EXPLAIN_PROF")
        # print(explain_prof)
        # ####################################################################################
        # prof_tabela = []
        # for each_tabela in explain_prof:
        #     prof_tabela.append(each_tabela[2])
        # for each in prof_tabela:
        #     each.upper()
        # print("TABELAS ENCONTRADAS NA QUERY PROFESSOR")
        # print(prof_tabela)
        ########################################################################

        prof_tabela = extract_tables(resposta_sql_prof)
        # for each_elm in prof_tabela:
        #     each_elm.upper()

        # print("TABELAS ENCONTRADAS NA QUERY PROFESSOR - SEM REPETICAO")
        # prof_tabela = list(dict.fromkeys(prof_tabela))
        # print(prof_tabela)

        join_alun_prof_tabela = set(alun_tabela) & set(prof_tabela)
        print("JOIN TABELAS ALUNO E PROFESSOR:", join_alun_prof_tabela)

        ################## if(len(join_alun_prof_tabela) == 0):
            
        # # Insere a resposta no lugar certo da base de dados
        # query = "INSERT INTO resposta (pergunta_pergunta_id, aluno_aluno_id, resposta_sql) VALUES (%s,%s,%s);"
        # mydb_cur.execute(query, (pergunta_id, aluno_id, resposta_sql))
        # mysql.connection.commit()
        # # SELECIONA O ULTIMO INDEX INSERIDO
        # mydb_cur.execute("SELECT LAST_INSERT_ID();")
        # data_last_inserted_id = mydb_cur.fetchall()
        # last_inserted_id = data_last_inserted_id[0][0]
        # PREPARA ESTRUTURA PARA COMPARACAO DE TABELAS VIA MYSQL AND DATASETS

        # PESQUISA O QUERY MYSQL DA PERGUNDA DO PROFESSOR
        query = "SELECT PERGUNTA_SQL,QUERY_ID FROM PERGUNTA WHERE PERGUNTA_ID = %s"
        mydb_cur.execute(query, (pergunta_id,))
        data_prof = mydb_cur.fetchall()
        print("DATA PROF:", data_prof)

        sqlquery = data_prof[0][0]
        # sqlquery.upper() 

        # PESQUISA O QUERY MYSQL DA RESPOSTA DO ALUNO
        sqlqueryal = resposta_sql_alun
        # sqlqueryal.upper()

        # sqlparsed = sqlparse.parse(sqlquery)

        # DETERMINA A PARTIR DA TABELA PERGUNTA O QUERY ID A SER UTILIZADO
        sqlquerykey = data_prof[0][1]

        print()
        # import mysql.connector

        print("IMPRIME A DATA_FRAME_KEY: ", sqlquerykey)
        print("CHAVE: ", sqlquerykey)
        sqlqueryprof = sqlquery
        print("SQL QUERY DO PROFESSOR: ", sqlqueryprof)
        sqlqueryalun = sqlqueryal
        print("SQL QUERY DO ALUNO: ", sqlqueryalun)

        # print("#########################################################################################")
        # # ANTES DE EXECUTAR CADA UMA DAS QUERYS VEREFICAR QUAIS AS TABELAS QUE SAO UTLIZADAS EM CADA UMA
        # # VERIFICAR QUE TABELAS EXISTEM PARA O PROFESSOR
        # query = "EXPLAIN " + sqlqueryprof
        # print("QUERY EXPLAIN DO PROFESSOR")
        # print(query)
        # # connection.commit()
        # cursor.execute(query)
        # explain_prof = cursor.fetchall()
        # print("TABELA EXPLICATIVA DA QUERY DO PROFESSOR")
        # data_frame_expl_prof = pandas.DataFrame(list(explain_prof))
        # print(data_frame_expl_prof)
        # tabelas_prof = []
        # for row in explain_prof:
        #     tabelas_prof.append(row[2])
        # print("TABELAS USADAS NA QUERY DO PROFESSOR")
        # print(tabelas_prof)

        # # VERIFICAR QUE TABELAS EXISTEM PARA O ALUNO
        # query = "EXPLAIN " + sqlqueryalun
        # print("QUERY EXPLAIN DO ALUNO")
        # print(query)
        # # connection.commit()
        # cursor.execute(query)
        # explain_alun = cursor.fetchall()
        # print("TABELA EXPLICATIVA DA QUERY DO ALUNO")
        # data_frame_expl_alun = pandas.DataFrame(list(explain_alun))
        # print(data_frame_expl_alun)
        # tabelas_alun = []
        # for row in explain_prof:
        #     tabelas_alun.append(row[2])
        # print("TABELAS USADAS NA QUERY DO ALUNO")
        # print(tabelas_alun)

        # print("#################################################################")
        # EXECUTAR CADA UMA DAS QUERYS
        
        # cursor.execute(sqlqueryprof)
        # records_prof = cursor.fetchall()

        # data_frame_prof = pandas.DataFrame(list(records_prof))
        # print("IMPRIMIR O DATA FRAME RESULTANTE DO PROFESSOR A CONEXAO")
        # print(data_frame_prof)

        # cursor.execute(sqlqueryalun)
        # records_query = cursor.fetchall()

        # data_frame_alun = pandas.DataFrame(list(records_query))
        # print("IMPRIMIR O DATA FRAME RESULTANTE DO ALUNO A CONEXAO")
        # print(data_frame_alun)

        # CRIACAO DOS DATA FRAMES PARA COMPARACAO COM datacompy
        # VERIFICAR SE DESPOLTA ERROS
        # CASO VERDADEIRO LEVANTA EXCEPÇÃO
        data_prof = pandas.read_sql(sqlqueryprof, con=jg_teste_cur)
        data_alun = pandas.read_sql(sqlqueryalun, con=jg_teste_cur)
        print("##################################################")
        print("## IMPRIME OS DATA SETS PARA ALUNO E PROFESSOR: ##")
        print("##################################################")
        print("DATA SET DO PROFESSOR:")
        print(data_prof)
        print("##################################################")
        print("DATA SET DO AUNO:")
        print(data_alun)

        # datacompy
        # VERIFICAR SE DESPOLTA ERROS
        # CASO VERDADEIRO LEVANTA EXCEPÇÃO
        compare = datacompy.Compare(
            data_prof,
            data_alun,
            join_columns=sqlquerykey)

        # IMPRIMIR RESULTADOS DA COMPARACAO ##
        ######################################
        print("###################################################################")
        print("## VARIAVEIS IMPRIMIDAS NA CONSOLA A COLOCAR NA TABELA RESULTADO ##")
        print("###################################################################")
        # print("Colunas que intreceptaram",
        #       len(compare.intersect_columns()))
        print("Linhas que intreceptaram: ", compare.intersect_rows.shape[0])
        print("Linha unicas no DataFrame 1:", len(compare.df1_unq_rows))
        print("Linha unicas no DataFrame 2:", len(compare.df2_unq_rows))
        # print()
        # print("Colunas unicas de Dataframe 1 :",
        #       compare.df1_unq_columns())
        # print("Colunas unicas de Dataframe 2 :",
        #       compare.df2_unq_columns())

        # print("IMPRIME LINHAS UNICAS")
        # print(data_prof)
        numero_linhas_totais = len(data_prof)
        print("NUMERO DE LINHAS EM DATAFRAME 1: ", numero_linhas_totais)
        numero_linhas_iguais = compare.intersect_rows.shape[0]
        print("NUMERO DE LINHAS IGUAIS: ", numero_linhas_iguais)
        # print("NUMERO DE COLUNAS QUE SE INTRESEPTAM COM O NOS DATAFRAMES")
        # numero_colunas_iguais = len(compare.intersect_columns())
        # print(len(compare.intersect_columns()))

        # AINDA ESTA POR CALCULAR
        # CAMPOS ESPECIFICOS

        # table_prof = extract_tables(sqlqueryprof)
        # table_alun = extract_tables(sqlqueryalun)

        # campos_totais = len(table_prof)
        # print(" * TABELAS")
        # print(set(table_prof) & set(table_alun))
        # campos_iguais = len(set(table_prof) & set(table_alun))

        pergunta_pergunta_id = pergunta_id
        tabelas_totais = len(prof_tabela)
        print("TABELAS TOTAIS: ", tabelas_totais)
        tabelas_iguais = len(join_alun_prof_tabela)
        print("TABELAS IGUAIS: ", tabelas_iguais)

        # query = "INSERT INTO RESULTADO (numero_linhas_iguais, numero_linhas_totais, tabelas_totais, tabelas_iguais, Pergunta_Pergunta_id) VALUES (%s,%s,%s,%s,%s);"
        # mydb_cur.execute(query, (numero_linhas_iguais, numero_linhas_totais, tabelas_totais, tabelas_iguais, pergunta_pergunta_id))
        # mysql.connection.commit()
        # print("COMIT RESULTADO BEM EXECUTADO")
        # # SELECIONA O ULTIMO INDEX INSERIDO
        # mydb_cur.execute("SELECT LAST_INSERT_ID();")
        # data_last_inserted_id = mydb_cur.fetchall()
        # print(data_last_inserted_id)
        # last_inserted_id = data_last_inserted_id[0][0]
        # resultado_resultado_id = last_inserted_id
        # resultado_pergunta_pregunta_id = pergunta_id
        # print("SELECT LAST INSERT INDEX DE RESULTADO OK:", resultado_resultado_id)

        # Insere a resposta no lugar certo da base de dados
        query = "INSERT INTO resposta (Resposta_sql, Aluno_Aluno_id, Pergunta_Pergunta_id, numero_linhas_totais, numero_linhas_iguais, tabelas_totais, tabelas_iguais ) VALUES (%s,%s,%s,%s,%s,%s,%s);"
        #query = "INSERT INTO `resposta`(`Resposta_sql`,`Aluno_Aluno_id`,`Pergunta_Pergunta_id`,`resultado_Resultado_id`,`resultado_Pergunta_Pergunta_id`) VALUES (%s,%s,%s,%s,%s);"
        mydb_cur.execute(query, (resposta_sql, aluno_id, pergunta_id,  numero_linhas_totais, numero_linhas_iguais, tabelas_totais, tabelas_iguais))
        mysql.connection.commit()

        print("COMIT RESPOSTA BEM EXECUTADO")
        # SELECIONA O ULTIMO INDEX INSERIDO
        mydb_cur.execute("SELECT LAST_INSERT_ID();")
        data_last_inserted_id = mydb_cur.fetchall()
        last_inserted_id = data_last_inserted_id[0][0]
        resposta_id = last_inserted_id
        print("RESPOSTA_ID:", resposta_id)

        # INSERE PARA CADA RESPOSTA AS TABELAS ENCONTRADAS CORRESPONDENTES NA BASE DE DADOS
        # 
        # Para a lista calculada a cima necessita de remover os duplicados e ignorar as leituras com < ou > no seu conteudo
        alun_tabela_tabela = alun_tabela
        alun_tabela_tabela = list(dict.fromkeys(alun_tabela_tabela))
        ############################################################################################
        # alun_tabela_tabela = [x for x in alun_tabela_tabela if not ("<" in x)]

        # Para a lista acima calculada verificar onde existe correspondencia com a lista anterior
        # Recolher os ids das tabelas a serem usados
        # para cada ID de tabela distinto inserir o ID completo de resposta composto por:
        # resposta_Resposta_id
        # resposta_Aluno_Aluno_id
        # tabela_Tabela_id

        for each_tabela in alun_tabela_tabela:
            query = "SELECT TABELA_ID FROM TABELA WHERE NOME = %s;"
            mydb_cur.execute(query, (each_tabela,))
            tabela_id = mydb_cur.fetchall()
            tabela_id = tabela_id[0][0]
            query = "INSERT INTO resposta_has_tabela (resposta_Resposta_id, tabela_Tabela_id) VALUES (%s,%s);"
            mydb_cur.execute(query, (resposta_id,tabela_id))
            mysql.connection.commit()

        # SELECIONA A RESPOSTA ANTERIORMENTE INSERIDA
        query = "SELECT RESPOSTA_SQL, ALUNO_ALUNO_ID, PERGUNTA_PERGUNTA_ID, NUMERO_LINHAS_IGUAIS, NUMERO_LINHAS_TOTAIS, TABELAS_TOTAIS, TABELAS_IGUAIS FROM RESPOSTA WHERE RESPOSTA_ID = %s;"
        mydb_cur.execute(query, (resposta_id,))
        data_last_resposta = mydb_cur.fetchall()
        mydb_cur.close()

        resposta = {
            'respotas_sql': data_last_resposta[0][0],
            'aluno_aluno_id': data_last_resposta[0][1],
            'pergunta_pergunta_id': data_last_resposta[0][2],
            'numero_linhas_totais': data_last_resposta[0][3],
            'numero_linhas_iguais': data_last_resposta[0][4],
            'tabelas_totais': data_last_resposta[0][5],
            'tabelas_iguais': data_last_resposta[0][6]
        }

        js = json.dumps(resposta)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp
    elif request.method == 'POST' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp


@app.route('/v1.0/respostas/<int:resposta_id>', methods=['GET'])
def respostas_update(resposta_id):
    form = RespostaForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        query = "SELECT RESPOSTA_ID, resposta_sql, aluno_aluno_id, pergunta_pergunta_id, numero_linhas_iguais, numero_linhas_totais, tabelas_totais, tabelas_iguais FROM RESPOSTA WHERE RESPOSTA_ID=%s"
        cur.execute(query, (resposta_id,))
        data = cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:
            resposta = {
                'resposta_id': data[0][0],
                'resposta_sql': data[0][1],
                'aluno_aluno_id': data[0][2],
                'pergunta_pergunta_id': data[0][3],
                'numero_linhas_iguais': data[0][4],
                'numero_linhas_totais': data[0][5],
                'tabelas_totais': data[0][6],
                'tabelas_iguais': data[0][7]
            }
            js = json.dumps(resposta)
            return Response(js, status=200, mimetype='application/json')
    # elif request.method == 'PUT' and form.validate():
    #     resposta_sql = request.form["resposta_sql"]
    #     cur = mysql.connection.cursor()
    #     query = "UPDATE RESPOSTA SET RESPOSTA_SQL=%s WHERE RESPOSTA_ID = %s;"
    #     cur.execute(query, (resposta_sql, resposta_id))
    #     mysql.connection.commit()
    #     cur.execute(
    #         "SELECT RESPOSTA_SQL FROM RESPOSTA WHERE RESPOSTA_ID = %s;", (resposta_id,))
    #     data = cur.fetchall()
    #     cur.close()
    #     resposta = {
    #         'resposta_sql': data[0][0]
    #     }
    #     js = json.dumps(resposta)
    #     resp = Response(js, status=200, mimetype='application/json')
    #     resp.headers['Links'] = 'http://127.0.0.1/respostas/'
    #     return resp
    # elif request.method == 'PUT' and not form.validate():
    #     resp = Response(status=400)
    #     resp.headers['Links'] = 'http://127.0.0.1/respostas/'
    #     return resp
    # elif request.method == 'DELETE':
    #     query = "DELETE FROM RESPOSTA WHERE RESPOSTA_ID = %s;"
    #     cur = mysql.connection.cursor()
    #     cur.execute(query, (resposta_id,))
    #     mysql.connection.commit()
    #     cur.fetchall()
    #     return Response(status=200)


@app.route('/v1.0/resposta/update/<int:resposta_id>', methods=['POST'])
def resposta_update(resposta_id):
    form = RespostaForm(request.form)
    if request.method == 'POST' and form.validate():
        resposta_sql = request.form["resposta_sql"]
        resposta_sql_alun = resposta_sql#.upper()
        pergunta_id = request.form["pergunta_pergunta_id"]
        aluno_id = request.form["aluno_aluno_id"]
        # INSERE NA BASE DE DADOS A NOVA RESPOSTA
        mydb_cur = mysql.connection.cursor()
        jg_teste_cur = MySQLdb.connect(host='localhost',
                                         database='jg_teste',
                                         user='root',
                                         password='user')

        # TESTAR SE O COMANDO SQL E EXECUTADO ATRAVES DO METODO EXPLAIN
        resposta_sql_alun_explain = "EXPLAIN " + resposta_sql

        cursor = jg_teste_cur.cursor()

        try:

            cursor.execute(resposta_sql_alun_explain)

        except Exception as error:

            print(error)
            print("dir error ", dir(error))
            print("error something", error.args)
            erro = {
                'erro': error.args
            }
            js = json.dumps(erro)
            print(js)
            print("ERROR SOMETHING!!!")
            return Response(js, status=406, mimetype='application/json')

        ####################################################################################
        # AQUI MERECE UM TRY CATCH...
        # Estraiu os nomes das tabelas envolvidas "possivelmente" com duplicados
        alun_tabela = extract_tables(resposta_sql)
        print("ALUNO_TABELA: ", alun_tabela)
        #alun_tabela = [x.upper() for x in alun_tabela]
        # REMOVE DUPLICADOS
        #####################################################################################
        alun_tabela = list(dict.fromkeys(alun_tabela))
        print("ALUNO_TABELA DEPOIS DE UPPER: ", alun_tabela)

        # Selecionar Qual a pergunta envolvida com determinada resposta
        query = "SELECT PERGUNTA_SQL, QUERY_ID FROM pergunta WHERE pergunta_id = %s"
        mydb_cur.execute(query, (pergunta_id,))
        data = mydb_cur.fetchall()
        resposta_sql_prof = data[0][0]
        query_id = data[0][1]

        # Selecionar as tabelas envolvidas para determinada pergunta
        query = "SELECT pergunta_Pergunta_id, tabela_Tabela_id from pergunta_has_tabela where pergunta_Pergunta_id = %s"
        mydb_cur.execute(query, (pergunta_id,))
        data = mydb_cur.fetchall()
        
        # Das tabelas envolvidas fazer uma lista 
        prof_tabela = []
        for each in data:
            query = "SELECT nome from tabela where tabela_id = %s"
            mydb_cur.execute(query,(each[1],))
            new_data = mydb_cur.fetchall()
            print("Each data prof tabela: ", new_data[0][0])
            prof_tabela.append(new_data[0][0])
        
        ######################################################################################
        # Remover Duplicados não existe necessáriamente necessidade disto parece-me
        prof_tabela = list(dict.fromkeys(prof_tabela))

        print("TABELA ALUNO: ", alun_tabela)
        print("TABELA PROFESSOR: ", prof_tabela)
        # Com a lista das tabelas envolvidas na pergunta produzir 
        # O numero de tabelas_totais e tabelas_iguais
        tabelas_totais = len(prof_tabela)

        tabelas_iguais = len(set(alun_tabela) & set(prof_tabela))

        if tabelas_iguais == 0:
            return Response(status=406)
        
        #########################################################################################
        # Fazer devidamente a comparação dos dois data frames resultantes das querys aluno e professor envolvidas
        data_prof = pandas.read_sql(resposta_sql_prof, con=jg_teste_cur)
        data_alun = pandas.read_sql(resposta_sql_alun, con=jg_teste_cur)
        print("##################################################")
        print("## IMPRIME OS DATA SETS PARA ALUNO E PROFESSOR: ##")
        print("##################################################")
        print("DATA SET DO PROFESSOR:")
        print(data_prof)
        print("##################################################")
        print("DATA SET DO AUNO:")
        print(data_alun)

        # datacompy
        # VERIFICAR SE DESPOLTA ERROS
        # CASO VERDADEIRO LEVANTA EXCEPÇÃO
        compare = datacompy.Compare(
            data_prof,
            data_alun,
            join_columns=query_id)

        # IMPRIMIR RESULTADOS DA COMPARACAO ##
        ######################################
        print("###################################################################")
        print("## VARIAVEIS IMPRIMIDAS NA CONSOLA A COLOCAR NA TABELA RESULTADO ##")
        print("###################################################################")
        # print("Colunas que intreceptaram",
        #       len(compare.intersect_columns()))
        print("Linhas que intreceptaram: ", compare.intersect_rows.shape[0])
        print("Linha unicas no DataFrame 1:", len(compare.df1_unq_rows))
        print("Linha unicas no DataFrame 2:", len(compare.df2_unq_rows))
        # print()
        # print("Colunas unicas de Dataframe 1 :",
        #       compare.df1_unq_columns())
        # print("Colunas unicas de Dataframe 2 :",
        #       compare.df2_unq_columns())

        # print("IMPRIME LINHAS UNICAS")
        # print(data_prof)
        numero_linhas_totais = len(data_prof)
        print("NUMERO DE LINHAS EM DATAFRAME 1: ", numero_linhas_totais)
        numero_linhas_iguais = compare.intersect_rows.shape[0]
        print("NUMERO DE LINHAS IGUAIS: ", numero_linhas_iguais)


        # Fazer devidamente o update da nova resposta a ser actualizada em BD

        query = "UPDATE RESPOSTA SET RESPOSTA_SQL=%s, numero_linhas_iguais=%s, numero_linhas_totais=%s, tabelas_totais=%s, tabelas_iguais=%s WHERE RESPOSTA_ID = %s;"
        mydb_cur.execute(query, (resposta_sql_alun, numero_linhas_iguais, numero_linhas_totais, tabelas_totais, tabelas_iguais, resposta_id))
        mysql.connection.commit()

        print("Fez o commit com sucesso da resposta com update....")

        # APAGAR TODAS AS REFERENCIAS ANTERIORES A TABELAS 
        # DENTRO DA TABELA PERGUNTA_HAS_TABELA ELEMINAR TODAS AS REFERENCIAS A  PERGUNTA_ID
        query = "DELETE FROM RESPOSTA_HAS_TABELA WHERE resposta_RESPOSTA_ID = %s;"
        mydb_cur.execute(query, (resposta_id,))
        mysql.connection.commit()

        print("Fez o delete com sucesso da resposta com update....")

        # REFAZER AS TABELAS ANTERIORES A TABELAS
        for each_tabela in alun_tabela:
            print("EACH_TABELA: ", each_tabela)
            query = "SELECT TABELA_ID FROM TABELA WHERE NOME = %s;"
            mydb_cur.execute(query, (each_tabela,))
            tabela_id = mydb_cur.fetchall()
            print(tabela_id)
            tabela_id = tabela_id[0][0]
            print("TABELA_ID: ", tabela_id)
            query = "INSERT INTO RESPOSTA_HAS_TABELA (RESPOSTA_RESPOSTA_ID, tabela_Tabela_id) VALUES (%s,%s);"
            mydb_cur.execute(query, (resposta_id,tabela_id))
            mysql.connection.commit()
            print("COMIT DA PERGUNTA HAS TABELA DEVIDAMENTE EXECUTADO! PERGUNTA_ID", pergunta_id, " TABELA_ID", tabela_id)

        # Mostrar devidamente o resultado resultante da alteração da resposta
        query = "SELECT resposta_id, RESPOSTA_SQL, aluno_aluno_id, pergunta_pergunta_id, numero_linhas_iguais, numero_linhas_totais, tabelas_totais, tabelas_iguais FROM RESPOSTA WHERE RESPOSTA_ID = %s;"
        mydb_cur.execute(query, (resposta_id,))
        data = mydb_cur.fetchall()
        mydb_cur.close()
        resposta = {
            'resposta_id': data[0][0],
            'resposta_sql': data[0][1],
            'aluno_aluno_id': data[0][2],
            'pergunta_pergunta_id': data[0][3],
            'numero_linhas_iguais': data[0][4],
            'numero_linhas_totais': data[0][5],
            'tabelas_totais': data[0][6],
            'tabelas_iguais': data[0][7]
        }
        js = json.dumps(resposta)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp
    elif request.method == 'POST' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp


@app.route('/v1.0/resposta/delete/<int:resposta_id>', methods=['POST'])
def resposta_delete(resposta_id):
    if request.method == 'POST' and request.form['_method'] == 'delete':
        query = "DELETE FROM RESPOSTA WHERE RESPOSTA_ID = %s;"
        cur = mysql.connection.cursor()
        cur.execute(query, (resposta_id,))
        mysql.connection.commit()
        cur.fetchall()
        cur.close()
        return Response(status=200)


@app.route('/v1.0/perguntas/', methods=['POST', 'GET'])
def perguntas():
    form = PerguntaForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute("SELECT PERGUNTA_ID, PERGUNTA,PERGUNTA_SQL, QUERY_ID FROM PERGUNTA;")
        data = cur.fetchall()
        cur.close()

        perguntas = []

        for elm in data:
            pergunta = {
                'pergunta_id': elm[0],
                'pergunta': elm[1],
                'pergunta_sql': elm[2],
                'query_id': elm[3]
            }
            perguntas.append(pergunta)

        # return render_template('/perguntas/perguntas.html', perguntas=data)
        return jsonify(perguntas)
    elif request.method == 'POST' and form.validate():
        # RECEBE OS DADOS DO REQUEST
        pergunta = request.form["pergunta"]
        pergunta_sql = request.form["pergunta_sql"]
        query_id = request.form["query_id"]
        # VERIFICA SE O SQL INSERIDO E VALIDO

        # RECOLHE AS TABELAS INSERIDAS NA QUERY PARA COLOCAR NA TABELA "TABELA" DA BASE DE DADOS
        mydb_cur = mysql.connection.cursor()
        jg_teste_cur = MySQLdb.connect(host='localhost',
                                         database='jg_teste',
                                         user='root',
                                         password='user')
        cursor = jg_teste_cur.cursor()
        # print("ACERCA DO CURSOR:", mydb_cur)
        query = "EXPLAIN " + pergunta_sql#.upper()
        # print("QUERY:", query)
        try:

            cursor.execute(query)

        except Exception as error:

            print(error)
            print("dir error ", dir(error))
            print("error something", error.args)
            erro = {
                'erro': error.args
            }
            js = json.dumps(erro)
            print(js)
            print("ERROR SOMETHING!!!")
            return Response(js, status=406, mimetype='application/json')
        #explain_prof = cursor.fetchall()
        #######################################################################
        # print("EXPLAIN ALUN")
        # print(explain_prof)
        # prof_tabela = []
        # for each_tabela in explain_prof:
        #     prof_tabela.append(each_tabela[2])
        # # for each in prof_tabela:
        # #     each.upper()
        # print("#########################")
        # print("## TABELAS ENCONTRADAS ##")
        # print("#########################")
        # print("TABELAS ENCONTRADAS NA QUERY PROFESSOR")
        # print(prof_tabela)

        ##################################################################################
        # Novamente isto merece protecção
        ##################################################################################
        prof_tabela = extract_tables(pergunta_sql)#.upper())
        # INSERE UM NOVA PERGUNTA DENTRO DA TABELA PERGUNTA

        ###################################################################################
        # ISTO MERECE PROTECAO Suponhamos que o query_id nao se encontra na pergunta_sql
        ###################################################################################
        query = "INSERT INTO PERGUNTA (PERGUNTA, PERGUNTA_SQL,query_id) VALUES (%s,%s,%s);"
        mydb_cur.execute(query, (pergunta, pergunta_sql, query_id))
        mysql.connection.commit()
        print("COMMIT DO INSERT PERGUNTA BEM EXECUTADO:")
        # SELECIONA O ULTIMO INDEX INSERIDO
        mydb_cur.execute("SELECT LAST_INSERT_ID();")
        data_last_inserted_id = mydb_cur.fetchall()
        last_inserted_id = data_last_inserted_id[0][0]
        pergunta_id = last_inserted_id

        print("PERGUNTA ID (bem recolhido): ", pergunta_id)
        # EFECTIVAMENTE COLOCA DENTRO DA TABELA "TABELA" A INFORMACAO RELATIVA AS TABELAS USADAS
        # Para a lista calculada a cima necessita de remover os duplicados e ignorar as leituras com < ou > no seu conteudo
        prof_tabela_tabela = prof_tabela
        prof_tabela_tabela = list(dict.fromkeys(prof_tabela_tabela))
        ##############################################################################
        # Do jeito que a aplicação esta isto nao e necessariamente necessario
        ##############################################################################
        # prof_tabela_tabela = [x for x in prof_tabela_tabela if not ("<" in x)]

        print("LISTA DE TABELAS SEM REPETICOES E SUBQUERYS")

        # Para a lista acima calculada verificar onde existe correspondencia com a lista anterior
        # Recolher os ids das tabelas a serem usados
        # para cada ID de tabela distinto inserir o ID completo de resposta composto por:
        # pergunta_Pergunta_ID
        # tabela_Tabela_id

        print(prof_tabela_tabela)

        for each_tabela in prof_tabela_tabela:
            print("EACH_TABELA: ", each_tabela)
            query = "SELECT TABELA_ID FROM TABELA WHERE NOME = %s;"
            mydb_cur.execute(query, (each_tabela,))
            ##########################################################################
            # AS Tabelas devem ser preenchidas de forma automatica...
            tabela_id = mydb_cur.fetchall()
            print(tabela_id)
            tabela_id = tabela_id[0][0]
            print("TABELA_ID: ", tabela_id)
            query = "INSERT INTO pergunta_has_tabela (pergunta_Pergunta_id, tabela_Tabela_id) VALUES (%s,%s);"
            mydb_cur.execute(query, (pergunta_id,tabela_id))
            mysql.connection.commit()
            print("COMIT DA PERGUNTA HAS TABELA DEVIDAMENTE EXECUTADO! PERGUNTA_ID", pergunta_id, " TABELA_ID", tabela_id)

        query = "SELECT PERGUNTA_ID, PERGUNTA,PERGUNTA_SQL, QUERY_ID FROM PERGUNTA WHERE PERGUNTA_ID = %s;"
        # COLOCA O ULTIMO INDEX INSERIDO DENTRO DE UMA QUERY
        mydb_cur.execute(query, (pergunta_id,))
        data = mydb_cur.fetchall()

        mydb_cur.close()
        js = json.dumps(data)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/perguntas/'
        return resp
    elif request.method == 'POST' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/perguntas/'
        return resp


@app.route('/v1.0/pergunta/update/<int:pergunta_id>', methods=['POST'])
def pergunta_update(pergunta_id):
    form = PerguntaForm(request.form)
    print("IMPRIMR O FORMULARIO")
    print(dir(form))
    print("IMPRIMIR O FORM VALIDATE:")
    print(form.validate())
    print("IMPRIME OS ERROS")
    print(form.errors)
    if request.method == 'POST' and form.validate():
        pergunta = request.form["pergunta"]
        pergunta_sql = request.form["pergunta_sql"]
        query_id = request.form["query_id"]
        mydb_cur = mysql.connection.cursor()
        jg_teste_cur = MySQLdb.connect(host='localhost',
                                         database='jg_teste',
                                         user='root',
                                         password='user')
        cursor = jg_teste_cur.cursor()
        # print("ACERCA DO CURSOR:", mydb_cur)
        query = "EXPLAIN " + pergunta_sql#.upper()
        # print("QUERY:", query)
        try:

            cursor.execute(query)

        except Exception as error:

            print(error)
            print("dir error ", dir(error))
            print("error something", error.args)
            erro = {
                'erro': error.args
            }
            js = json.dumps(erro)
            print(js)
            print("ERROR SOMETHING!!!")
            return Response(js, status=406, mimetype='application/json')

        # JA COMEÇO A PENSAR NUMA FORMA DE PORTEGER ISTO E VERIFICAR SE AS TABELAS RECOLHIDAS
        # SE ENCONTRAM DENTRO DA TABELA "TABELA"
        prof_tabela = extract_tables(pergunta_sql)#.upper())

        prof_tabela_tabela = prof_tabela
        prof_tabela_tabela = list(dict.fromkeys(prof_tabela_tabela))
        #prof_tabela_tabela = [x for x in prof_tabela_tabela if not ("<" in x)]
        ###########################################################################
        print("LISTA DE TABELAS SEM REPETICOES E SUBQUERYS")

        query = "UPDATE PERGUNTA SET PERGUNTA=%s, PERGUNTA_SQL=%s, QUERY_ID=%s WHERE PERGUNTA_ID = %s;"
        mydb_cur.execute(query, (pergunta, pergunta_sql, query_id, pergunta_id))
        mysql.connection.commit()

        # APAGAR TODAS AS REFERENCIAS ANTERIORES A TABELAS 
        # DENTRO DA TABELA PERGUNTA_HAS_TABELA ELEMINAR TODAS AS REFERENCIAS A  PERGUNTA_ID
        query = "DELETE FROM PERGUNTA_HAS_TABELA WHERE PERGUNTA_PERGUNTA_ID = %s;"
        mydb_cur.execute(query, (pergunta_id,))
        mysql.connection.commit()
        # REFAZER AS TABELAS ANTERIORES A TABELAS
        for each_tabela in prof_tabela_tabela:
            print("EACH_TABELA: ", each_tabela)
            query = "SELECT TABELA_ID FROM TABELA WHERE NOME = %s;"
            mydb_cur.execute(query, (each_tabela,))
            tabela_id = mydb_cur.fetchall()
            print(tabela_id)
            tabela_id = tabela_id[0][0]
            print("TABELA_ID: ", tabela_id)
            query = "INSERT INTO pergunta_has_tabela (pergunta_Pergunta_id, tabela_Tabela_id) VALUES (%s,%s);"
            mydb_cur.execute(query, (pergunta_id,tabela_id))
            mysql.connection.commit()
            print("COMIT DA PERGUNTA HAS TABELA DEVIDAMENTE EXECUTADO! PERGUNTA_ID", pergunta_id, " TABELA_ID", tabela_id)

        query = "SELECT PERGUNTA, PERGUNTA_SQL, QUERY_ID FROM PERGUNTA WHERE PERGUNTA_ID = %s"
        mydb_cur.execute(query, (pergunta_id,))
        data = mydb_cur.fetchall()
        mydb_cur.close()
        js = json.dumps(data)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/perguntas/'
        return resp
    elif request.method == 'POST' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/perguntas/'
        return resp


@app.route('/v1.0/pergunta/delete/<int:pergunta_id>', methods=['POST'])
def pergunta_delete(pergunta_id):
    if request.method == 'POST' and request.form['_method'] == 'delete':
        query = "DELETE FROM PERGUNTA WHERE PERGUNTA_ID = %s"
        cur = mysql.connection.cursor()
        cur.execute(query, (pergunta_id,))
        mysql.connection.commit()
        cur.fetchall()
        return Response(status=200)


@app.route('/v1.0/perguntas/<int:pergunta_id>', methods=['GET'])
def perguntas_update(pergunta_id):
    form = PerguntaForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        query = "SELECT PERGUNTA_ID, PERGUNTA, PERGUNTA_SQL, QUERY_ID FROM PERGUNTA WHERE PERGUNTA_ID=%s"
        cur.execute(query, (pergunta_id,))
        data = cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:

            pergunta = {
                'pergunta_id': data[0][0],
                'pergunta': data[0][1],
                'pergunta_sql': data[0][2],
                'query_id': data[0][3]
            }

            print(json.dumps(pergunta))
            js = json.dumps(pergunta)
            return Response(js, status=200, mimetype='application/json')
    # elif request.method == 'PUT' and form.validate():
    #     pergunta = request.form["pergunta"]
    #     pergunta_sql = request.form["pergunta_sql"]
    #     query_id = request.form["query_id"]
    #     cur = mysql.connection.cursor()
    #     query = "UPDATE PERGUNTA SET PERGUNTA=%s, PERGUNTA_SQL=%s, QUERY_ID=%s WHERE PERGUNTA_ID = %s"
    #     cur.execute(query, (pergunta, pergunta_sql, query_id, pergunta_id ))
    #     mysql.connection.commit()
    #     cur.execute(
    #         "SELECT PERGUNTA, PERGUNTA_SQL, QUERY_ID FROM PERGUNTA WHERE PERGUNTA_ID = %s", (pergunta_id,))
    #     data = cur.fetchall()
    #     cur.close()
    #     js = json.dumps(data)
    #     resp = Response(js, status=200, mimetype='application/json')
    #     resp.headers['Links'] = 'http://127.0.0.1/perguntas/'
    #     return resp
    # elif request.method == 'PUT' and not form.validate():
    #     resp = Response(status=400)
    #     resp.headers['Links'] = 'http://127.0.0.1/perguntas/'
    #     return resp
    # elif request.method == 'DELETE':
    #     query = "DELETE FROM PERGUNTA WHERE PERGUNTA_ID = %s"
    #     cur = mysql.connection.cursor()
    #     cur.execute(query, (pergunta_id,))
    #     mysql.connection.commit()
    #     cur.fetchall()
    #     return Response(status=200)


@app.route('/v1.0/pergunta_has_tabela/<int:pergunta_id>', methods=['GET'])
def pergunta_has_tabela(pergunta_id):
    if request.method == 'GET':
        mydb_cur = mysql.connection.cursor()
        query = 'SELECT pergunta_Pergunta_id, tabela_Tabela_id FROM pergunta_has_tabela WHERE pergunta_Pergunta_id = %s;'
        mydb_cur.execute(query, (pergunta_id,))
        data = mydb_cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:
            pergunta_has_tabelas = []
            
            for elm in data:
                pergunta_has_tabela = {
                    'pergunta_Pergunta_id': elm[0],
                    'tabela_Tabela_id': elm[1]
                }
                pergunta_has_tabelas.append(pergunta_has_tabela)
            js = json.dumps(pergunta_has_tabelas)
            return Response(js, status=200, mimetype='application/json')

@app.route('/v1.0/resposta_has_tabela/<int:resposta_id>/', methods=['GET'])
def resposta_has_tabela(resposta_id):
    print("DATA RESPOSTA HAS TABELA ANTES DO GET: ")
    if request.method == 'GET':
        mydb_cur = mysql.connection.cursor()
        query = 'SELECT resposta_Resposta_id, tabela_Tabela_id FROM resposta_has_tabela WHERE resposta_Resposta_id = %s;'
        mydb_cur.execute(query, (resposta_id,))
        data = mydb_cur.fetchall()
        print("DATA RESPOSTA HAS TABELA DATA: ", data)
        if len(data) <= 0:
            return Response(status=404)
        else:
            resposta_has_tabelas = []

            for each in data:
                resposta_has_tabela = {
                    'resposta_Resposta_id': each[0],
                    'tabela_Tabela_id': each[1]
                }
                resposta_has_tabelas.append(resposta_has_tabela)
            print("DATA RESPOSTA HAS TABELA JSON: ", data)    
            js = json.dumps(resposta_has_tabelas)
            return Response(js, status=200, mimetype='application/json')

@app.route('/v1.0/tabela/<int:tabela_id>', methods=['GET'])
def tabela(tabela_id):
    if request.method == 'GET':
        mydb_cur = mysql.connection.cursor()
        query = 'SELECT tabela_id, nome FROM tabela WHERE tabela_id = %s;'
        mydb_cur.execute(query, (tabela_id,))
        data = mydb_cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:
            tabela = {
                'tabela_id': data[0][0],
                'nome': data[0][1]
            }
            js = json.dumps(tabela)
            return Response(js, status=200, mimetype='application/json')    

# @app.route('/v1.0/about')
# def about():
#     return render_template('about.html')


@app.errorhandler(404)
def page_not_found(e):
    return 'This page was not found'



if __name__ == '__main__':
    app.run(port=80, debug=True)
