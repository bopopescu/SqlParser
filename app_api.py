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
from sqlparse.sql import IdentifierList, Identifier
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
CORS(app)

# ACRESCENTAR O ID


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
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                raise StopIteration
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_real_name()  # .get_name()
        elif isinstance(item, Identifier):
            yield item.get_real_name()  # .get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value  # .value


def extract_tables(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    return list(extract_table_identifiers(stream))


class AlunoForm(Form):
    nome = StringField('nome', [validators.Length(min=3, max=45)])
    numero = IntegerField('numero', [validators.required()])
    password = PasswordField('password', [validators.DataRequired()])


class PerguntaForm(Form):
    pergunta = StringField('pergunta', [validators.Length(min=15, max=255)])
    pergunta_sql = StringField(
        'pergunta_sql', [validators.Length(min=12, max=255)])
    query_id = StringField('query_id', [validators.Length(min=2, max=45)])


class RespostaForm(Form):
    resposta_sql = StringField(
        'resposta_sql', [validators.Length(min=0, max=255)])
    aluno_aluno_id = IntegerField('aluno_aluno_id', [validators.required()])
    pergunta_pergunta_id = IntegerField('pergunta_pergunta_id', [validators.required()])


# class ResultadoForm(Form):
#     numero_de_linhas_iguais = IntegerField(
#         'numero_de_linhas_iguais', [validators.required()])
#     numero_de_colunas_iguais = IntegerField(
#         'numero_de_colunas_iguais', [validators.required()])
#     colunas_totais = IntegerField('colunas_totais', [validators.required()])
#     colunas_iguais = IntegerField('colunas_iguais', [validators.required()])
#     campos_totais = IntegerField('campos_totais', [validators.required()])
#     campos_iguais = IntegerField('campos_iguais', [validators.required()])


@app.route('/v1.0/alunos/', methods=['POST', 'GET'])
def alunos():
    form = AlunoForm(request.form)
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
            'SELECT RESPOSTA_ID, RESPOSTA_SQL, ALUNO_ALUNO_ID, PERGUNTA_PERGUNTA_ID FROM resposta;')
        data = cur.fetchall()
        cur.close()
        respostas = []

        for elm in data:
            resposta = {
                'resposta_id': elm[0],
                'resposta_sql': elm[1],
                'aluno_aluno_id': elm[2],
                'pergunta_pergunta_id': elm[3]
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
        resposta_sql_alun = resposta_sql_alun.upper()
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
        cursor.execute(query)
        # Verifica se o comando pode ser executado 
        # Caso não seja levanta uma excepção
        explain_alun = cursor.fetchall()
        print("EXPLAIN ALUN")
        print(explain_alun)
        alun_tabela = []
        for each_tabela in explain_alun:
            alun_tabela.append(each_tabela[2])
        print("#########################")
        print("## TABELAS ENCONTRADAS ##")
        print("#########################")
        print("TABELAS ENCONTRADAS NA QUERY ALUNO")
        print(alun_tabela)
        print("TABELAS ENCONTRADAS NA QUERY ALUNO - SEM REPETICAO")
        alun_tabela = list(dict.fromkeys(alun_tabela))
        print(alun_tabela)
        # PESQUISA O QUERY MYSQL DA PERGUNDA DO PROFESSOR
        query = "SELECT PERGUNTA_SQL,QUERY_ID FROM PERGUNTA WHERE PERGUNTA_ID = %s"
        mydb_cur.execute(query, (pergunta_id,))
        data_prof = mydb_cur.fetchall()
        print("DATA_PROF")
        print(data_prof)
        # VERIFICA TAMANHO data_prof 
        # Levanta Excepção
        resposta_sql_prof = data_prof[0][0]
        resposta_sql_prof.upper()
        query = "EXPLAIN " + resposta_sql_prof.upper()
        print("QUERY EXPLAIN PROF")
        print(query)
        cursor.execute(query)
        explain_prof = cursor.fetchall()
        print("EXPLAIN_PROF")
        print(explain_prof)
        prof_tabela = []
        for each_tabela in explain_prof:
            prof_tabela.append(each_tabela[2])

        print("TABELAS ENCONTRADAS NA QUERY PROFESSOR")
        print(prof_tabela)
        print("TABELAS ENCONTRADAS NA QUERY PROFESSOR - SEM REPETICAO")
        prof_tabela = list(dict.fromkeys(prof_tabela))
        print(prof_tabela)

        join_alun_prof_tabela = set(alun_tabela) & set(prof_tabela)
        print("JOIN TABELAS ALUNO E PROFESSOR - SEM REPETICAO")
        print(join_alun_prof_tabela)

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

        sqlquery = data_prof[0][0]
        sqlquery.upper()

        # PESQUISA O QUERY MYSQL DA RESPOSTA DO ALUNO
        sqlqueryal = resposta_sql
        sqlqueryal.upper()

        # sqlparsed = sqlparse.parse(sqlquery)

        # DETERMINA A PARTIR DA TABELA PERGUNTA O QUERY ID A SER UTILIZADO
        sqlquerykey = data_prof[0][1]

        print()
        # import mysql.connector

        print("IMPRIME A DATA_FRAME_KEY")
        print(sqlquerykey)
        # ACRESCENTAR O ID A QUERY #
        ############################
        print()
        print("CHAVE:")
        print(sqlquerykey)
        print()
        # print("SQL QUERYS")
        # print(sqlquery)
        # acrescentar_id(sqlquery, sqlquerykey)
        print("SQL QUERY DO PROFESSOR:")
        sqlqueryprof = sqlquery
        print(sqlqueryprof)
        print()
        # print()
        # print(sqlqueryal)
        # # acrescentar_id(sqlqueryal, sqlquerykey)
        print("SQL QUERY DO ALUNO:")
        sqlqueryalun = sqlqueryal
        print(sqlqueryalun)
        print()

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
        print("Linhas que intreceptaram",
              compare.intersect_rows.shape[0])
        print()
        print("Linha unicas no DataFrame 1:",
              len(compare.df1_unq_rows))
        print("Linha unicas no DataFrame 2:",
              len(compare.df2_unq_rows))
        # print()
        # print("Colunas unicas de Dataframe 1 :",
        #       compare.df1_unq_columns())
        # print("Colunas unicas de Dataframe 2 :",
        #       compare.df2_unq_columns())

        # print("IMPRIME LINHAS UNICAS")
        # print(data_prof)
        numero_linhas_totais = len(data_prof)
        print("NUMERO DE LINHAS EM DATAFRAME 1")
        print(numero_linhas_totais)
        print("NUMERO DE LINHAS IGUAIS")
        numero_linhas_iguais = compare.intersect_rows.shape[0]
        print(numero_linhas_iguais)
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
        print("TABELAS IGUAIS", tabelas_iguais)

        query = "INSERT INTO RESULTADO (numero_linhas_iguais, numero_linhas_totais, tabelas_totais, tabelas_iguais, Pergunta_Pergunta_id) VALUES (%s,%s,%s,%s,%s);"
        mydb_cur.execute(query, (numero_linhas_iguais, numero_linhas_totais, tabelas_totais, tabelas_iguais, pergunta_pergunta_id))
        mysql.connection.commit()
        print("COMIT RESULTADO BEM EXECUTADO")
        # SELECIONA O ULTIMO INDEX INSERIDO
        mydb_cur.execute("SELECT LAST_INSERT_ID();")
        data_last_inserted_id = mydb_cur.fetchall()
        print(data_last_inserted_id)
        last_inserted_id = data_last_inserted_id[0][0]
        resultado_resultado_id = last_inserted_id
        resultado_pergunta_pregunta_id = pergunta_id
        print("SELECT LAST INSERT INDEX DE RESULTADO OK:", resultado_resultado_id)

        # Insere a resposta no lugar certo da base de dados
        # query = "INSERT INTO RESPOSTA (pergunta_pergunta_id, Aluno_Aluno_id, Resposta_sql, resultado_Resultado_id, resposta_Pergunta_Pergunta_id) VALUES (%s,%s,%s,%s,%s);"
        query = "INSERT INTO `resposta`(`Resposta_sql`,`Aluno_Aluno_id`,`Pergunta_Pergunta_id`,`resultado_Resultado_id`,`resultado_Pergunta_Pergunta_id`) VALUES (%s,%s,%s,%s,%s);"
        mydb_cur.execute(query, (resposta_sql, aluno_id, pergunta_id,  resultado_resultado_id, resultado_pergunta_pregunta_id))
        mysql.connection.commit()

        print("COMIT RESPOSTA BEM EXECUTADO")
        # SELECIONA O ULTIMO INDEX INSERIDO
        mydb_cur.execute("SELECT LAST_INSERT_ID();")
        data_last_inserted_id = mydb_cur.fetchall()
        last_inserted_id = data_last_inserted_id[0][0]
        resposta_id = last_inserted_id

        # SELECIONA A RESPOSTA ANTERIORMENTE INSERIDA
        query = "SELECT RESPOSTA_SQL FROM RESPOSTA WHERE RESPOSTA_ID = %s;"
        mydb_cur.execute(query, (resposta_id,))
        data_last_resposta = mydb_cur.fetchall()
        mydb_cur.close()

        resposta = {
            'respotas_sql': data_last_resposta[0][0],
            'resultados_sql': {'resultado_id': resultado_resultado_id,
                               'numero_linhas_iguais': numero_linhas_iguais, 
                               'numero_linhas_totais': numero_linhas_totais,
                               'tabelas_totais': tabelas_totais,
                               'tabelas_iguais': tabelas_iguais,
                               'Pergunta_Pergunta_id': pergunta_pergunta_id}
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
        query = "SELECT RESPOSTA_ID, resposta_sql, aluno_aluno_id, pergunta_pergunta_id FROM RESPOSTA WHERE RESPOSTA_ID=%s"
        cur.execute(query, (resposta_id,))
        data = cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:
            resposta = {
                'resposta_id': data[0][0],
                'resposta_sql': data[0][1],
                'aluno_aluno_id': data[0][2],
                'pergunta_pergunta_id': data[0][3]
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
        cur = mysql.connection.cursor()
        query = "UPDATE RESPOSTA SET RESPOSTA_SQL=%s WHERE RESPOSTA_ID = %s;"
        cur.execute(query, (resposta_sql, resposta_id))
        mysql.connection.commit()
        cur.execute(
            "SELECT RESPOSTA_SQL FROM RESPOSTA WHERE RESPOSTA_ID = %s;", (resposta_id,))
        data = cur.fetchall()
        cur.close()
        resposta = {
            'resposta_sql': data[0][0]
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
        cur.execute("SELECT PERGUNTA_ID, PERGUNTA,PERGUNTA_SQL FROM PERGUNTA;")
        data = cur.fetchall()
        cur.close()

        perguntas = []

        for elm in data:
            aluno = {
                'pergunta_id': elm[0],
                'pergunta': elm[1],
                'pergunta_sql': elm[2]
            }
            perguntas.append(aluno)

        # return render_template('/perguntas/perguntas.html', perguntas=data)
        return jsonify(perguntas)
    elif request.method == 'POST' and form.validate():
        # RECEBE OS DADOS DO REQUEST
        pergunta = request.form["pergunta"]
        pergunta_sql = request.form["pergunta_sql"]
        query_id = request.form["query_id"]
        # INSERE UM NOVA PERGUNTA DENTRO DA TABELA PERGUNTA
        cur = mysql.connection.cursor()
        query = "INSERT INTO PERGUNTA (PERGUNTA, PERGUNTA_SQL,query_id) VALUES (%s,%s,%s);"
        cur.execute(query, (pergunta, pergunta_sql, query_id))
        mysql.connection.commit()
        # SELECIONA O ULTIMO INDEX INSERIDO
        cur.execute("SELECT LAST_INSERT_ID()")
        data_last_inserted_id = cur.fetchall()
        last_inserted_id = data_last_inserted_id[0][0]
        query = "SELECT PERGUNTA_ID, PERGUNTA,PERGUNTA_SQL FROM PERGUNTA WHERE PERGUNTA_ID = %s;"
        # COLOCA O ULTIMO INDEX INSERIDO DENTRO DE UMA QUERY
        cur.execute(query, (last_inserted_id,))
        data = cur.fetchall()
        cur.close()
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
    if request.method == 'POST' and form.validate():
        pergunta = request.form["pergunta"]
        pergunta_sql = request.form["pergunta_sql"]
        query_id = request.form["query_id"]
        cur = mysql.connection.cursor()
        query = "UPDATE PERGUNTA SET PERGUNTA=%s, PERGUNTA_SQL=%s, QUERY_ID=%s WHERE PERGUNTA_ID = %s"
        cur.execute(query, (pergunta, pergunta_sql, query_id, pergunta_id))
        mysql.connection.commit()
        cur.execute(
            "SELECT PERGUNTA, PERGUNTA_SQL, QUERY_ID FROM PERGUNTA WHERE PERGUNTA_ID = %s", (pergunta_id,))
        data = cur.fetchall()
        cur.close()
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


@app.route('/v1.0/resultados/', methods=['GET'])
def resultados():
    # form = ResultadoForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute('SELECT resultado_id, numero_linhas_iguais, numero_linhas_totais, numero_colunas_iguais,colunas_totais,colunas_iguais,campos_totais,campos_iguais,Pergunta_Pergunta_id,Resposta_Resposta_id,Resposta_Aluno_Aluno_id,Resposta_Pergunta_Pergunta_id FROM resultado;')
        data = cur.fetchall()
        cur.close()
        if len(data) <= 0:
            return Response(status=404)
        else:
            resultados = []

            for elm in data:
                resultado = {
                    'resultado_id': elm[0],
                    'numero_linhas_iguais': elm[1],
                    'numero_linhas_totais': elm[2],
                    'numero_colunas_iguais': elm[3],
                    'colunas_totais': elm[4],
                    'colunas_iguais': elm[5],
                    'campos_totais': elm[6],
                    'campos_iguais': elm[7],
                    'pergunta_pergunta_id': elm[8],
                    'resposta_resposta_id': elm[9],
                    'resposta_aluno_aluno_id': elm[10],
                    'resposta_pergunta_pergunta_id': elm[11]
                }
                resultados.append(resultado)
            js = json.dumps(resultados)
            return Response(js, status=200, mimetype='application/json')
        # return render_template('/resultados/resultados.html', resultados=data)


@app.route('/v1.0/resultado/<int:resultado_id>', methods=['GET'])
def resultados_update(resultado_id):
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        query = 'SELECT numero_linhas_iguais,numero_colunas_iguais,colunas_totais,colunas_iguais,campos_totais,campos_iguais,Pergunta_Pergunta_id,Resposta_Resposta_id,Resposta_Aluno_Aluno_id,Resposta_Pergunta_Pergunta_id FROM resultado WHERE RESULTADO_ID = %s;'
        cur.execute(query, (resultado_id,))
        data = cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:
            resultado = {
                'numero_linhas_iguais': data[0][0],
                'numero_colunas_iguais': data[0][1],
                'colunas_totais': data[0][2],
                'colunas_iguais': data[0][3],
                'campos_totais': data[0][4],
                'campos_iguais': data[0][5],
                'Pergunta_Pergunta_id': data[0][6],
                'Resposta_Resposta_id': data[0][7],
                'Resposta_Aluno_Aluno_id': data[0][8],
                'Resposta_Pergunta_Pergunta_id': data[0][9]
            }
            js = json.dumps(resultado)
            return Response(js, status=200, mimetype='application/json')


@app.route('/v1.0/about')
def about():
    return render_template('about.html')


@app.errorhandler(404)
def page_not_found(e):
    return 'This page was not found'


@app.route('/')
@app.route('/home')
def home():
    return render_template('home.html')


@app.route('/teste/')
def teste():
    # dates = DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD')

    return '<h3>Teste</h3>'


if __name__ == '__main__':
    app.run(port=80, debug=True)
