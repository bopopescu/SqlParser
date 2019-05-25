from flask import Flask, render_template, url_for, redirect, jsonify
from flask_mysqldb import MySQL
from flask import request, Response
import json
import yaml
from wtforms import *

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

app = Flask(__name__, instance_relative_config=True)
app.config.from_object(__name__)

db = yaml.load(open('db.yaml'))
app.config['MYSQL_HOST'] = db['mysql_host']
app.config['MYSQL_USER'] = db['mysql_user']
app.config['MYSQL_PASSWORD'] = db['mysql_password']
app.config['MYSQL_DB'] = db['mysql_db']

mysql = MySQL(app)

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
    query_id = StringField('query_id', [validators.Length(min=2,max=45)])


class RespostaForm(Form):
    resposta_sql = StringField(
        'resposta_sql', [validators.Length(min=0, max=255)])


class ResultadoForm(Form):
    numero_de_linhas_iguais = IntegerField(
        'numero_de_linhas_iguais', [validators.required()])
    numero_de_colunas_iguais = IntegerField(
        'numero_de_colunas_iguais', [validators.required()])
    colunas_totais = IntegerField('colunas_totais', [validators.required()])
    colunas_iguais = IntegerField('colunas_iguais', [validators.required()])
    campos_totais = IntegerField('campos_totais', [validators.required()])
    campos_iguais = IntegerField('campos_iguais', [validators.required()])


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
                #'aluno_id' : elm[0],
                'nome': elm[1],
                'numero': elm[2]#,
                #'Password': elm[3]
            }
            alunos.append(aluno)
        #print(data.row0)
        #return render_template('/alunos/alunos.html', students=data)
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


@app.route('/v1.0/aluno/<int:aluno_id>', methods=['PUT', 'DELETE', 'GET'])
def alunos_update(aluno_id):
    form = AlunoForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        query = "SELECT NOME, NUMERO FROM aluno WHERE NUMERO=%s"
        cur.execute(query, (aluno_id,))
        data = cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:
            aluno = {
                'nome': data[0][0],
                'numero': data[0][1]
            }
            js = json.dumps(aluno)
            resp =  Response(js, status=200, mimetype='application/json')
            resp.headers['Links'] = 'http://127.0.0.1/aluno'
            return resp
    elif request.method == 'PUT' and form.validate():
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
    elif request.method == 'PUT' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/aluno'
        return resp
    elif request.method == 'DELETE':
        query = "DELETE FROM aluno WHERE aluno_ID = %s"
        cur = mysql.connection.cursor()
        cur.execute(query, (aluno_id,))
        mysql.connection.commit()
        cur.fetchall()
        return Response(status=200)

@app.route('/v1.0/respostas/', methods=['GET'])
def respostas():
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute('SELECT RESPOSTA_ID, RESPOSTA_SQL FROM resposta;')
        data = cur.fetchall()
        cur.close()
        alunos = []

        for elm in data:
            aluno = {
                'resposta_id': elm[0],
                'resposta_sql': elm[1]
            }
            alunos.append(aluno)
        #return render_template('/respostas/respostas.html', respostas=data)
        return jsonify(data)

@app.route('/v1.0/resposta/<int:pergunta_id>/<int:aluno_id>',  methods=['POST'])
def resposta_insert(pergunta_id, aluno_id):
    form = RespostaForm(request.form)
    if request.method == 'POST' and form.validate():
        # VERIFICA SE OS FORMULARIOS ESTAO BEM FEITOS
        resposta_sql = request.form["resposta_sql"]
        # INSERE NA BASE DE DADOS A NOVA RESPOSTA
        cur = mysql.connection.cursor()
        # try:
        #     #from flask_mysqldb import MySQL
        #     #mysql = MySQL(app)
        #     #global mysql
            
        # except Error as e:
        #     print()
        #     print("Error while connecting to MySQL", e)
        #     print()
        # finally:
        #     from flask_mysqldb import MySQL
        #     mysql = MySQL(app)
        #     print("MYSQL *************************")
        #     print(mysql)
        #     conn = mysql.connection()
        #     cur = conn.cursor()

        query = "INSERT INTO resposta (pergunta_pergunta_id, aluno_aluno_id, resposta_sql) VALUES (%s,%s,%s);"
        cur.execute(query, (pergunta_id, aluno_id, resposta_sql))
        mysql.connection.commit()
        # SELECIONA O ULTIMO INDEX INSERIDO
        cur.execute("SELECT LAST_INSERT_ID();")
        data_last_inserted_id = cur.fetchall()
        last_inserted_id = data_last_inserted_id[0][0]
        # PREPARA ESTRUTURA PARA COMPARACAO DE TABELAS VIA MYSQL AND DATASETS

        # PESQUISA O QUERY MYSQL DA PERGUNDA DO PROFESSOR
        query = "SELECT PERGUNTA_SQL,QUERY_ID FROM PERGUNTA WHERE PERGUNTA_ID = %s"
        cur.execute(query,(pergunta_id,))
        data = cur.fetchall()

        sqlquery = data[0][0]
        sqlquery.upper()

        # PESQUISA O QUERY MYSQL DA RESPOSTA DO ALUNO
        sqlqueryal = resposta_sql
        sqlqueryal.upper()

        #sqlparsed = sqlparse.parse(sqlquery)

        sqlquerykey = data[0][1]

        # import mysql.connector 

        # connection = MySQLdb.connect(host='localhost',
        #                              database='jg_teste',
        #                              user='root',
        #                              password='user')

        print()
        #import mysql.connector 
        try:
            print("Cria a connexão!")
            print()
            connection = MySQLdb.connect(host='localhost',
                                                database='jg_teste',
                                                user='root',
                                                password='user')

            if connection:#.is_connected():

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

                print("IMPRIME A DATA_FRAME_KEY")
                print(sqlquerykey)
                # ACRESCENTAR O ID A QUERY #
                ############################
                print()
                print("CHAVE:")
                print(sqlquerykey)
                print()
                print("SQL QUERYS")
                print(sqlquery)
                sqlqueryprof = sqlquery #acrescentar_id(sqlquery, sqlquerykey)
                print(sqlqueryprof)
                print()
                print(sqlqueryal)
                sqlqueryalun = sqlqueryal #acrescentar_id(sqlqueryal, sqlquerykey)
                print(sqlqueryalun)

                # EXECUTAR CADA UMA DAS QUERYS
                cursor_prof = connection.cursor()
                cursor_prof.execute(sqlqueryprof)
                records_prof = cursor_prof.fetchall()

                data_frame_prof = pandas.DataFrame(list(records_prof))
                # print(data_frame_prof)

                cursor_alun = connection.cursor()
                cursor_alun.execute(sqlqueryalun)
                records_query = cursor_alun.fetchall()

                data_frame_alun = pandas.DataFrame(list(records_query))
                print()

                # COMPARACAO DOS DATA FRAMES
                data_prof = pandas.read_sql(sqlqueryprof, con=connection)
                data_alun = pandas.read_sql(sqlqueryalun, con=connection)

                # datacompy
                compare = datacompy.Compare(
                    data_prof,
                    data_alun,
                    join_columns=sqlquerykey)

                # IMPRIMIR RESULTADOS DA COMPARACAO ##
                ######################################
                print("#######################################################################################################")
                print("Colunas que intreceptaram", len(compare.intersect_columns()))
                print("Linhas que intreceptaram", compare.intersect_rows.shape[0])
                print()
                print("Linha unicas no DataFrame 1:", len(compare.df1_unq_rows))
                print("Linha unicas no DataFrame 2:", len(compare.df2_unq_rows))
                print()
                print("Colunas unicas de Dataframe 1 :", compare.df1_unq_columns())
                print("Colunas unicas de Dataframe 2 :", compare.df2_unq_columns())

                cursor_prof.rowcount

                numero_linhas_totais = len(compare.df1_unq_rows)
                numero_linhas_iguais = compare.intersect_rows.shape[0]
                numero_colunas_iguais = len(compare.intersect_columns())
                print(len(compare.intersect_columns()))

                # AINDA ESTA POR CALCULAR
                colunas_totais = 99999
                colunas_iguais = 99999
                
                table_prof = extract_tables(sqlqueryprof)
                table_alun = extract_tables(sqlqueryalun)

                campos_totais = len(table_prof)
                print(" * TABELAS")
                print(set(table_prof) & set(table_alun))
                campos_iguais = len(set(table_prof) & set(table_alun))

                pergunta_pergunta_id = pergunta_id
                resposta_resposta_id = last_inserted_id
                resposta_aluno_aluno_id = aluno_id
                resposta_pergunta_pergunta_id = pergunta_id

                query = "INSERT INTO RESULTADO (numero_linhas_iguais,numero_colunas_iguais,colunas_totais,colunas_iguais,campos_totais,campos_iguais,Pergunta_Pergunta_id,Resposta_Resposta_id,Resposta_Aluno_Aluno_id,Resposta_Pergunta_Pergunta_id)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                cur.execute(query,(numero_linhas_iguais,numero_colunas_iguais, colunas_totais,colunas_iguais,campos_totais,campos_iguais,pergunta_pergunta_id,resposta_resposta_id,resposta_aluno_aluno_id,resposta_pergunta_pergunta_id))
                mysql.connection.commit()

                # print(data_frame_prof)
        except Error as e:
            print("Error while connecting to MySQL", e)
            print()
        finally:
            # closing database connection.
            if(connection):#.is_connected()):
                #cursor_prof.close()
                #cursor_alun.close()
                connection.close()
                print("MySQL connection is closed")

        
        # SELECIONA A RESPOSTA ANTERIORMENTE INSERIDA
        query = "SELECT RESPOSTA_SQL FROM RESPOSTA WHERE RESPOSTA_ID = %s;"
        cur.execute(query, (last_inserted_id,))
        data = cur.fetchall()
        cur.close()

        resposta = {
            'respotas_sql': data[0][0]
        }

        js = json.dumps(resposta)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp
    elif request.method == 'POST' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp


@app.route('/v1.0/resposta/<int:resposta_id>', methods=['PUT', 'DELETE', 'GET'])
def respostas_update(resposta_id):
    form = RespostaForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        query = "SELECT RESPOSTA_SQL FROM RESPOSTA WHERE RESPOSTA_ID=%s"
        cur.execute(query, (resposta_id,))
        data = cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:
            resposta = { 
                'resposta_sql': data[0][0]
            }
            js = json.dumps(resposta)
            return Response(js, status=200, mimetype='application/json')
    elif request.method == 'PUT' and form.validate():
        resposta_sql = request.form["resposta_sql"]
        cur = mysql.connection.cursor()
        query = "UPDATE RESPOSTA SET RESPOSTA_SQL=%s WHERE RESPOSTA_ID = %s;"
        cur.execute(query, (resposta_sql,resposta_id))
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
    elif request.method == 'PUT' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp
    elif request.method == 'DELETE':
        query = "DELETE FROM RESPOSTA WHERE RESPOSTA_ID = %s;"
        cur = mysql.connection.cursor()
        cur.execute(query, (resposta_id,))
        mysql.connection.commit()
        cur.fetchall()
        return Response(status=200)

@app.route('/v1.0/perguntas/', methods=['POST', 'GET'])
def perguntas():
    form = PerguntaForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute("SELECT PERGUNTA,PERGUNTA_SQL FROM PERGUNTA;")
        data = cur.fetchall()
        cur.close()
        #return render_template('/perguntas/perguntas.html', perguntas=data)
        return jsonify(data)
    elif request.method == 'POST' and form.validate():
        # RECEBE OS DADOS DO REQUEST
        pergunta = request.form["pergunta"]
        pergunta_sql = request.form["pergunta_sql"]
        query_id = request.form["query_id"]
        # INSERE UM NOVA PERGUNTA DENTRO DA TABELA PERGUNTA
        cur = mysql.connection.cursor()
        query = "INSERT INTO PERGUNTA (PERGUNTA, PERGUNTA_SQL,query_id) VALUES (%s,%s,%s);"
        cur.execute(query, (pergunta, pergunta_sql,query_id))
        mysql.connection.commit()
        # SELECIONA O ULTIMO INDEX INSERIDO
        cur.execute("SELECT LAST_INSERT_ID()")
        data_last_inserted_id = cur.fetchall()
        last_inserted_id = data_last_inserted_id[0][0]
        query = "SELECT PERGUNTA,PERGUNTA_SQL FROM PERGUNTA WHERE PERGUNTA_ID = %s;"
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


@app.route('/v1.0/perguntas/<int:pergunta_id>', methods=['PUT', 'DELETE', 'GET'])
def perguntas_update(pergunta_id):
    form = PerguntaForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        query = "SELECT PERGUNTA, PERGUNTA_SQL FROM PERGUNTA WHERE PERGUNTA_ID=%s"
        cur.execute(query, (pergunta_id,))
        data = cur.fetchall()
        if len(data) <= 0:
            return Response(status=404)
        else:
            js = json.dumps(data)
            return Response(js, status=200, mimetype='application/json')
    elif request.method == 'PUT' and form.validate():
        pergunta = request.form["pergunta"]
        pergunta_sql = request.form["pergunta_sql"]
        cur = mysql.connection.cursor()
        query = "UPDATE aluno SET PERGUNTA=%s, PERGUNTA_SQL=%s WHERE PREGUNTA_ID = %s"
        cur.execute(query, (pergunta, pergunta_sql, pergunta_id))
        mysql.connection.commit()
        cur.execute(
            "SELECT PERGUNTA, PERGUNTA_ID FROM PERGUNTA WHERE PERGUNTA_ID = %s", (pergunta_id,))
        data = cur.fetchall()
        cur.close()
        js = json.dumps(data)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/perguntas/'
        return resp
    elif request.method == 'PUT' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/perguntas/'
        return resp
    elif request.method == 'DELETE':
        query = "DELETE FROM PERGUNTA WHERE PERGUNTA_ID = %s"
        cur = mysql.connection.cursor()
        cur.execute(query, (pergunta_id,))
        mysql.connection.commit()
        cur.fetchall()
        return Response(status=200)

@app.route('/v1.0/resultados/', methods=['GET'])
def resultados():
    form = ResultadoForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute('SELECT numero_linhas_iguais,numero_colunas_iguais,colunas_totais,colunas_iguais,campos_totais,campos_iguais,Pergunta_Pergunta_id,Resposta_Resposta_id,Resposta_Aluno_Aluno_id,Resposta_Pergunta_Pergunta_id FROM resultado;')
        data = cur.fetchall()
        cur.close()
        if len(data) <= 0:
            return Response(status=404)
        else:
            resultados = []

            for elm in data:
                resultado = {
                    'numero_linhas_iguais': elm[0],
                    'numero_colunas_iguais': elm[1],
                    'colunas_totais': elm[2],
                    'colunas_iguais': elm[3],
                    'campos_totais': elm[4],
                    'campos_iguais': elm[5],
                    'Pergunta_Pergunta_id': elm[6],
                    'Resposta_Resposta_id': elm[7],
                    'Resposta_Aluno_Aluno_id': elm[8],
                    'Resposta_Pergunta_Pergunta_id': elm[9]
                }
                resultados.append(resultado)
            js = json.dumps(resultados)
            return Response(js, status=200, mimetype='application/json')
        #return render_template('/resultados/resultados.html', resultados=data)

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
            return Response(js,status=200,mimetype='application/json')

@app.route('/v1.0/about')
def about():
    return render_template('about.html')


@app.errorhandler(404)
def page_not_found(e):
    return 'This page was not found'


if __name__ == '__main__':
    app.run(port=80,debug=True)
