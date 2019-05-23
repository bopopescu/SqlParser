from flask import Flask, render_template, url_for, redirect
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


app = Flask(__name__, instance_relative_config=True)
mysql = MySQL()
app.config.from_object(__name__)

db = yaml.load(open('db.yaml'))
app.config['MYSQL_HOST'] = db['mysql_host']
app.config['MYSQL_USER'] = db['mysql_user']
app.config['MYSQL_PASSWORD'] = db['mysql_password']
app.config['MYSQL_DB'] = db['mysql_db']
mysql.init_app(app)


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


@app.route('/alunos/', methods=['POST', 'GET'])
def alunos():
    form = AlunoForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute('SELECT * FROM aluno;')
        data = cur.fetchall()
        cur.close()
        return render_template('/alunos/alunos.html', students=data)
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
        js = json.dumps(data)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/alunos/'
        return resp
    elif request.method == 'POST' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/alunos/'
        return resp


@app.route('/alunos/<int:aluno_id>', methods=['PUT', 'DELETE', 'GET'])
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
            js = json.dumps(data)
            return Response(js, status=200, mimetype='application/json')
    elif request.method == 'PUT' and form.validate():
        nome = request.form["nome"]
        numero = request.form["numero"]
        password = request.form["password"]
        cur = mysql.connection.cursor()
        query = "UPDATE aluno SET NOME=%s, NUMERO=%s, PASSWORD=%s WHERE NUMERO = %s"
        cur.execute(query, (nome, numero, password, aluno_id))
        mysql.connection.commit()
        cur.execute(
            "SELECT NOME, NUMERO FROM aluno WHERE aluno_ID = %s", (aluno_id,))
        data = cur.fetchall()
        cur.close()
        js = json.dumps(data)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/alunos'
        return resp
    elif request.method == 'PUT' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/alunos'
        return resp
    elif request.method == 'DELETE':
        query = "DELETE FROM aluno WHERE aluno_ID = %s"
        cur = mysql.connection.cursor()
        cur.execute(query, (aluno_id,))
        mysql.connection.commit()
        cur.fetchall()
        return Response(status=200)


@app.route('/respostas/', methods=['GET'])
def respostas():
    form = RespostaForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute('SELECT RESPOSTA_SQL FROM resposta;')
        data = cur.fetchall()
        cur.close()
        return render_template('/respostas/respostas.html', respostas=data)

@app.route('/respostas/<int:pergunta_id>/<int:aluno_id>',  methods=['POST'])
def resposta_insert(pergunta_id, aluno_id):
    form = RespostaForm(request.form)
    if request.method == 'POST' and form.validate():
        # VERIFICA SE OS FORMULARIOS ESTAO BEM FEITOS
        resposta_sql = request.form["resposta_sql"]
        # INSERE NA BASE DE DADOS A NOVA RESPOSTA
        try:
            from flask_mysqldb import MySQL
            mysql = MySQL(app)
            #global mysql
            cur = mysql.connection.cursor()
        except Error as e:
            print()
            print("Error while connecting to MySQL", e)
            print()
        finally:
            from flask_mysqldb import MySQL
            mysql = MySQL(app)
            print("MYSQL *************************")
            print(mysql)
            conn = mysql.connection()
            cur = conn.cursor()

        query = "INSERT INTO resposta (pergunta_id, aluno_id, resposta_sql) VALUES (%s,%s,%s);"
        cur.execute(query, (pergunta_id, aluno_id, resposta_sql))
        mysql.connection.commit()
        # SELECIONA O ULTIMO INDEX INSERIDO
        cur.execute("SELECT LAST_INSERT_ID();")
        data_last_inserted_id = cur.fetchall()
        last_inserted_id = data_last_inserted_id[0][0]
        # PREPARA ESTRUTURA PARA COMPARACAO DE TABELAS VIA MYSQL AND DATASETS

        # PESQUISA O QUERY MYSQL DA PERGUNDA DO PROFESSOR
        query = "SELECT PERGUNTA_SQL,QUERY_ID FROM PERGUNTA WHERE PERGUNDA_ID = %s"
        cur.execute(query,(pergunta_id,))
        data = cur.fetchall()

        sqlquery = data[0][0]
        sqlquery.upper()

        # PESQUISA O QUERY MYSQL DA RESPOSTA DO ALUNO
        sqlqueryal = resposta_sql
        sqlqueryal.upper()

        #sqlparsed = sqlparse.parse(sqlquery)

        sqlquerykey = data[0][1]

        # print()
        import mysql.connector
        try:
            print("Cria a connexão!")
            print()
            connection = mysql.connector.connect(host='localhost',
                                                database='jg_teste',
                                                user='root',
                                                password='user')

            if connection.is_connected():

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
                        else:
                            lista.append(each.value)
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
                sqlqueryprof = acrescentar_id(sqlquery, sqlquerykey)
                print(sqlqueryprof)
                sqlqueryalun = acrescentar_id(sqlqueryal, sqlquerykey)
                print(sqlqueryalun)

                # EXECUTAR CADA UMA DAS QUERYS
                cursor_prof = connection.cursor()
                cursor_prof.execute(sqlqueryprof)
                records_prof = cursor_prof.fetchall()

                data_frame_prof = pandas.DataFrame(records_prof)
                # print(data_frame_prof)

                cursor_alun = connection.cursor()
                cursor_alun.execute(sqlqueryalun)
                records_query = cursor_alun.fetchall()

                data_frame_alun = pandas.DataFrame(records_prof)
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
                colunas_totais = compare.df1_unq_columns()
                colunas_iguais = len(compare.intersect_columns())
                
                table_prof = extract_tables(sqlqueryprof)
                table_alun = extract_tables(sqlqueryalun)

                campos_totais = len(table_prof)
                campos_iguais = len(table_prof & table_alun)

                query = "INSERT INTO RESULTADO (numero_linhas_totais, numero_linhas_iguais, colunas_totais,colunas_iguais,campos_totais,campos_iguais, pergunta_pergunta_id) VALUES (%s,%s,%s,%s,%s,%s,%s);"
                cur.execute(query,(numero_linhas_totais,numero_linhas_iguais,colunas_totais,colunas_iguais,campos_totais,campos_iguais,pergunta_id))


                # print(data_frame_prof)
        except Error as e:
            print("Error while connecting to MySQL", e)
            print()
        finally:
            # closing database connection.
            if(connection.is_connected()):
                cursor_prof.close()
                cursor_alun.close()
                connection.close()
                print("MySQL connection is closed")

        
        # SELECIONA A RESPOSTA ANTERIORMENTE INSERIDA
        query = "SELECT RESPOSTA_SQL FROM RESPOSTA WHERE RESPOSTA_ID = %s;"
        cur.execute(query, (last_inserted_id,))
        data = cur.fetchall()
        cur.close()
        js = json.dumps(data)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp
    elif request.method == 'POST' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp


@app.route('/respostas/<int:resposta_id>', methods=['PUT', 'DELETE', 'GET'])
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
            js = json.dumps(data)
            return Response(js, status=200, mimetype='application/json')
    elif request.method == 'PUT' and form.validate():
        resposta_sql = request.form["resposta_sql"]
        cur = mysql.connection.cursor()
        query = "UPDATE aluno SET RESPOSTA_SQL=%s WHERE RESPOSTA_ID = %s"
        cur.execute(query, (resposta_sql,))
        mysql.connection.commit()
        cur.execute(
            "SELECT RESPOSTA_SQL FROM PERGUNTA WHERE RESPOSTA_ID = %s", (resposta_id,))
        data = cur.fetchall()
        cur.close()
        js = json.dumps(data)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp
    elif request.method == 'PUT' and not form.validate():
        resp = Response(status=400)
        resp.headers['Links'] = 'http://127.0.0.1/respostas/'
        return resp
    elif request.method == 'DELETE':
        query = "DELETE FROM RESPOSTA WHERE RESPOSTA_ID = %s"
        cur = mysql.connection.cursor()
        cur.execute(query, (resposta_id,))
        mysql.connection.commit()
        cur.fetchall()
        return Response(status=200)

@app.route('/perguntas/', methods=['POST', 'GET'])
def perguntas():
    form = PerguntaForm(request.form)
    if request.method == 'GET':
        cur = mysql.connection.cursor()
        cur.execute("SELECT PERGUNTA,PERGUNTA_SQL FROM PERGUNTA;")
        data = cur.fetchall()
        cur.close()
        return render_template('/perguntas/perguntas.html', perguntas=data)
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


@app.route('/perguntas/<int:pergunta_id>', methods=['PUT', 'DELETE', 'GET'])
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


@app.route('/about')
def about():
    return render_template('about.html')


@app.errorhandler(404)
def page_not_found(e):
    return 'This page was not found'


if __name__ == '__main__':
    app.run(debug=True, port=80)
