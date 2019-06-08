from Flask import Blueprint
from wtforms import *


utils = Blueprint('forms', __name__)

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