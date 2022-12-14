import json
import os
import re

import pandas as pd
import requests
import telebot
from flask import Flask, request

# constans
token = '5829774627:AAEWpqEGlQ1OOoOAfE3_AiXnCLSQcZexq1k'


bot = telebot.TeleBot(token)
server = Flask(__name__)

def load_dataset(store_id):
    # loading test dataset
    df10 = pd.read_csv('test.csv')
    df_store_raw = pd.read_csv('store.csv')

    # merge test dataset + store
    df_test = pd.merge(df10, df_store_raw, how='left', on = 'Store')

    # choose store for prediction
    df_test = df_test[df_test['Store'] == store_id]

    if not df_test.empty:
        # remove closed days
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop('Id', axis = 1)

        # convert dataframe to json
        data = json.dumps(df_test.to_dict(orient='records'))

    else:
        data = 'error'

    return data

def predict(data):
    # API call
    url = 'https://teste-deploy-render-bc3n.onrender.com/rossmann/predict'

    header = {'Content-type': 'application/json'} # indica qual tipo de dado esta recebendo
    data = data

    r = requests.post( url, data = data, headers = header)
    # metodo POST serve para enviar o dado

    print('Status code {}'.format(r.status_code))
    # para indicar se a request é válida
    # retorno de 200 significa que está tudo okay

    d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())

    return d1


@bot.message_handler(regexp='[0-9]+')
def responder_2(message):

    store_id = re.findall('[0-9]+',message.text)

    store_id = int(store_id[0])

    #loading data
    data = load_dataset(store_id)

    if data != 'error':
        #prediction
        d1 = predict(data)
        # calcution
        d2 = d1[['store', 'prediction']].groupby('store').sum().reset_index()

        # send message
        msg = 'ID Loja {} irá vender R${:,.2f} nas próximas 6 semanas.'.format(
            d2['store'].values[0], d2['prediction'].values[0] )

        bot.send_message(message.chat.id, msg)

        text = """
        Digite outro ID Loja, caso queira outra previsão de vendas.

Para voltar, digite Sair."""

        bot.send_message(message.chat.id, text)

    else:
        bot.send_message(message.chat.id, 'Loja não disponível. Favor, digite outro Loja ID.')

    # bot.reply_to(message, store_id)

def verificar(message):
    return True

@bot.message_handler(func=verificar)
def responder(message):
    text = """
    Olá, Bem vindo ao ROSSMANN BOT.
Aqui é possivel obter as previsões de vendas nas próximas 6 semanas.

Favor, digite o número da loja para obter a previsão de vendas."""

    bot.reply_to(message, text)

@server.route('/' + token, methods=['POST'])
def getMessage():
    bot.process_new_updates([telebot.types.Update.de_json(request.stream.read().decode("utf-8"))])
    return "!", 200


@server.route("/")
def webhook():
    bot.remove_webhook()
    return "!", 200


if __name__ == "__main__":
    server.run(host="0.0.0.0", port=int(os.environ.get('PORT', 5000)))

