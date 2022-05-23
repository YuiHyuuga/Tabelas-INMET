#!/usr/bin/env python
import pandas as pd
from datetime import datetime
import urllib3
import json
import requests
import numpy as np
from pathlib import Path
import math

http = urllib3.PoolManager()


def getToken(host, user, password):
    url = 'http://{}/api/auth/login'.format(host)
    values = {}
    values['username'] = user
    values['password'] = password
    req = requests.post(url, json=values)
    jsonmessage = json.loads(req.content)
    authorization = jsonmessage["token"]
    print(req.status_code)

    return 'Bearer '+authorization


def insertDevice(host,content, token):
    header = {}
    header['X-Authorization'] = token
    url = 'http://{}/api/device'.format(host)

    command = requests.post(url, json=content, headers=header)
    print(command.status_code)
    print(command.content)


def getCredentials(host, token, deviceid):
    header = {}
    header['X-Authorization'] = token
    url = "http://{}/api/device/{}/credentials".format(host, deviceid)
    command = requests.get(url, headers=header)
    print(command.status_code)
    print(command.content)
    jsonmessage = json.loads(command.content)

    return jsonmessage['credentialsId']


def insertdeviceAttributes(host, token, devicecredential, content):
    header = {}
    header['X-Authorization'] = token
    url = "http://{}/api/v1/{}/attributes".format(host, devicecredential)
    command = requests.post(url, json=content, headers=header)
    print(command.status_code)
    print(command.content)


def data_para_timestamp(data):
    d = datetime.strptime(data, '%Y-%m-%d %H:%M:%S')
    mili = d.timestamp()*1000  # x1000 para timestamp em milissegundos
    return mili


def getAlldevices(host, limit, token):
   header = {}
   header['X-Authorization'] = token
   url = 'http://{}/api/tenant/devices?limit={}'.format(host,limit)
   command = requests.get(url, headers=header)
   print(command.status_code)
   print('x')
   print(command.content)
   jsonmessage = json.loads(command.content)
   device_list = jsonmessage['data']
   return device_list


def getDeviceByName(nome, host, limit, token):
   lista_de_devices = getAlldevices(host, limit, token)
   for i in lista_de_devices:
       if i['name'] == nome:
             return i


def getDeviceUUID(nome,host,limit,token):
    return (getDeviceByName(nome,host,limit,token)['id']['id'])


def Telemetry(host, token, devicecredential, content):
    header = {}
    header['X-Authorization'] = token
    url = "http://{}/api/v1/{}/telemetry".format(host, devicecredential)
    command = requests.post(url, json=content, headers=header)
    print(command.status_code)
    print(command.content)



atributos = {}
device = {}
limite = 10
arquivos = []
p = Path('/home/joana/PlanilhasINMET').glob('**/*')
files = [x for x in p if x.is_file()]
for i in files:
    arquivos.append(i)

arquivos.sort(reverse=True)

site = 'localhost:8080'
usuario = 'joanaszborges@gmail.com'
passe = 'kurama24'



sensors = ['pressao_inst', 'vento_vel', 'precipitacao', 'rad_global', 'vento_dir', 'vento_rajmax', 'pressao_atmax', 'pressao_atmin', 'temp_inst',
        'umi_rel', 'temp_po', 'temp_max', 'temp_min', 'temp_maxpo', 'temp_minpo', 'umi_relmax', 'umi_relmin']


for a in range(len(arquivos)):
    token = getToken(site, usuario, passe)

    endereco = arquivos[a]
    tabela = pd.read_csv(endereco).as_matrix()

    codigo = tabela[1][0]

    verificar = getDeviceByName(codigo, site, limite, token)
    verificar = str(verificar)

    col = np.ma.size(tabela, 1)  # qtd de colunas da tabela
    lin = np.ma.size(tabela, 0)  # qtd de linhas da tabela

    if codigo in verificar:

        id = getDeviceUUID(codigo, site, limite, token)
        cred = getCredentials(site, token, id)

        for i in range(lin):
            token = getToken(site, usuario, passe)
            telemetria = {}

            data = tabela[i][6]
            timestamp = data_para_timestamp(data)
            timestamp = int(timestamp)
            dados = {}

            for s in range(17):
                temp = sensors[s]
                temp2 = tabela[i][s+8]

                if temp2 != ' ' or temp2 == '':
                    temp2 = float(temp2)
                print(temp2)


                if not (math.isnan(temp2) or temp2 == ' '):
                    telemetria[temp] = tabela[i][s+8]

            dados = {'ts': timestamp, 'values': telemetria}
            Telemetry(site, token, cred, dados)
            print("Fim Telemetria")

        print("Fim Tabela")



    else:

        device['name'] = codigo
        device['type'] = 'INMET_Automatic_Station'

        atributos['estado'] = tabela[1][1]
        atributos['cidade'] = tabela[1][2]
        atributos['altitude'] = tabela[1][3]
        atributos['lat'] = tabela[1][4]
        atributos['long'] = tabela[1][5]

        insertDevice(site, device, token)
        limite+=1

        id = getDeviceUUID(codigo, site, limite, token)
        cred = getCredentials(site, token, id)

        insertdeviceAttributes(site, token, cred, atributos)


        for i in range(lin):
            telemetria = {}
            token = getToken(site, usuario, passe)

            data = tabela[i][6]
            timestamp = data_para_timestamp(data)
            timestamp = int(timestamp)

            dados = {}

            for s in range(17):
                temp = sensors[s]
                temp2 = tabela[i][s + 8]

                if temp2 != ' ' or temp2 == '':
                    temp2 = float(temp2)

                print(temp2)

                if not (math.isnan(temp2) or temp2 == ' '):
                   telemetria[temp] = tabela[i][s+8]


            dados = {'ts': timestamp, 'values': telemetria}
            Telemetry(site, token, cred, dados)
            print("Fim Telemetria")

        print("Fim tabela")
