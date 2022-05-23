import pandas as pd
from datetime import datetime
import numpy as np
from pathlib import Path

meses={
    'JAN': '01',
    'FEV': '02',
    'MAR': '03',
    'ABR': '04',
    'MAI': '05',
    'JUN': '06',
    'JUL': '07',
    'AGO': '08',
    'SET': '09',
    'OUT': '10',
    'NOV': '11',
    'DEZ': '12'
}

def converteLat(lat):
    N = 'N' in lat
    d = lat[0]
    bol1 = True
    if lat[1] != "°":
        bol1 = False
        d += lat[1]
    d = float(d)
    if bol1:
        m = lat[2]
        m += lat[3]
    else:
        m = lat[3]
        m += lat[4]
    m = float(m)
    latitude = (d + m / 60.) * (1 if N else -1)
    return latitude

def converteLong(long):
    W = 'W' in long
    d = long[0]
    d += long[1]
    d = float(d)
    m = long[3]
    m += long[4]
    m = float(m)
    longitude = (d + m / 60.) * (-1 if W else 1)
    return longitude

def tratandolista(nova):
    for i in nova:
        rep = str(i)
        if "(1)" not in rep:
            lista.append(rep)
    return lista

def nometabela(caminho):
    bar = 0
    contador = 0
    nome = ""
    for j in caminho:
        if j == "/":
            bar += 1
        contador += 1
        if bar >= 8:
            nome = caminho[contador:]
            break
    return nome

def ano(diretorio):
    barra = 0
    con = 0
    ano = ""
    anos = "  "
    for a in diretorio:
        if a =="/":
            barra+=1
        con+=1
        if barra>=5:
            ano = diretorio[con:]
            break
    for z in ano:
        if z =="/":
            break
        else:
            anos+=z
    return anos

nova =[]
p = Path('/home/joana/Downloads/DADOS ESTAÇÕES AUTOMÁTICAS INMET').glob('**/*')
files = [x for x in p if x.is_file()]

for i in files:
    nova.append(i)

nova.sort(reverse=True)

lista =[]
rep = ""

lista = tratandolista(nova)

for a in range(0, len(lista),2):
    try:
        planilha1 =(lista[a])
        planilha2 = (lista[a+1])

        planilha_mestre = pd.read_excel(planilha1).as_matrix()
        subordinada = pd.read_excel(planilha2).as_matrix()
        planilha_mestre = np.delete(planilha_mestre, np.s_[183:], axis=1)  # apagando colunas nan específicas nas tabelas
        subordinada = np.delete(subordinada, np.s_[217:], axis=1)
        subordinada = np.delete(subordinada, np.s_[0], axis=1)
        arquivo = []

        final = np.concatenate((planilha_mestre, subordinada), axis=1) #união das tabelas
        z = 8
        codigo = ""
        cidade = ""

        tabela = nometabela(planilha1)
        tabela = tabela.replace("_", " ")
        tabela = tabela.replace(".xls", " ")
        tabela = tabela.strip()
        tabela+=ano(planilha1)

        estado = tabela[0]
        estado +=tabela[1]
        for k in range(3,7):
            codigo+=tabela[k]

        while True:
            if tabela[z] == "2":
                break
            else:
                cidade+= tabela[z]
                z+=1
        col = np.ma.size(final, 1)  # qtd de colunas da matriz
        lin = np.ma.size(final, 0)  # qtd de linhas da matriz
        lista1 = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
        lista2 = [1,2,3,4,5,6,7,8,9,10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
        rad = []
        atm = []
        if "2000" in tabela and codigo =="A804":
            dado = final[11][0]
            dado = str(dado)
            dado = dado[7:]
            dado = int(dado)
        else:
            dado = final[11][0]
            dado = str(dado)
            dado = dado[:4]
            dado = int(dado)

        if codigo == "A804" and dado>=2000 and dado <=2014:
            for a in range(10, lin):
                for b in range(96, 110):
                    rad.append(final[a][b])

            for a in range(10, lin):
                for b in range(73, 96):
                    atm.append(final[a][b])

        else:
            for a in range(10, lin):
                for b in range(73, 87):
                    rad.append(final[a][b])

        g = 0
        y = 0
        altura = final[4][1].strip("m")
        lats = final[5][1]
        longs = final[6][1]


        latitude = converteLat(lats)
        longitude = converteLong(longs)

        if dado >= 2000 and dado <= 2014:  # código para reorganização das tabelas 2000-2014, o método usado para diferenciar as versões é identificando o ano de cada tabela
            for i in range(10, lin):
                for j in range(1, 25):
                    if codigo == "A804": # essa peça de código é para uma tabela específica que se diferencia das demais versões de tabelas
                        mes = ""
                        data = ""
                        comp = ""
                        aaa = str(final[i][0])
                        comp += aaa[3]
                        comp += aaa[4]
                        comp += aaa[5]
                        aaa = aaa.upper()
                        mes += aaa[3]
                        mes += aaa[4]
                        mes += aaa[5]
                        if mes.lower() != comp:
                            mesagora = meses.get(mes)
                            print(mesagora)
                            data += aaa[0]
                            data += aaa[1]
                            data += aaa[2]
                            data += mesagora[0]
                            data += mesagora[1]
                            data += aaa[6]
                            data += aaa[7]
                            data += aaa[8]
                            data += aaa[9]
                            data += aaa[10]
                            print(data)
                            novahora = datetime.strptime(data,'%d-%m-%Y')
                            final[i][0] = novahora

                        if j in lista2 and j in lista1:
                            arquivo.append(
                                [codigo,estado,cidade,altura, latitude, longitude, final[i][0].replace(hour=j - 1), j - 1, final[i][j + 24],
                                 final[i][j + 133],
                                 final[i][j], rad[g], final[i][j + 157], final[i][j + 109], final[i][j + 48],
                                 atm[y], final[i][j + 181], final[i][j + 373], final[i][j + 205],
                                 final[i][j + 229], final[i][j + 253], final[i][j + 277], final[i][j + 301],
                                 final[i][j + 325], final[i][j + 349]])

                            g += 1  # lista1
                            y += 1  # lista2

                        elif j in lista1:
                            arquivo.append(
                                [codigo,estado,cidade,altura, latitude, longitude, final[i][0].replace(hour=j - 1), j - 1, final[i][j + 24],
                                 final[i][j + 133],
                                 final[i][j], rad[g], final[i][j + 157], final[i][j + 109], final[i][j + 48],
                                 " ", final[i][j + 181], final[i][j + 373], final[i][j + 205],
                                 final[i][j + 229], final[i][j + 253], final[i][j + 277], final[i][j + 301],
                                 final[i][j + 325], final[i][j + 349]])

                            g += 1

                        elif j in lista2:
                            arquivo.append(
                                [codigo,estado,cidade,altura, latitude, longitude, final[i][0].replace(hour=j - 1), j - 1, final[i][j + 24],
                                 final[i][j + 133], final[i][j], " ", final[i][j + 157], final[i][j + 109],
                                 final[i][j + 48],
                                 atm[y], final[i][j + 181], final[i][j + 373], final[i][j + 205],
                                 final[i][j + 229], final[i][j + 253], final[i][j + 277], final[i][j + 301],
                                 final[i][j + 325], final[i][j + 349]])

                            y += 1

                        else:
                            arquivo.append(
                                [codigo,estado,cidade,altura, latitude, longitude, final[i][0].replace(hour=j - 1), j - 1, final[i][j + 24],
                                 final[i][j + 133],
                                 final[i][j], " ", final[i][j + 157], final[i][j + 109], final[i][j + 48],
                                 " ", final[i][j + 181], final[i][j + 373], final[i][j + 205],
                                 final[i][j + 229], final[i][j + 253], final[i][j + 277], final[i][j + 301],
                                 final[i][j + 325], final[i][j + 349]])
                        # fim da peça de código para a tabela específica

                    else:
                         if j in lista1:
                             arquivo.append(
                                [codigo,estado,cidade,altura, latitude, longitude, final[i][0].replace(hour=j - 1), j - 1, final[i][j], final[i][j + 24],
                                 final[i][j + 86], rad[g], final[i][j + 48], final[i][j + 110], final[i][j + 134],
                                 final[i][j + 158], final[i][j + 182], final[i][j + 206], final[i][j + 230],
                                 final[i][j + 254], final[i][j + 278], final[i][j + 302], final[i][j + 326],
                                 final[i][j + 350], final[i][j + 374]])
                             g += 1

                         else:
                             arquivo.append(
                                [codigo,estado,cidade,altura, latitude, longitude, final[i][0].replace(hour=j - 1), j - 1, final[i][j], final[i][j + 24],
                                 final[i][j + 86], " ", final[i][j + 48], final[i][j + 110], final[i][j + 134],
                                 final[i][j + 158], final[i][j + 182], final[i][j + 206], final[i][j + 230],
                                 final[i][j + 254], final[i][j + 278], final[i][j + 302], final[i][j + 326],
                                 final[i][j + 350], final[i][j + 374]])




        else:  # código para reorganização das tabelas 2015-2016 e 2017
            for i in range(10, lin):
                for j in range(1, 25):


                    if j in lista1:
                            arquivo.append(
                                [codigo,estado,cidade,altura, latitude, longitude, final[i][0].replace(hour=j - 1), j - 1, final[i][j], final[i][j + 24],
                                 final[i][j + 48], rad[g], final[i][j + 86], final[i][j + 110], final[i][j + 134],
                                 final[i][j + 158], final[i][j + 182], final[i][j + 206], final[i][j + 230],
                                 final[i][j + 254], final[i][j + 278], final[i][j + 302], final[i][j + 326],
                                 final[i][j + 350], final[i][j + 374]])
                            g += 1

                    else:

                            arquivo.append(
                                [codigo,estado,cidade,altura, latitude, longitude, final[i][0].replace(hour=j - 1), j - 1, final[i][j], final[i][j + 24],
                                 final[i][j + 48], " ", final[i][j + 86], final[i][j + 110], final[i][j + 134],
                                 final[i][j + 158], final[i][j + 182], final[i][j + 206], final[i][j + 230],
                                 final[i][j + 254], final[i][j + 278], final[i][j + 302], final[i][j + 326],
                                 final[i][j + 350], final[i][j + 374]])

        # Fim da peça de código para reorganização


        # Exportação das tabelas:
        novo = pd.DataFrame(arquivo)  # convertendo a matriz em dataframe para exportação
        novo.to_csv(tabela, index=False, sep=',',
                    header=['CÓDIGO DA ESTAÇÃO','ESTADO','CIDADE','ALTITUDE (m)', 'LATITUDE', 'LONGITUDE', 'DATA', 'HORA', 'PRESSÃO ATMOSFÉRICA (hPa)',
                            'VENTO VELOCIDADE (m/s)', 'PRECIPITAÇÃO (mm)', 'RADIAÇÃO GLOBAL (KJ/M²)',
                            'VENTO, DIREÇÃO (graus)', 'VENTO, RAJADA MÁXIMA (m/s)', 'PRESSÃO ATMOSFÉRICA MÁXIMA (hPa)',
                            'PRESSÃO ATMOSFÉRICA MÍNIMA (hPa)', 'TEMPERATURA DO AR (°C)', 'UMIDADE RELATIVA DO AR (%)',
                            'TEMPERATURA DO PONTO DE ORVALHO (°C)', 'TEMPERATURA MÁXIMA (°C)',
                            'TEMPERATURA MÍNIMA (°C)', 'TEMPERATURA MÁXIMA DO PONTO DE ORVALHO (°C)',
                            'TEMPERATURA MÍNIMA DO PONTO DE ORVALHO (°C)', 'UMIDADE RELATIVA MÁXIMA DO AR (%)',
                            'UMIDADE RELATIVA MÍNIMA DO AR (%)'])



        #alguns testes
        print(tabela)
        print(" ")
        print(final)
        print(" ")
    except IndexError:
        continue