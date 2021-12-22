# Marcio Quaresma - nov/2021
# Retorno Cielo v.2
# processa todos os arquivos retorno cielo de um diretorio

import os
import pandas as pd
from datetime import datetime
import cx_Oracle

log = open('log'+datetime.today().strftime('%Y-%m-%d_%H-%M')+'.txt','w')
def gravaLog(txt):
    log.write(datetime.today().strftime('%H:%M:%S')+' '+txt+'\n')

gravaLog('***** Inicio do processo *****')
gravaLog('log retornoCielo - by Marcio Quaresma - dez/2021')

if os.path.isfile('arquivos.ret'):
    nomes = ['arqRet'] 
    colun = [(0, 50)] 
    arqRet = pd.read_fwf('arquivos.ret', skiprows=0, header=None, names=nomes, colspecs=colun, converters={x:str for x in nomes})
    gravaLog('arquivos.ret encontrado - sera processado somente os arquivos nele não encontrados')
else:
    arqRet = pd.DataFrame({'arqRet':['arquivos.ret nao encontrado']})
    gravaLog('arquivos.ret nao encontrado - processo sera demorado')


def vlr(num):
    if pd.isna(num):
        num = 0
    strNum = str(num)
    p=strNum.find('.')
    t = len(strNum)
    if p==-1:
        ap=strNum
        dp='00'
    else:
        ap=strNum[0:p]
        dp=strNum[p+1:t]
    if len(dp)==1:
        dp+='0'
    #print (ap,dp,p,t)
    strNum=ap+','+dp
    t=len(strNum)
    if t==3:
        strNum='     _'+strNum
    if t==4:
        strNum='    _'+strNum
    if t==5:
        strNum='   _'+strNum
    if t==6:
        strNum='  _'+strNum
    if t==7:
        strNum=' _'+strNum
    if t==8:
        strNum='_'+strNum
    return strNum
    
def arqProcessado(x): # verifica se arquivo ja foi processado
    for i in range(len(arqRet)):
        if arqRet.arqRet[i]==x:
            return True
    return False

# formata numero de Cartao com mascara de ******
def card(x):
    xi=x.find('******')-6
    xf=xi+16
    return x[xi:xf]

# Verifica se dado já foi migrado
def migrado(dtCompensa,autori,sin):
    sql = "select dtCompen from SITEF.RETORNO_CIELO where dtCompen='"+dtCompensa+"' and codAuto='"+autori+"' and sinal='"+sin+"'"
    c2.execute(sql)
    r2 = c2.fetchone()
    if r2:
        return True
    else:
        return False
    
path = '/home/marcioquaresma/retornoCielo/' # produção
# path = '/home/marcio/retornoCielo/' # teste
for arq in os.listdir(path):
    if arqProcessado(arq):
        # print('*',arq,' processado anteriormente')
        # log.write(datetime.today().strftime('%H:%M:%S')+' '+arq + ' processado anteriormente \n')
        continue # ----pula esse----
    # pega arquivo
    #                -0-        -9-          -1-      -0-        -1-       -1-      -1-      -1-      -1-         -1-        -1-       -1-       -1-       -1-       -1-        -1-       -2-         -2-      -2-        -2-       -2-
    nomes = ['tp',  'estabele','registros', 'numRO', 'dtCompen','parc',   'plan',  'dtPag', 'sinal', 'vlrBruto', 'vlrTaxa', 'vlrLiq', 'banco',  'agencia','conta',  'bandeira','nuUnico','nuCartao','dtVenda','codAuto', 'nsuDoc', 'terminal'] 
    colun = [(0, 1),(1, 11),    (1, 12),     (11, 18),(11, 19),  (18, 20), (21, 23),(31,37), (43, 44),(45, 57),   (63, 71),  (87, 99), (100,103),(104,108),(112,121),(184,187), (188,202),(18, 37),  (37, 45), (66, 72),  (92, 98), (152, 160)] 
    ret   = pd.read_fwf(path+arq, skiprows=0, header=None, names=nomes, colspecs=colun, converters={x:str for x in nomes})

    # Migra Dados para Banco
    # Conecta Banco Oracle
    conecta = pd.read_csv('conecta.txt')
    con = cx_Oracle.connect(
        user=conecta.usuario[0], 
        password=conecta.senha[0], 
        dsn=conecta.ip[0], 
        encoding="UTF-8")
    c1 = con.cursor() # cria Cursor 1
    c2 = con.cursor() # cria Cursor 2
    # migra dados
    lin=0
    # for i in tqdm(range(len(ret))): # barra de progresso
    for i in range(len(ret)):
        if ret.tp[i]=='0':
            dtCompen=str(ret.dtCompen[i])
            gravaLog('processando ' + arq)
            continue
        if ret.tp[i]=='1':
            sinal=ret.sinal[i]
            vlrBruto=float(ret.vlrBruto[i])/100
            vlrTaxa=float(ret.vlrTaxa[i])/100
            vlrLiq=float(ret.vlrLiq[i])/100
            bandeira=ret.bandeira[i]
            banco=ret.banco[i]
            agencia=ret.agencia[i]
            conta=ret.conta[i]
            estabele=ret.estabele[i]
            nuUnico=ret.nuUnico[i]
            numRO = ret.numRO[i]
            dtPag = ret.dtPag[i]
            if pd.isna(ret.parc[i]):
                parc='00'
            else:
                parc=ret.parc[i]
            if pd.isna(ret.plan[i]):
                plan='00'
            else:
                plan=ret.plan[i]
            continue

        if ret.tp[i]=='2':
            lin += 1
            seq=str(lin).zfill(3)
            codAuto=(ret.codAuto[i])
            nsuDoc=(ret.nsuDoc[i])
            dtVenda=ret.dtVenda[i]
            terminal=(ret.terminal[i])

            if pd.isna(ret.nuCartao[i]):
                nuCartao='taxa_servico'
                codAuto=seq
            else:
                nuCartao=card(ret.nuCartao[i])
            if migrado(dtCompen,codAuto,sinal):
                gravaLog(arq +' '+ dtCompen +' '+ codAuto + ' processado anteriormente *')
                continue # ----pula esse----

            insSql='insert into SITEF.RETORNO_CIELO( dtCompen,seq,estabele,codAuto,sinal,vlrBruto,vlrTaxa,vlrLiq,nuCartao,parc,plan,numRO,dtPag,dtVenda,banco,agencia,conta,bandeira,nuUnico,nsuDoc,terminal'
            insSql+=') values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21 )'
            val = ( dtCompen, seq, estabele, codAuto, sinal, vlrBruto, vlrTaxa, vlrLiq, nuCartao, parc, plan, numRO, dtPag, dtVenda, banco, agencia, conta, bandeira, nuUnico, nsuDoc, terminal )
            c1.execute( insSql, val )
            con.commit()
            log.write(datetime.today().strftime('%H:%M:%S')+' '+'insert ' + dtCompen +' '+ seq +' '+ estabele +' '+ codAuto +' '+ sinal +' '+ vlr(vlrBruto) + '\n')
            continue
        if ret.tp[i]=='9':
            linhas=int(ret.registros[i])
            if linhas==0:
                gravaLog(arq + ' vazio')
            else:
                gravaLog(arq+' processado '+str(linhas).zfill(3)+' linhas')

    # Desconecta servidor
    c1.close()
    c2.close()
    con.close()
gravaLog('***** Varredura retornoCielo concluido *****')
# Salva arquivos.ret
# path = '/home/marcio/retornoCielo/'
arquivo = open('arquivos.ret','w')
for arq in os.listdir(path):
    arquivo.write(arq+'\n')
arquivo.close()
gravaLog('arquivos.ret salvo')

con = cx_Oracle.connect(
    user=conecta.usuario[0], 
    password=conecta.senha[0], 
    dsn=conecta.ip[0], 
    encoding="UTF-8")
c1 = con.cursor() # cria Cursor 1

gravaLog('executando SITEF.SetNuCLIENTE()')
c1.execute('BEGIN SITEF.SetNuCLIENTE(); END;')
gravaLog('SITEF.SetNuCLIENTE() concluido')

gravaLog('executando SITEF.SetNuASSINATURA()')
c1.execute('BEGIN SITEF.SetNuASSINATURA(); END;')
gravaLog('SITEF.SetNuASSINATURA() concluido')

gravaLog('executando SITEF.BaixaCartaoScap()')
c1.execute('BEGIN SITEF.BaixaCartaoScap(); END;')
gravaLog('SITEF.BaixaCartaoScap() concluido')

gravaLog('executando SITEF.SetBaixados()')
c1.execute('BEGIN SITEF.SetBaixados(); END;')
gravaLog('SITEF.SetBaixados() concluido')
c1.close()
con.close()
# print('* Processo concluido')
gravaLog('***** Processo concluido *****')
log.close()

