# apaga arquivos com o tamanho = 502 e arquivos com a mesma data e tamanho
# /home/marcio/retornoCielo/20210223_CIELO04_0001235141_2790207
#                           ---------------------------
import os # Marcio Quaresma dez/2021
path = '/home/marcioquaresma/retornoCielo/'
files = os.listdir(path)
arquivos = sorted(files)
x = 0
tamanho = 0
tamanhoAnt = 0
dtArq = 'x'
dtArqAnt = 'x'
arqAnt = 'x'
print('apaga - by Marcio Quaresma - dez/2021')
print('apaga arquivos retorno Cielo vizios e arquivos com a mesma data e tamanho')
for f in arquivos:
    f_path = os.path.join(path,f)
    dtArq=f[0:8]
    if(os.path.isfile(f_path)): # se arquivo com caminho completo existe
        tamanho = os.path.getsize(f_path)
        # print(f, dtArq, tamanho)
        if dtArq==dtArqAnt and tamanho==tamanhoAnt:
            os.remove(f_path)
            x += 1
            print(arqAnt, '=', f, '--> removido')
        arqAnt = f
        dtArqAnt = dtArq
        tamanhoAnt = tamanho
        if tamanho==502: # 502 é tamanho de arquivo vazio
            print(tamanho, f_path, '--> removido')
            os.remove(f_path)
            x += 1
if x>0:
      print(x,'arquivos apagados')
else:
      print('nada a ser apagado')

