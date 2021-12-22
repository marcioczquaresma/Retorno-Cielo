create or replace PROCEDURE                                     SETBAIXADOS AS 
BEGIN ----> setBaixados <----- Marcio Quaresma - dez2021 - quarto
For i in (
----> setBaixados <-----
select seq, estabele, dtCompen, dtVenda, parc, plan, nuCartao, r.nuDocumento,r.tpAtiva, codAuto, nsuDoc, vlrBruto, vlrLiq, numRO, nuUnico, terminal,
    c.nmEmpresa,c.nuCliente,c.vlCartaoTRN, r.tpPago,r.dtCompensa,r.nuReceber, to_number(ce.nmcodExterno) idlan
from SITEF.RETORNO_CIELO rt
left join scap.cartaoTRN c on  substr(c.nuDocumento,4,6)=rt.nsuDoc and c.nmAutorizacao=rt.codAuto
left join scap.receber r on rt.nuDocumento = r.nuDocumento
left join scap.codigoExterno ce on ce.nuReferencia = r.nuReceber and ce.nmreferencia='RECEBER'
where sinal = '+' and rt.dtCompen >= '20210101' and rt.idlan is null and r.nuSufixo=to_number(parc) and r.tpAtiva='S' 
    and r.nuPortador in ( 2,12,13,15,20,21,23,24,25,27,28,29,30,31,42 ) and r.dtCompensa is not null
order by rt.dtCompen, rt.seq, r.nuReceber
) LOOP
        update SITEF.RETORNO_CIELO set 
            idLAN=i.idLAN,
            nuDocumento=i.nuDocumento,
            nuRECEBER=i.nuReceber,
            tpAtiva=i.tpAtiva
        where dtCompen=i.dtCompen and seq=i.seq;
        commit;
END LOOP;
END SETBAIXADOS;

