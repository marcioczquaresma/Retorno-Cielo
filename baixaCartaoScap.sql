create or replace PROCEDURE                         BAIXACARTAOSCAP AS 
BEGIN ----> baixaCartaoScap <----- Marcio Quaresma - dez2021 -----> PRODUÇÃO - terceiro
For i in (
--ligação entre RETORNO_CIELO x SCAP x RM
select unique seq, estabele, dtCompen,r.dtCompensa,r.tpPago,r.tpAtiva, dtVenda, parc, plan, nuCartao, rt.nuDOCUMENTO, codAuto, nsuDoc, vlrBruto, vlrTaxa, vlrLiq, r.nuReceber
from SITEF.RETORNO_CIELO rt
left join scap.receber r on rt.nuDOCUMENTO = r.nuDocumento and r.nuSufixo=to_number(rt.parc) 
where sinal = '+' and rt.idlan is null and r.tpAtiva='S' and r.nuPortador in ( 2,12,13,15,20,21,23,24,25,27,28,29,30,31,42 )
    and r.dtCompensa is null and rt.dtCompen >= '20210101' 
order by rt.dtCompen, rt.seq, r.nuReceber) LOOP
    if i.dtCompensa is null then 
        update scap.receber set 
            tpPago='S', 
            vlPago=i.vlrBruto, 
            vlTaxa=i.vlrTaxa, 
            vlACRESCIMO=i.vlrTaxa,
            tpAtiva=i.tpAtiva,
            vlDESCONTO=0, 
            dtCompensa=to_date(i.dtCompen,'YYYYMMDD')
        where nuReceber=i.nuReceber and dtCompensa is null;
        commit;
    end if;
END LOOP;
END BAIXACARTAOSCAP;

