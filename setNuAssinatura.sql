create or replace PROCEDURE                                                                   SETNUASSINATURA AS 
BEGIN ----> setNuAssinatura <----- Marcio Quaresma - dez2021 - segundo
For i in (
----> setNuAssinatura <-----
select unique seq,dtCompen,rt.dtVenda,rt.parc,rt.codAuto,rt.vlrBruto, rt.nuCliente,rt.nuClienteCartao,rt.nuDOCUMENTO,
  a.nuASSINATURA, r.nuRECEBER, r.tpAtiva
  from SITEF.RETORNO_CIELO rt 
  left join scap.assinatura a on rt.nuCliente = a.nuCliente and a.dtCancelado is null and a.dtFim>to_date(rt.dtCompen,'YYYYMMDD')
  left join scap.receber r on rt.nuDOCUMENTO = r.nuDocumento and r.nuSufixo=to_number(rt.parc) 
  where sinal='+' and rt.idLAN is null and rt.nuCliente is not null and rt.nuDOCUMENTO is null 
    and r.nuPortador in ( 2,12,13,15,20,21,23,24,25,27,28,29,30,31,42 )
  order by rt.dtCompen, rt.seq
) LOOP
    if i.nuASSINATURA is not null then
        if i.nuRECEBER is not null then
            update SITEF.RETORNO_CIELO set 
                nuDocumento=i.nuASSINATURA,
                tpAtiva=i.tpAtiva,
                nuRECEBER=i.nuRECEBER
            where dtCompen=i.dtCompen and seq=i.seq;
            commit;
        else
            update SITEF.RETORNO_CIELO set 
                nuDocumento=i.nuASSINATURA
            where dtCompen=i.dtCompen and seq=i.seq;
            commit;
        end if;
    end if;
END LOOP;
END SETNUASSINATURA;

