create or replace PROCEDURE                         SETNUCLIENTE AS 
BEGIN -- setNuCliente - Marcio Quaresma - dez2021 - primeiro
For i in (
--ligação entre RETORNO_CIELO x SCAP x RM
select seq, estabele, dtCompen, dtVenda, parc, plan, nuCartao, codAuto, nsuDoc, vlrBruto, terminal, c.nuCliente,c.nuClienteCartao
from SITEF.RETORNO_CIELO rt
left join scap.cartaoTRN c on  substr(c.nuDocumento,4,6)=rt.nsuDoc and c.nmAutorizacao=rt.codAuto
where sinal = '+' and rt.nuCliente is null
order by rt.dtCompen, rt.seq
) LOOP
    if i.nuCliente is not null then 
        update SITEF.RETORNO_CIELO set 
            nuCliente=i.nuCliente, 
            nuClienteCartao=i.nuClienteCartao
        where dtCompen=i.dtCompen and seq=i.seq;
        commit;
    end if;
END LOOP;
END SETNUCLIENTE;

