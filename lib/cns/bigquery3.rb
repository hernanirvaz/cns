# frozen_string_literal: true

require('google/cloud/bigquery')
require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # (see Bigquery)
  class Bigquery
    private

    # @return [String] comando insert SQL formatado etht (norml)
    def etht_ins
      "insert #{BD}.etht(blocknumber,timestamp,txhash,nonce,blockhash,transactionindex,axfrom,axto,iax," \
      'value,gas,gasprice,gasused,iserror,txreceipt_status,input,contractaddress,dias' \
      ") VALUES#{apies.novtx.map { |obj| etht_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado ethk (token)
    def ethk_ins
      "insert #{BD}.ethk(blocknumber,timestamp,txhash,nonce,blockhash,transactionindex,axfrom,axto,iax," \
      'value,tokenname,tokensymbol,tokendecimal,gas,gasprice,gasused,input,contractaddress,dias' \
      ") VALUES#{apies.novkx.map { |obj| ethk_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado eos
    def eost_ins
      "insert #{BD}.eos(gseq,aseq,bnum,time,contract,action,acfrom,acto,iax,amount,moeda,memo,dias" \
      ") VALUES#{apigm.novax.map { |obj| eost_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado det (trades)
    def det_ins
      "insert #{BD}.det(txid,time,tp,user,btc,eur,dtc,dias) VALUES#{apide.trades.map { |obj| det_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado del (ledger)
    def del_ins
      "insert #{BD}.del(txid,time,tp,add,moe,qt,fee) VALUES#{apide.ledger.map { |obj| del_val(obj) }.join(',')}"
    end

    # @example (see Apibc#norml_es)
    # @param [Hash] htx transacao norml etherscan
    # @return [String] valores formatados etht (norml parte1)
    def etht_1val(htx)
      "(#{Integer(htx[:blockNumber])}," \
      "#{Integer(htx[:timeStamp])}," \
      "'#{htx[:hash]}'," \
      "#{Integer(htx[:nonce])}," \
      "'#{htx[:blockHash]}'," \
      "#{Integer(htx[:transactionIndex])}," \
      "'#{htx[:from]}'," \
      "'#{htx[:to]}'," \
      "'#{htx[:iax]}'," \
      "#{etht_2val(htx)}"
    end

    # @param (see etht_1val)
    # @return [String] valores formatados etht (norml parte2)
    def etht_2val(htx)
      txr = htx[:txreceipt_status]
      "cast('#{htx[:value]}' as numeric)," \
      "cast('#{htx[:gas]}' as numeric)," \
      "cast('#{htx[:gasPrice]}' as numeric)," \
      "cast('#{htx[:gasUsed]}' as numeric)," \
      "#{Integer(htx[:isError])}," \
      "#{txr.length.zero? ? 'null' : txr}," \
      "#{etht_3val(htx)}"
    end

    # @param (see etht_1val)
    # @return [String] valores formatados etht (norml parte3)
    def etht_3val(htx)
      cta = htx[:contractAddress]
      inp = htx[:input]
      "#{inp.length.zero? ? 'null' : "'#{inp}'"}," \
      "#{cta.length.zero? ? 'null' : "'#{cta}'"}," \
      "#{Integer(ops[:h][htx[:blockNumber]] || 0)})"
    end

    # @example (see Apibc#token_es)
    # @param [Hash] hkx token event etherscan
    # @return [String] valores formatados ethk (token parte1)
    def ethk_1val(hkx)
      "(#{Integer(hkx[:blockNumber])}," \
      "#{Integer(hkx[:timeStamp])}," \
      "'#{hkx[:hash]}'," \
      "#{Integer(hkx[:nonce])}," \
      "'#{hkx[:blockHash]}'," \
      "#{Integer(hkx[:transactionIndex])}," \
      "'#{hkx[:from]}'," \
      "'#{hkx[:to]}'," \
      "'#{hkx[:iax]}'," \
      "#{ethk_2val(hkx)}"
    end

    # @param (see ethk_1val)
    # @return [String] valores formatados ethk (token parte2)
    def ethk_2val(hkx)
      "cast('#{hkx[:value]}' as numeric)," \
      "'#{hkx[:tokenName]}'," \
      "'#{hkx[:tokenSymbol]}'," \
      "#{Integer(hkx[:tokenDecimal])}," \
      "cast('#{hkx[:gas]}' as numeric)," \
      "cast('#{hkx[:gasPrice]}' as numeric)," \
      "cast('#{hkx[:gasUsed]}' as numeric)," \
      "#{ethk_3val(hkx)}"
    end

    # @param (see ethk_1val)
    # @return [String] valores formatados ethk (token parte3)
    def ethk_3val(hkx)
      cta = hkx[:contractAddress]
      inp = hkx[:input]
      "#{inp.length.zero? ? 'null' : "'#{inp}'"}," \
      "#{cta.length.zero? ? 'null' : "'#{cta}'"}," \
      "#{Integer(ops[:h][hkx[:blockNumber]] || 0)})"
    end

    # @example (see Apibc#ledger_gm)
    # @param [Hash] hlx ledger greymass
    # @return [String] valores formatados para insert eos (parte1)
    def eost_1val(hlx)
      act = hlx[:action_trace][:act]
      "(#{hlx[:global_action_seq]}," \
      "#{hlx[:account_action_seq]}," \
      "#{hlx[:block_num]}," \
      "DATETIME(TIMESTAMP('#{hlx[:block_time]}'))," \
      "'#{act[:account]}'," \
      "'#{act[:name]}'," \
      "#{eost_2val(hlx, act)}"
    end

    # @param (see eost_1val)
    # @param [Hash] act dados da acao
    # @return [String] valores formatados para insert eos (parte2)
    def eost_2val(hlx, act)
      dat = act[:data]
      qtd = dat[:quantity].to_s
      str = dat[:memo].inspect
      "'#{dat[:from]}'," \
      "'#{dat[:to]}'," \
      "'#{hlx[:iax]}'," \
      "#{qtd.to_d},'#{qtd[/[[:upper:]]+/]}'," \
      "nullif('#{str.gsub(/['"]/, '')}','nil')," \
      "#{ops[:h][String(hlx[:itx])] || 0})"
    end
  end
end
