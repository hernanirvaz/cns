# frozen_string_literal: true

require('google/cloud/bigquery')
require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar etherscan/greymass & bigquery
  class Bigquery
    # @return [String] comando insert SQL formatado etht (trx normais)
    def etht_ins
      "insert #{BD}.etht(blocknumber,timestamp,txhash,nonce,blockhash,transactionindex,axfrom,axto,iax," \
      'value,gas,gasprice,gasused,iserror,txreceipt_status,input,contractaddress,dias' \
      ") VALUES#{apies.novtx.map { |e| etht_val1(e) }.join(',')}"
    end

    # @return [String] valores formatados etht (trx normais parte1)
    def etht_val1(htx)
      "(#{Integer(htx[:blockNumber])}," \
      "#{Integer(htx[:timeStamp])}," \
      "'#{htx[:hash]}'," \
      "#{Integer(htx[:nonce])}," \
      "'#{htx[:blockHash]}'," \
      "#{Integer(htx[:transactionIndex])}," \
      "'#{htx[:from]}'," \
      "'#{htx[:to]}'," \
      "'#{htx[:iax]}'," \
      "#{etht_val2(htx)}"
    end

    # @return [String] valores formatados etht (trx normais parte2)
    def etht_val2(htx)
      "cast('#{htx[:value]}' as numeric)," \
      "cast('#{htx[:gas]}' as numeric)," \
      "cast('#{htx[:gasPrice]}' as numeric)," \
      "cast('#{htx[:gasUsed]}' as numeric)," \
      "#{Integer(htx[:isError])}," \
      "#{htx[:txreceipt_status].length.zero? ? 'null' : htx[:txreceipt_status]}," \
      "#{etht_val3(htx)}"
    end

    # @return [String] valores formatados etht (trx normais parte3)
    def etht_val3(htx)
      "#{htx[:input].length.zero? ? 'null' : "'#{htx[:input]}'"}," \
      "#{htx[:contractAddress].length.zero? ? 'null' : "'#{htx[:contractAddress]}'"}," \
      "#{Integer(ops[:h][htx[:blockNumber]] || 0)})"
    end

    # @return [String] comando insert SQL formatado ethk (trx token)
    def ethk_ins
      "insert #{BD}.ethk(blocknumber,timestamp,txhash,nonce,blockhash,transactionindex,axfrom,axto,iax," \
      'value,tokenname,tokensymbol,tokendecimal,gas,gasprice,gasused,input,contractaddress,dias' \
      ") VALUES#{apies.novkx.map { |e| ethk_val1(e) }.join(',')}"
    end

    # @return [String] valores formatados ethk (trx token parte1)
    def ethk_val1(hkx)
      "(#{Integer(hkx[:blockNumber])}," \
      "#{Integer(hkx[:timeStamp])}," \
      "'#{hkx[:hash]}'," \
      "#{Integer(hkx[:nonce])}," \
      "'#{hkx[:blockHash]}'," \
      "#{Integer(hkx[:transactionIndex])}," \
      "'#{hkx[:from]}'," \
      "'#{hkx[:to]}'," \
      "'#{hkx[:iax]}'," \
      "#{ethk_val2(hkx)}"
    end

    # @return [String] valores formatados ethk (trx token parte2)
    def ethk_val2(hkx)
      "cast('#{hkx[:value]}' as numeric)," \
      "'#{hkx[:tokenName]}'," \
      "'#{hkx[:tokenSymbol]}'," \
      "#{Integer(hkx[:tokenDecimal])}," \
      "cast('#{hkx[:gas]}' as numeric)," \
      "cast('#{hkx[:gasPrice]}' as numeric)," \
      "cast('#{hkx[:gasUsed]}' as numeric)," \
      "#{ethk_val3(hkx)}"
    end

    # @return [String] valores formatados ethk (trx token parte3)
    def ethk_val3(hkx)
      "#{hkx[:input].length.zero? ? 'null' : "'#{hkx[:input]}'"}," \
      "#{hkx[:contractAddress].length.zero? ? 'null' : "'#{hkx[:contractAddress]}'"}," \
      "#{Integer(ops[:h][hkx[:blockNumber]] || 0)})"
    end

    # @return [String] comando insert SQL formatado eos
    def eost_ins
      "insert #{BD}.eos(gseq,aseq,bnum,time,contract,action,acfrom,acto,iax,amount,moeda,memo,dias" \
      ") VALUES#{apigm.novax.map { |e| eost_val1(e) }.join(',')}"
    end

    # @param [Hash] htx transacao ligadas a uma carteira - sem elementos irrelevantes
    # @return [String] valores formatados para insert eos (parte1)
    def eost_val1(htx)
      a = htx[:action_trace][:act]
      "(#{htx[:global_action_seq]}," \
      "#{htx[:account_action_seq]}," \
      "#{htx[:block_num]}," \
      "DATETIME(TIMESTAMP('#{htx[:block_time]}'))," \
      "'#{a[:account]}'," \
      "'#{a[:name]}'," \
      "#{eost_val2(htx, a)}"
    end

    # @param [Hash] htx transacao ligadas a uma carteira - sem elementos irrelevantes
    # @return [String] valores formatados para insert eos (parte2)
    def eost_val2(htx, act)
      d = act[:data]
      q = d[:quantity].to_s
      s = d[:memo].inspect
      "'#{d[:from]}'," \
      "'#{d[:to]}'," \
      "'#{htx[:iax]}'," \
      "#{q.to_d},'#{q[/[[:upper:]]+/]}'," \
      "nullif('#{s.gsub(/['"]/, '')}','nil')," \
      "#{ops[:h][String(htx[:itx])] || 0})"
    end
  end
end
