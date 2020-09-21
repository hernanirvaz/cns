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
      ") VALUES#{apies.novtx.map { |e| etht_val1(e) }.join(',')}"
    end

    # @example (see Apibc#norml_es)
    # @param [Hash] htx transacao norml etherscan
    # @return [String] valores formatados etht (norml parte1)
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

    # @param (see etht_val1)
    # @return [String] valores formatados etht (norml parte2)
    def etht_val2(htx)
      "cast('#{htx[:value]}' as numeric)," \
      "cast('#{htx[:gas]}' as numeric)," \
      "cast('#{htx[:gasPrice]}' as numeric)," \
      "cast('#{htx[:gasUsed]}' as numeric)," \
      "#{Integer(htx[:isError])}," \
      "#{htx[:txreceipt_status].length.zero? ? 'null' : htx[:txreceipt_status]}," \
      "#{etht_val3(htx)}"
    end

    # @param (see etht_val1)
    # @return [String] valores formatados etht (norml parte3)
    def etht_val3(htx)
      "#{htx[:input].length.zero? ? 'null' : "'#{htx[:input]}'"}," \
      "#{htx[:contractAddress].length.zero? ? 'null' : "'#{htx[:contractAddress]}'"}," \
      "#{Integer(ops[:h][htx[:blockNumber]] || 0)})"
    end

    # @return [String] comando insert SQL formatado ethk (token)
    def ethk_ins
      "insert #{BD}.ethk(blocknumber,timestamp,txhash,nonce,blockhash,transactionindex,axfrom,axto,iax," \
      'value,tokenname,tokensymbol,tokendecimal,gas,gasprice,gasused,input,contractaddress,dias' \
      ") VALUES#{apies.novkx.map { |e| ethk_val1(e) }.join(',')}"
    end

    # @example (see Apibc#token_es)
    # @param [Hash] hkx token event etherscan
    # @return [String] valores formatados ethk (token parte1)
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

    # @param (see ethk_val1)
    # @return [String] valores formatados ethk (token parte2)
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

    # @param (see ethk_val1)
    # @return [String] valores formatados ethk (token parte3)
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

    # @example (see Apibc#ledger_gm)
    # @param [Hash] hlx ledger greymass
    # @return [String] valores formatados para insert eos (parte1)
    def eost_val1(hlx)
      a = hlx[:action_trace][:act]
      "(#{hlx[:global_action_seq]}," \
      "#{hlx[:account_action_seq]}," \
      "#{hlx[:block_num]}," \
      "DATETIME(TIMESTAMP('#{hlx[:block_time]}'))," \
      "'#{a[:account]}'," \
      "'#{a[:name]}'," \
      "#{eost_val2(hlx, a)}"
    end

    # @param (see eost_val1)
    # @param [Hash] act dados da acao
    # @return [String] valores formatados para insert eos (parte2)
    def eost_val2(hlx, act)
      d = act[:data]
      q = d[:quantity].to_s
      s = d[:memo].inspect
      "'#{d[:from]}'," \
      "'#{d[:to]}'," \
      "'#{hlx[:iax]}'," \
      "#{q.to_d},'#{q[/[[:upper:]]+/]}'," \
      "nullif('#{s.gsub(/['"]/, '')}','nil')," \
      "#{ops[:h][String(hlx[:itx])] || 0})"
    end
  end
end
