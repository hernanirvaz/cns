# frozen_string_literal: true

require('google/cloud/bigquery')
require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  BD = 'hernanirvaz.coins'
  FO = File.expand_path("~/#{File.basename($PROGRAM_NAME)}.log")

  # classe para processar bigquery
  class Bigquery
    # @return [Google::Cloud::Bigquery] API bigquery
    attr_reader :api
    # @return [Google::Cloud::Bigquery::QueryJob] job bigquery
    attr_reader :job
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops
    # @return (see sql)
    attr_reader :sqr

    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra transacoes trades & ledger?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    # @return [Bigquery] API bigquery
    def initialize(pop)
      # usa env GOOGLE_APPLICATION_CREDENTIALS para obter credentials
      # @see https://cloud.google.com/bigquery/docs/authentication/getting-started
      @api = Google::Cloud::Bigquery.new
      @ops = pop
    end

    # mostra situacao completa entre kraken/bitcoinde/paymium/therock/etherscan/greymass & bigquery
    def mostra_tudo
      apius.mostra_resumo
      apide.mostra_resumo
      apies.mostra_resumo
      apigm.mostra_resumo
    end

    # mostra situacao completa entre kraken/etherscan & bigquery
    def mostra_skrk
      apius.mostra_resumo
      apies.mostra_resumo
    end

    # mostra situacao completa entre etherscan & bigquery
    def mostra_seth
      apies.mostra_resumo_simples
    end

    # @return [String] texto inicial transacoes
    def trs_ini
      Time.now.strftime('TRANSACOES  %Y-%m-%d %H:%M:%S ')
    end

    # insere (caso existam) dados novos kraken/bitcoinde/paymium/therock/etherscan/greymass no bigquery
    def processa_tudo
      str = "#{processa_us}, #{processa_de}, #{processa_eth}, #{processa_eos}"
      puts(trs_ini + str)
    end

    # insere (caso existam) dados novos kraken/etherscan no bigquery
    def processa_wkrk
      str = "#{processa_us}, #{processa_eth}"
      puts(trs_ini + str)
    end

    # insere (caso existam) dados novos etherscan no bigquery
    def processa_weth
      str = processa_eth
      puts(trs_ini + str)
    end

    # insere (caso existam) dados novos etherscan no bigquery (output to file)
    def processa_ceth
      str = processa_ethc
      File.open(FO, mode: 'a') { |out| out.puts(trs_ini + str) }
    end

    private

    # insere transacoes blockchain novas nas tabelas netht (norml), nethi (internas), nethp (block), nethw (withdrawals), nethk (token)
    #
    # @return [String] linhas & tabelas afetadas
    def processa_eth
      str = 'ETH'
      str += format(' %<n>i netht', n: dml(netht_ins)) if apies.novtx.count.positive?
      str += format(' %<n>i nethi', n: dml(nethi_ins)) if apies.novix.count.positive?
      str += format(' %<n>i nethp', n: dml(nethp_ins)) if apies.novpx.count.positive?
      str += format(' %<n>i nethw', n: dml(nethw_ins)) if apies.novwx.count.positive?
      str += format(' %<n>i nethk', n: dml(nethk_ins)) if apies.novkx.count.positive?
      str
    end

    # insere transacoes blockchain novas nas tabelas netht (norml), nethi (internas), nethp (block), nethw (withdrawals), nethk (token)
    #
    # @return [String] linhas & tabelas afetadas
    def processa_ethc
      str = 'ETH'
      str += format(' %<n>i netht', n: dml(netbt_ins)) if apiesc.novtx.count.positive?
      str += format(' %<n>i nethi', n: dml(netbi_ins)) if apiesc.novix.count.positive?
      str += format(' %<n>i nethp', n: dml(netbp_ins)) if apiesc.novpx.count.positive?
      str += format(' %<n>i nethw', n: dml(netbw_ins)) if apiesc.novwx.count.positive?
      str += format(' %<n>i nethk', n: dml(netbk_ins)) if apiesc.novkx.count.positive?
      str
    end

    # insere transacoes exchange kraken novas nas tabelas ust (trades), usl (ledger)
    #
    # @return [String] linhas & tabelas afetadas
    def processa_us
      str = 'KRAKEN'
      str += format(' %<n>i ust', n: dml(ust_ins)) if apius.trades.count.positive?
      str += format(' %<n>i usl', n: dml(usl_ins)) if apius.ledger.count.positive?
      str
    end

    # insere transacoes exchange bitcoinde novas nas tabelas det (trades), del (ledger)
    #
    # @return [String] linhas & tabelas afetadas
    def processa_de
      str = 'BITCOINDE'
      str += format(' %<n>i det', n: dml(det_ins)) if apide.trades.count.positive?
      str += format(' %<n>i del', n: dml(del_ins)) if apide.ledger.count.positive?
      str
    end

    # insere transacoes blockchain novas na tabela eos
    #
    # @return [String] linhas & tabelas afetadas
    def processa_eos
      str = 'EOS'
      str += format(' %<n>i eos ', n: dml(eost_ins)) if apigm.novax.count.positive?
      str
    end

    # cria job bigquery & verifica execucao
    #
    # @param cmd (see sql)
    # @return [Boolean] job ok?
    def job?(cmd)
      @job = api.query_job(cmd)
      job.wait_until_done!
      fld = job.failed?
      puts(job.error['message']) if fld
      fld
    end

    # cria Structured Query Language (SQL) job bigquery
    #
    # @param [String] cmd comando SQL a executar
    # @param [String] res resultado quando SQL tem erro
    # @return [Google::Cloud::Bigquery::Data] resultado do SQL
    def sql(cmd, res = [])
      @sqr = job?(cmd) ? res : job.data
    end

    # cria Data Manipulation Language (DML) job bigquery
    #
    # @param cmd (see sql)
    # @return [Integer] numero linhas afetadas
    def dml(cmd)
      job?(cmd) ? 0 : job.num_dml_affected_rows
    end

    # @return [Etherscan] API blockchain ETH
    def apies
      @apies ||= Etherscan.new(
        {
          wb: sql("select * from #{BD}.wetb order by ax"),
          ni: sql("select * from #{BD}.netbi"),
          nk: sql("select * from #{BD}.netbk"),
          np: sql("select * from #{BD}.netbp"),
          nt: sql("select * from #{BD}.netbt"),
          nw: sql("select * from #{BD}.netbw")
        },
        ops
      )
    end

    # @return [Etherscan] API blockchain ETH
    def apiesc
      @apiesc ||= Etherscan.new(
        {
          wb: sql("select * from #{BD}.wetc order by ax"),
          ni: sql("select * from #{BD}.netci"),
          nk: sql("select * from #{BD}.netck"),
          np: sql("select * from #{BD}.netcp"),
          nt: sql("select * from #{BD}.netct"),
          nw: sql("select * from #{BD}.netcw")
        },
        ops
      )
    end

    # @return [Greymass] API blockchain EOS
    def apigm
      @apigm ||= Greymass.new({ wb: sql("select * from #{BD}.weos order by ax"), nt: sql("select * from #{BD}.neosx") }, ops)
    end

    # @return [Kraken] API exchange kraken
    def apius
      @apius ||= Kraken.new(
        {
          sl: sql("select * from #{BD}.cuss").first,
          nt: sql("select * from #{BD}.cust order by time,txid"),
          nl: sql("select * from #{BD}.cusl order by time,txid")
        },
        ops
      )
    end

    # @return [Bitcoinde] API exchange bitcoinde
    def apide
      @apide ||= Bitcoinde.new(
        {
          sl: sql("select * from #{BD}.cdes").first,
          nt: sql("select * from #{BD}.cdet order by time,txid"),
          nl: sql("select * from #{BD}.cdel order by time,txid")
        },
        ops
      )
    end

    # @return [String] comando insert SQL formatado netht (norml)
    def bnetht_ins
      "insert #{BD}.netht(blocknumber,timestamp,txhash,nonce,blockhash,transactionindex,axfrom,axto,iax," \
        'value,gas,gasprice,gasused,iserror,txreceipt_status,input,contractaddress,dias) VALUES'
    end

    # @return [String] comando insert SQL formatado nethi (internas)
    def bnethi_ins
      "insert #{BD}.nethi(blocknumber,timestamp,txhash,axfrom,axto,iax," \
        'value,contractaddress,input,type,gas,gasused,traceid,iserror,errcode) VALUES'
    end

    # @return [String] comando insert SQL formatado nethp (block)
    def bnethp_ins
      "insert #{BD}.nethp(blocknumber,timestamp,blockreward,iax) VALUES"
    end

    # @return [String] comando insert SQL formatado nethw (withdrawals)
    def bnethw_ins
      "insert #{BD}.nethw(withdrawalindex,validatorindex,address,amount,blocknumber,timestamp) VALUES"
    end

    # @return [String] comando insert SQL formatado nethk (token)
    def bnethk_ins
      "insert #{BD}.nethk(blocknumber,timestamp,txhash,nonce,blockhash,transactionindex,axfrom,axto,iax," \
        'value,tokenname,tokensymbol,tokendecimal,gas,gasprice,gasused,input,contractaddress,dias) VALUES'
    end

    # @return [String] comando insert SQL formatado netht (norml)
    def netht_ins
      "#{bnetht_ins}#{apies.novtx.map { |obj| netht_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado nethi (internas)
    def nethi_ins
      "#{bnethi_ins}#{apies.novix.map { |obj| nethi_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado nethp (block)
    def nethp_ins
      "#{bnethp_ins}#{apies.novpx.map { |obj| nethp_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado nethw (withdrawals)
    def nethw_ins
      "#{bnethw_ins}#{apies.novwx.map { |obj| nethw_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado nethk (token)
    def nethk_ins
      "#{bnethk_ins}#{apies.novkx.map { |obj| nethk_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado netht (norml)
    def netbt_ins
      "#{bnetht_ins}#{apiesc.novtx.map { |obj| netht_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado nethi (internas)
    def netbi_ins
      "#{bnethi_ins}#{apiesc.novix.map { |obj| nethi_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado nethp (block)
    def netbp_ins
      "#{bnethp_ins}#{apiesc.novpx.map { |obj| nethp_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado nethw (withdrawals)
    def netbw_ins
      "#{bnethw_ins}#{apiesc.novwx.map { |obj| nethw_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado nethk (token)
    def netbk_ins
      "#{bnethk_ins}#{apiesc.novkx.map { |obj| nethk_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado eos
    def eost_ins
      "insert #{BD}.neost(gseq,aseq,bnum,time,contract,action,acfrom,acto,iax,amount,moeda,memo,dias" \
        ") VALUES#{apigm.novax.map { |obj| eost_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado det (trades)
    def det_ins
      "insert #{BD}.cdet(txid,time,tp,user,btc,eur,dtc,dias) VALUES#{apide.trades.map { |obj| det_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado del (ledger)
    def del_ins
      "insert #{BD}.cdel(txid,time,tp,add,moe,qt,fee) VALUES#{apide.ledger.map { |obj| del_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado ust (trades)
    def ust_ins
      "insert #{BD}.cust(txid,ordertxid,pair,time,type,ordertype,price,cost,fee,vol,margin,misc,ledgers,dias) " \
        "VALUES#{apius.trades.map { |key, val| ust_val(key, val) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado usl (ledger)
    def usl_ins
      "insert #{BD}.cusl(txid,refid,time,type,aclass,asset,amount,fee) " \
        "VALUES#{apius.ledger.map { |key, val| usl_val(key, val) }.join(',')}"
    end

    # @example (see Apibc#norml_es)
    # @param [Hash] htx transacao norml etherscan
    # @return [String] valores formatados netht (norml parte1)
    def netht_val(htx)
      txr = htx[:txreceipt_status]
      cta = htx[:contractAddress]
      inp = htx[:input]
      "(#{Integer(htx[:blockNumber])}," \
        "#{Integer(htx[:timeStamp])}," \
        "'#{htx[:hash]}'," \
        "#{Integer(htx[:nonce])}," \
        "'#{htx[:blockHash]}'," \
        "#{Integer(htx[:transactionIndex])}," \
        "'#{htx[:from]}'," \
        "'#{htx[:to]}'," \
        "'#{htx[:iax]}'," \
        "cast('#{htx[:value]}' as numeric)," \
        "cast('#{htx[:gas]}' as numeric)," \
        "cast('#{htx[:gasPrice]}' as numeric)," \
        "cast('#{htx[:gasUsed]}' as numeric)," \
        "#{Integer(htx[:isError])}," \
        "#{txr.empty? ? 'null' : txr}," \
        "#{inp.empty? ? 'null' : "'#{inp}'"}," \
        "#{cta.empty? ? 'null' : "'#{cta}'"}," \
        "#{Integer(ops[:h][htx[:blockNumber]] || 0)})"
    end

    # @example (see Apibc#inter_es)
    # @param [Hash] htx transacao internas etherscan
    # @return [String] valores formatados nethi (internas parte1)
    def nethi_val(htx)
      cta = htx[:contractAddress]
      inp = htx[:input]
      tid = htx[:traceId]
      txr = htx[:errCode]
      "(#{Integer(htx[:blockNumber])}," \
        "#{Integer(htx[:timeStamp])}," \
        "'#{htx[:hash]}'," \
        "'#{htx[:from]}'," \
        "'#{htx[:to]}'," \
        "'#{htx[:iax]}'," \
        "cast('#{htx[:value]}' as numeric)," \
        "#{cta.empty? ? 'null' : "'#{cta}'"}," \
        "#{inp.empty? ? 'null' : "'#{inp}'"}," \
        "'#{htx[:type]}'," \
        "cast('#{htx[:gas]}' as numeric)," \
        "cast('#{htx[:gasUsed]}' as numeric)," \
        "#{tid.empty? ? 'null' : "'#{tid}'"}," \
        "#{Integer(htx[:isError])}," \
        "#{txr.empty? ? 'null' : txr})"
    end

    # @example (see Apibc#block_es)
    # @param [Hash] htx transacao block etherscan
    # @return [String] valores formatados nethi (block parte1)
    def nethp_val(htx)
      "(#{Integer(htx[:blockNumber])}," \
        "#{Integer(htx[:timeStamp])}," \
        "cast('#{htx[:blockReward]}' as numeric)," \
        "'#{htx[:iax]}')"
    end

    # @example (see Apibc#block_es)
    # @param [Hash] htx transacao withdrawals etherscan
    # @return [String] valores formatados nethi (withdrawals parte1)
    def nethw_val(htx)
      "(#{Integer(htx[:withdrawalIndex])}," \
        "#{Integer(htx[:validatorIndex])}," \
        "'#{htx[:address]}'," \
        "cast('#{htx[:amount]}' as numeric)," \
        "#{Integer(htx[:blockNumber])}," \
        "#{Integer(htx[:timestamp])})"
    end

    # @example (see Apibc#token_es)
    # @param [Hash] hkx token event etherscan
    # @return [String] valores formatados nethk (token parte1)
    def nethk_val(hkx)
      cta = hkx[:contractAddress]
      inp = hkx[:input]
      "(#{Integer(hkx[:blockNumber])}," \
        "#{Integer(hkx[:timeStamp])}," \
        "'#{hkx[:hash]}'," \
        "#{Integer(hkx[:nonce])}," \
        "'#{hkx[:blockHash]}'," \
        "#{Integer(hkx[:transactionIndex])}," \
        "'#{hkx[:from]}'," \
        "'#{hkx[:to]}'," \
        "'#{hkx[:iax]}'," \
        "cast('#{hkx[:value]}' as numeric)," \
        "'#{hkx[:tokenName]}'," \
        "'#{hkx[:tokenSymbol]}'," \
        "#{Integer(hkx[:tokenDecimal])}," \
        "cast('#{hkx[:gas]}' as numeric)," \
        "cast('#{hkx[:gasPrice]}' as numeric)," \
        "cast('#{hkx[:gasUsed]}' as numeric)," \
        "#{inp.empty? ? 'null' : "'#{inp}'"}," \
        "#{cta.empty? ? 'null' : "'#{cta}'"}," \
        "#{Integer(ops[:h][hkx[:blockNumber]] || 0)})"
    end

    # @example (see Apibc#ledger_gm)
    # @param [Hash] hlx ledger greymass
    # @return [String] valores formatados para insert eos (parte1)
    def eost_val(hlx)
      act = hlx[:action_trace][:act]
      dat = act[:data]
      qtd = dat[:quantity].to_s
      str = dat[:memo].inspect
      "(#{hlx[:global_action_seq]}," \
        "#{hlx[:account_action_seq]}," \
        "#{hlx[:block_num]}," \
        "DATETIME(TIMESTAMP('#{hlx[:block_time]}'))," \
        "'#{act[:account]}'," \
        "'#{act[:name]}'," \
        "'#{dat[:from]}'," \
        "'#{dat[:to]}'," \
        "'#{hlx[:iax]}'," \
        "#{qtd.to_d},'#{qtd[/[[:upper:]]+/]}'," \
        "nullif('#{str.gsub(/['"]/, '')}','nil')," \
        "#{ops[:h][String(hlx[:itx])] || 0})"
    end

    # @example (see Apice#trades_de)
    # @param [Hash] htx trade bitcoinde
    # @return [String] valores formatados det (trades parte1)
    def det_val(htx)
      "('#{htx[:trade_id]}'," \
        "DATETIME(TIMESTAMP('#{htx[:successfully_finished_at]}'))," \
        "'#{htx[:type]}'," \
        "'#{htx[:trading_partner_information][:username]}'," \
        'cast(' \
        "#{htx[:type] == 'buy' ? htx[:amount_currency_to_trade_after_fee] : "-#{htx[:amount_currency_to_trade]}"} " \
        'as numeric),' \
        "cast(#{htx[:volume_currency_to_pay_after_fee]} as numeric)," \
        "DATETIME(TIMESTAMP('#{htx[:trade_marked_as_paid_at]}'))," \
        "#{Integer(ops[:h][htx[:trade_id]] || 0)})"
    end

    # @example (see Apice#deposits_de)
    # @example (see Apice#withdrawals_de)
    # @param [Hash] hlx ledger (deposits + withdrawals) bitcoinde
    # @return [String] valores formatados del (ledger)
    def del_val(hlx)
      tip = hlx[:tp]
      "(#{hlx[:txid]}," \
        "DATETIME(TIMESTAMP('#{hlx[:time].iso8601}'))," \
        "'#{tip}'," \
        "'#{hlx[:add]}'," \
        "'#{hlx[:moe]}'," \
        "cast(#{tip == 'withdrawal' ? '-' : ''}#{hlx[:qt]} as numeric)," \
        "cast(#{hlx[:fee]} as numeric))"
    end

    # @example (see Apice#trades_us)
    # @param [String] idx identificador transacao
    # @param [Hash] htx trade kraken
    # @return [String] valores formatados ust (trades parte1)
    def ust_val(idx, htx)
      msc = htx[:misc].to_s
      "('#{idx}'," \
        "'#{htx[:ordertxid]}'," \
        "'#{htx[:pair]}'," \
        "PARSE_DATETIME('%s', '#{String(htx[:time].round)}')," \
        "'#{htx[:type]}'," \
        "'#{htx[:ordertype]}'," \
        "cast(#{htx[:price]} as numeric)," \
        "cast(#{htx[:cost]} as numeric)," \
        "cast(#{htx[:fee]} as numeric)," \
        "cast(#{htx[:vol]} as numeric)," \
        "cast(#{htx[:margin]} as numeric)," \
        "#{msc.empty? ? 'null' : "'#{msc}'"}," \
        "'#{apius.ledger.select { |_, val| val[:refid] == idx }.keys.join(',') || ''}'," \
        "#{Integer(ops[:h][idx] || 0)})"
    end

    # @example (see Apice#ledger_us)
    # @param idx (see ust_val)
    # @param [Hash] hlx ledger kraken
    # @return [String] valores formatados usl (ledger)
    def usl_val(idx, hlx)
      acl = hlx[:aclass].to_s
      "('#{idx}'," \
        "'#{hlx[:refid]}'," \
        "PARSE_DATETIME('%s', '#{String(hlx[:time].round)}')," \
        "'#{hlx[:type]}'," \
        "#{acl.empty? ? 'null' : "'#{acl}'"}," \
        "'#{hlx[:asset]}'," \
        "cast(#{hlx[:amount]} as numeric)," \
        "cast(#{hlx[:fee]} as numeric))"
    end
  end
end
