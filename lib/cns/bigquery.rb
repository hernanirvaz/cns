# frozen_string_literal: true

require('google/cloud/bigquery')
require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  BD = 'hernanirvaz.coins'
  FO = File.expand_path("~/#{File.basename($PROGRAM_NAME)}.log")
  # Define table configurations at the class level
  TC = {
    i: %w[blocknumber timestamp txhash axfrom axto iax value contractaddress input type gas gasused traceid iserror errcode],
    p: %w[blocknumber timestamp blockreward iax],
    w: %w[withdrawalindex validatorindex address amount blocknumber timestamp],
    t: %w[blocknumber timestamp txhash nonce blockhash transactionindex axfrom axto iax value gas gasprice gasused iserror txreceipt_status input contractaddress dias],
    k: %w[blocknumber timestamp txhash nonce blockhash transactionindex axfrom axto iax value tokenname tokensymbol tokendecimal gas gasprice gasused input contractaddress dias],
    neost: %w[gseq aseq bnum time contract action acfrom acto iax amount moeda memo dias],
    cdet: %w[txid time tp user btc eur dtc dias],
    cdel: %w[txid time tp add moe qt fee],
    cust: %w[txid ordertxid pair time type ordertype price cost fee vol margin misc ledgers dias],
    cusl: %w[txid refid time type aclass asset amount fee]
  }

  # classe para processar bigquery
  class Bigquery
    # @return [Google::Cloud::Bigquery] API bigquery
    # @return [Google::Cloud::Bigquery::QueryJob] job bigquery
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    # @return (see sql)
    attr_reader :api, :job, :ops, :sqr

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
      tabelas_eth(apies, 'netb')
    end

    # insere transacoes blockchain novas nas tabelas netht (norml), nethi (internas), nethp (block), nethw (withdrawals), nethk (token)
    #
    # @return [String] linhas & tabelas afetadas
    def processa_ethc
      tabelas_eth(apiesc, 'netc')
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

      return false unless job.failed?

      puts("BigQuery Error: #{job.error['message']}")
      true
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

    def initialize_etherscan_client(prx)
      Etherscan.new(
        {
          wb: sql("SELECT * FROM #{BD}.wet#{prx[-1]} ORDER BY ax"),
          ni: sql("SELECT * FROM #{BD}.#{prx}i"),
          nk: sql("SELECT * FROM #{BD}.#{prx}k"),
          np: sql("SELECT * FROM #{BD}.#{prx}p"),
          nt: sql("SELECT * FROM #{BD}.#{prx}t"),
          nw: sql("SELECT * FROM #{BD}.#{prx}w")
        },
        ops
      )
    end

    # @return [Etherscan] API blockchain ETH
    def apies
      @apies ||= initialize_etherscan_client('netb')
    end

    # @return [Etherscan] API blockchain ETH
    def apiesc
      @apiesc ||= initialize_etherscan_client('netc')
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

    # Generic ETH data processor
    def tabelas_eth(src, prx)
      str = ['ETH']
      %i[t i p w k].each do |typ|
        novx = src.send("nov#{typ}x")
        next if novx.empty?

        str << format(' %<n>i %<t>s', n: dml(insert_eht(typ, novx)), t: "#{prx.chop}h#{typ}")
      end
      str.join
    end

    def insert_eht(typ, lin)
      "INSERT #{BD}.neth#{typ} (#{TC[typ].join(',')}) VALUES #{lin.map { |itm| send("neth#{typ}_val", itm) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado eos
    def eost_ins
      "insert #{BD}.neost(#{TC[:neost].join(',')}) VALUES#{apigm.novax.map { |obj| eost_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado det (trades)
    def det_ins
      "insert #{BD}.cdet(#{TC[:cdet].join(',')}) VALUES#{apide.trades.map { |obj| det_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado del (ledger)
    def del_ins
      "insert #{BD}.cdel(#{TC[:cdel].join(',')}) VALUES#{apide.ledger.map { |obj| del_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado ust (trades)
    def ust_ins
      "insert #{BD}.cust(#{TC[:cust].join(',')}) VALUES#{apius.trades.map { |key, val| ust_val(key, val) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado usl (ledger)
    def usl_ins
      "insert #{BD}.cusl(#{TC[:cusl].join(',')}) VALUES#{apius.ledger.map { |key, val| usl_val(key, val) }.join(',')}"
    end

    # SQL value formatting methods with improved safety
    def quote(value)
      return 'null' if value.nil? || value.empty?

      "'#{value.gsub('\'', "''")}'" # Escape single quotes
    end

    def numeric(value)
      "CAST('#{value}' AS NUMERIC)"
    end

    def integer(value)
      Integer(value).to_s
    rescue StandardError
      'null'
    end

    # @param [Hash] htx transacao norml etherscan
    # @return [String] valores formatados netht (norml parte1)
    def netht_val(htx)
      txr = htx[:txreceipt_status]
      inp = htx[:input]
      cta = htx[:contractAddress]
      "(#{[
        integer(htx[:blockNumber]),
        integer(htx[:timeStamp]),
        quote(htx[:hash]),
        integer(htx[:nonce]),
        quote(htx[:blockHash]),
        integer(htx[:transactionIndex]),
        quote(htx[:from]),
        quote(htx[:to]),
        quote(htx[:iax]),
        numeric(htx[:value]),
        numeric(htx[:gas]),
        numeric(htx[:gasPrice]),
        numeric(htx[:gasUsed]),
        integer(htx[:isError]),
        txr.empty? ? 'null' : integer(txr),
        inp.empty? ? 'null' : quote(inp),
        cta.empty? ? 'null' : quote(cta),
        integer(ops.dig(:h, htx[:blockNumber]) || 0)
      ].join(',')})"
    end

    # @param [Hash] htx transacao internas etherscan
    # @return [String] valores formatados nethi (internas parte1)
    def nethi_val(htx)
      cta = htx[:contractAddress]
      inp = htx[:input]
      tid = htx[:traceId]
      txr = htx[:errCode]
      "(#{[
        integer(htx[:blockNumber]),
        integer(htx[:timeStamp]),
        quote(htx[:hash]),
        quote(htx[:from]),
        quote(htx[:to]),
        quote(htx[:iax]),
        numeric(htx[:value]),
        cta.empty? ? 'null' : quote(cta),
        inp.empty? ? 'null' : quote(inp),
        quote(htx[:type]),
        numeric(htx[:gas]),
        numeric(htx[:gasUsed]),
        tid.empty? ? 'null' : quote(tid),
        integer(htx[:isError]),
        txr.empty? ? 'null' : integer(txr)
      ].join(',')})"
    end

    # @param [Hash] htx transacao block etherscan
    # @return [String] valores formatados nethi (block parte1)
    def nethp_val(htx)
      "(#{[integer(htx[:blockNumber]), integer(htx[:timeStamp]), numeric(htx[:blockReward]), quote(htx[:iax])].join(',')})"
    end

    # @param [Hash] htx transacao withdrawals etherscan
    # @return [String] valores formatados nethi (withdrawals parte1)
    def nethw_val(htx)
      "(#{[
        integer(htx[:withdrawalIndex]),
        integer(htx[:validatorIndex]),
        quote(htx[:address]),
        numeric(htx[:amount]),
        integer(htx[:blockNumber]),
        integer(htx[:timestamp])
      ].join(',')})"
    end

    # @param [Hash] hkx token event etherscan
    # @return [String] valores formatados nethk (token parte1)
    def nethk_val(htx)
      inp = htx[:input]
      cta = htx[:contractAddress]
      "(#{[
        integer(htx[:blockNumber]),
        integer(htx[:timeStamp]),
        quote(htx[:hash]),
        integer(htx[:nonce]),
        quote(htx[:blockHash]),
        integer(htx[:transactionIndex]),
        quote(htx[:from]),
        quote(htx[:to]),
        quote(htx[:iax]),
        numeric(htx[:value]),
        quote(htx[:tokenName]),
        quote(htx[:tokenSymbol]),
        integer(htx[:tokenDecimal]),
        numeric(htx[:gas]),
        numeric(htx[:gasPrice]),
        numeric(htx[:gasUsed]),
        inp.empty? ? 'null' : quote(inp),
        cta.empty? ? 'null' : quote(cta),
        integer(ops.dig(:h, htx[:blockNumber]) || 0)
      ].join(',')})"
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
