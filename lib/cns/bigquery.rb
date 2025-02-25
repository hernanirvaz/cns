# frozen_string_literal: true

require('google/cloud/bigquery')
require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  BD = 'hernanirvaz.coins'
  FO = File.expand_path("~/#{File.basename($PROGRAM_NAME)}.log")
  EM = %i[EOS XETH ZEUR btc eth]
  TB = {
    i: %w[blocknumber timestamp txhash axfrom axto iax value contractaddress input type gas gasused traceid iserror errcode dias],
    p: %w[blocknumber timestamp blockreward iax dias],
    w: %w[withdrawalindex validatorindex address amount blocknumber timestamp dias],
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
      @ops = pop.transform_keys(&:to_sym)
    end

    # mostra situacao completa entre kraken/bitcoinde/paymium/therock/etherscan/greymass & bigquery
    def mtudo
      apius.mresumo
      apide.mresumo
      apies.mresumo
      apigm.mresumo
    end

    # mostra situacao completa entre kraken/etherscan & bigquery
    def mskrk
      apius.mresumo
      apies.mresumo
    end

    # mostra situacao completa entre etherscan & bigquery
    def mseth
      apies.mresumo_simples
    end

    # @return [String] texto inicial transacoes
    def tct
      Time.now.strftime('TRANSACOES  %Y-%m-%d %H:%M:%S')
    end

    # insere (caso existam) dados novos kraken/bitcoinde/paymium/therock/etherscan/greymass no bigquery
    def ptudo
      puts("#{tct} #{pus}, #{pde}, #{peth}, #{peos}")
    end

    # insere (caso existam) dados novos kraken/etherscan no bigquery
    def pwkrk
      puts("#{tct} #{pus}, #{peth}")
    end

    # insere (caso existam) dados novos etherscan no bigquery
    def pweth
      puts("#{tct} #{peth}")
    end

    # insere (caso existam) dados novos etherscan no bigquery (output to file)
    def pceth
      File.open(FO, mode: 'a') { |out| out.puts("#{tct} #{pethc}") }
    end

    private

    # insere transacoes blockchain novas nas tabelas netht (norml), nethi (internas), nethp (block), nethw (withdrawals), nethk (token)
    #
    # @return [String] linhas & tabelas afetadas
    def peth
      tabelas_out(apies, %w[ETH], %i[t i p w k], 'neth')
    end

    # insere transacoes blockchain novas nas tabelas netht (norml), nethi (internas), nethp (block), nethw (withdrawals), nethk (token)
    #
    # @return [String] linhas & tabelas afetadas
    def pethc
      tabelas_out(apiesc, %w[ETH], %i[t i p w k], 'neth')
    end

    # insere transacoes exchange kraken novas nas tabelas ust (trades), usl (ledger)
    #
    # @return [String] linhas & tabelas afetadas
    def pus
      tabelas_cus(apius, %w[KRAKEN], %i[cust cusl])
    end

    # insere transacoes exchange bitcoinde novas nas tabelas det (trades), del (ledger)
    #
    # @return [String] linhas & tabelas afetadas
    def pde
      tabelas_out(apide, %w[BITCOINDE], %i[cdet cdel])
    end

    # insere transacoes blockchain novas na tabela eos
    #
    # @return [String] linhas & tabelas afetadas
    def peos
      tabelas_out(apigm, %w[EOS], %i[neost])
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

    def apiesg(prx)
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
      @apies ||= apiesg('netb')
    end

    # @return [Etherscan] API blockchain ETH
    def apiesc
      @apiesc ||= apiesg('netc')
    end

    # @return [Greymass] API blockchain EOS
    def apigm
      @apigm ||= Greymass.new({ wb: sql("select * from #{BD}.weos order by ax"), nt: sql("select * from #{BD}.neosx") }, ops)
    end

    # @return [Kraken] API exchange kraken
    def apius
      @apius ||= Kraken.new({ sl: sql("select * from #{BD}.cuss").first, nt: sql("select * from #{BD}.cust"), nl: sql("select * from #{BD}.cusl") }, ops)
    end

    # @return [Bitcoinde] API exchange bitcoinde
    def apide
      @apide ||= Bitcoinde.new({ sl: sql("select * from #{BD}.cdes").first, nt: sql("select * from #{BD}.cdet"), nl: sql("select * from #{BD}.cdel") }, ops)
    end

    def tabelas_cus(src, str, ltb, prx = '')
      ltb.each do |itm|
        novx = src.send("nov#{prx}#{itm}")
        next if novx.empty?

        # puts(insert_cus(prx, itm, novx))
        str << format(' %<n>i %<t>s', n: dml(insert_cus(prx, itm, novx)), t: "#{prx}#{itm}")
      end
      str.join
    end

    def tabelas_out(src, str, ltb, prx = '')
      ltb.each do |itm|
        novx = src.send("nov#{prx}#{itm}")
        next if novx.empty?

        # puts(insert_out(prx, itm, novx))
        str << format(' %<n>i %<t>s', n: dml(insert_out(prx, itm, novx)), t: "#{prx}#{itm}")
      end
      str.join
    end

    # @return [String] comando insert SQL formatado
    def insert_cus(prx, tbl, lin)
      "INSERT #{BD}.#{prx}#{tbl} (#{TB[tbl].join(',')}) VALUES #{lin.map { |key, val| send("#{prx}#{tbl}_val", key, val) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado
    def insert_out(prx, tbl, lin)
      "INSERT #{BD}.#{prx}#{tbl} (#{TB[tbl].join(',')}) VALUES #{lin.map { |itm| send("#{prx}#{tbl}_val", itm) }.join(',')}"
    end

    # SQL value formatting methods with improved safety
    def fqt(value)
      value.nil? || value.empty? ? 'null' : "'#{value}'"
    end

    def fnm(value)
      "CAST(#{value.to_d} AS NUMERIC)"
    rescue StandardError
      'CAST(0 AS NUMERIC)'
    end

    def fin(value)
      Integer(value).to_s
    rescue StandardError
      '0'
    end

    def ftm(sec)
      "PARSE_DATETIME('%s', '#{sec.round}')"
    end

    def fts(value)
      "DATETIME(TIMESTAMP('#{value.iso8601}'))"
    end

    # @param [Hash] htx transacao norml etherscan
    # @return [String] valores formatados netht (norml parte1)
    def netht_val(htx)
      vls = [
        fin(htx[:blockNumber]),
        fin(htx[:timeStamp]),
        fqt(htx[:hash]),
        fin(htx[:nonce]),
        fqt(htx[:blockHash]),
        fin(htx[:transactionIndex]),
        fqt(htx[:from]),
        fqt(htx[:to]),
        fqt(htx[:iax]),
        fnm(htx[:value]),
        fnm(htx[:gas]),
        fnm(htx[:gasPrice]),
        fnm(htx[:gasUsed]),
        fin(htx[:isError]),
        fin(htx[:txreceipt_status]),
        fqt(htx[:input]),
        fqt(htx[:contractAddress]),
        fin(ops.dig(:h, htx[:hash]))
      ]
      "(#{vls.join(',')})"
    end

    # @param [Hash] htx transacao internas etherscan
    # @return [String] valores formatados nethi (internas parte1)
    def nethi_val(htx)
      vls = [
        fin(htx[:blockNumber]),
        fin(htx[:timeStamp]),
        fqt(htx[:hash]),
        fqt(htx[:from]),
        fqt(htx[:to]),
        fqt(htx[:iax]),
        fnm(htx[:value]),
        fqt(htx[:contractAddress]),
        fqt(htx[:input]),
        fqt(htx[:type]),
        fnm(htx[:gas]),
        fnm(htx[:gasUsed]),
        fqt(htx[:traceId]),
        fin(htx[:isError]),
        fqt(htx[:errCode]),
        fin(ops.dig(:h, htx[:hash]))
      ]
      "(#{vls.join(',')})"
    end

    # @param [Hash] htx transacao block etherscan
    # @return [String] valores formatados nethi (block parte1)
    def nethp_val(htx)
      vls = [fin(htx[:blockNumber]), fin(htx[:timeStamp]), fnm(htx[:blockReward]), fqt(htx[:iax]), fin(ops.dig(:h, htx[:blockNumber]))]
      "(#{vls.join(',')})"
    end

    # @param [Hash] htx transacao withdrawals etherscan
    # @return [String] valores formatados nethi (withdrawals parte1)
    def nethw_val(htx)
      vls = [
        fin(htx[:withdrawalIndex]),
        fin(htx[:validatorIndex]),
        fqt(htx[:address]),
        fnm(htx[:amount]),
        fin(htx[:blockNumber]),
        fin(htx[:timeStamp]),
        fin(ops.dig(:h, htx[:withdrawalIndex]))
      ]
      "(#{vls.join(',')})"
    end

    # @param [Hash] hkx token event etherscan
    # @return [String] valores formatados nethk (token parte1)
    def nethk_val(htx)
      vls = [
        fin(htx[:blockNumber]),
        fin(htx[:timeStamp]),
        fqt(htx[:hash]),
        fin(htx[:nonce]),
        fqt(htx[:blockHash]),
        fin(htx[:transactionIndex]),
        fqt(htx[:from]),
        fqt(htx[:to]),
        fqt(htx[:iax]),
        fnm(htx[:value]),
        fqt(htx[:tokenName]),
        fqt(htx[:tokenSymbol]),
        fin(htx[:tokenDecimal]),
        fnm(htx[:gas]),
        fnm(htx[:gasPrice]),
        fnm(htx[:gasUsed]),
        fqt(htx[:input]),
        fqt(htx[:contractAddress]),
        fin(ops.dig(:h, htx[:hash]))
      ]
      "(#{vls.join(',')})"
    end

    # @example (see Apibc#ledger_gm)
    # @param [Hash] hlx ledger greymass
    # @return [String] valores formatados para insert eos (parte1)
    def neost_val(htx)
      # act = htx[:action_trace][:act]
      # dat = act[:data]
      # qtd = dat[:quantity].to_s
      # str = dat[:memo].inspect
      vls = [
        fin(htx[:global_action_seq]),
        fin(htx[:account_action_seq]),
        fin(htx[:block_num]),
        fts(htx[:block_time]),
        fqt(htx[:account]),
        fqt(htx[:name]),
        fqt(htx[:from]),
        fqt(htx[:to]),
        fqt(htx[:iax]),
        fnm(htx[:quantity]),
        fqt(htx[:moe]),
        fqt(htx[:memo]),
        fin(ops.dig(:h, htx[:itx]))
      ]
      "(#{vls.join(',')})"
    end

    # @param [Hash] htx trade bitcoinde
    # @return [String] valores formatados det (trades parte1)
    def cdet_val(htx)
      vls = [
        fqt(htx[:trade_id]),
        fts(htx[:successfully_finished_at]),
        fqt(htx[:type]),
        fqt(htx[:trading_partner_information][:username]),
        fnm(htx[:type] == 'buy' ? htx[:amount_currency_to_trade_after_fee] : "-#{htx[:amount_currency_to_trade]}"),
        fnm(htx[:volume_currency_to_pay_after_fee]),
        fts(htx[:trade_marked_as_paid_at]),
        fin(ops.dig(:h, htx[:trade_id]))
      ]
      "(#{vls.join(',')})"
    end

    # @param [Hash] hlx ledger (deposits + withdrawals) bitcoinde
    # @return [String] valores formatados del (ledger)
    def cdel_val(htx)
      vls = [
        fin(htx[:txid]),
        fts(htx[:time]),
        fqt(htx[:tp]),
        fqt(htx[:add]),
        fqt(htx[:moe]),
        fnm(htx[:tp] == 'withdrawal' ? "-#{htx[:qt]}" : "#{htx[:qt]}"),
        fnm(htx[:fee])
      ]
      "(#{vls.join(',')})"
    end

    # @param [String] idx identificador transacao
    # @param [Hash] htx trade kraken
    # @return [String] valores formatados ust (trades parte1)
    def cust_val(idx, htx)
      # gets ledgers related to this trade
      ldg = apius.exd[:kl].select { |_, obj| obj[:refid] == idx.to_s }.keys.join(',')
      vls = [
        fqt(idx),
        fqt(htx[:ordertxid]),
        fqt(htx[:pair]),
        fts(htx[:time]),
        fqt(htx[:type]),
        fqt(htx[:ordertype]),
        fnm(htx[:price]),
        fnm(htx[:cost]),
        fnm(htx[:fee]),
        fnm(htx[:vol]),
        fnm(htx[:margin]),
        fqt(htx[:misc]),
        fqt(ldg),
        fin(ops.dig(:h, idx))
      ]
      "(#{vls.join(',')})"
    end

    # @param idx (see ust_val)
    # @param [Hash] hlx ledger kraken
    # @return [String] valores formatados usl (ledger)
    def cusl_val(idx, hlx)
      vls = [fqt(idx), fqt(hlx[:refid]), fts(hlx[:time]), fqt(hlx[:type]), fqt(hlx[:aclass]), fqt(hlx[:asset]), fnm(hlx[:amount]), fnm(hlx[:fee])]
      "(#{vls.join(',')})"
    end
  end
end
