# frozen_string_literal: true

require('google/cloud/bigquery')
require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar bigquery
  class Bigquery
    BD = 'hernanirvaz.coins'
    FO = File.expand_path("~/#{File.basename($PROGRAM_NAME)}.log")
    TB = {
      netht: %w[txhash blocknumber timestamp nonce blockhash transactionindex axfrom axto iax value gas gasprice gasused iserror txreceipt_status input contractaddress dias],
      hetht: %i[hash blockNumber timeStamp nonce blockHash transactionIndex from to iax value gas gasPrice gasUsed isError txreceipt_status input contractAddress],
      nethi: %w[txhash blocknumber timestamp axfrom axto iax value contractaddress input type gas gasused traceid iserror errcode dias],
      hethi: %i[hash blockNumber timeStamp from to iax value contractAddress input type gas gasUsed traceId isError errCode],
      nethp: %w[blocknumber timestamp blockreward iax dias],
      hethp: %i[blockNumber timeStamp blockReward iax],
      nethw: %w[withdrawalindex validatorindex address amount blocknumber timestamp dias],
      hethw: %i[withdrawalIndex validatorIndex address amount blockNumber timeStamp],
      nethk: %w[txhash blocknumber timestamp nonce blockhash transactionindex axfrom axto iax value tokenname tokensymbol tokendecimal gas gasprice gasused input contractaddress dias],
      hethk: %i[hash blockNumber timeStamp nonce blockHash transactionIndex from to iax value tokenName tokenSymbol tokenDecimal gas gasPrice gasUsed input contractAddress],
      neost: %w[gseq aseq bnum time contract action acfrom acto iax amount moeda memo dias],
      heost: %i[global_action_seq account_action_seq block_num block_time account name from to iax quantity moe memo],
      cdet: %w[txid time tp user btc eur dtc dias],
      hdet: %i[trade_id successfully_finished_at type username btc eur trade_marked_as_paid_at],
      cdel: %w[txid time tp add moe qt fee],
      hdel: %i[nxid time tp add moe qtd fee],
      cust: %w[txid ordertxid pair time type ordertype price cost fee vol margin misc ledgers dias],
      hust: %i[txid ordertxid pair time type ordertype price cost fee vol margin misc ledgers],
      cusl: %w[txid refid time type aclass asset amount fee],
      husl: %i[txid refid time type aclass asset amount fee]
    }.freeze
    # para testes bigquery
    TL = {
      ins: 'INSERT',
      exo: false,
      est: '', # ' limit 226',
      esi: '', # ' limit 22',
      esp: '', # ' limit 72',
      esw: '', # ' limit 2320',
      esk: '', # ' limit 20',
      gmt: '', # ' limit 1091',
      ust: '', # ' limit 182',
      usl: '', # ' limit 448',
      det: '', # ' limit 27',
      del: '' # ' limit 16'
    }.freeze

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

    # insere dados novos kraken/bitcoinde/etherscan/greymass no bigquery
    def ptudo
      puts("#{tct} #{pus}, #{pde}, #{peth}, #{peos}")
    end

    # insere dados novos kraken/etherscan no bigquery
    def pwkrk
      puts("#{tct} #{pus}, #{peth}")
    end

    # insere dados novos etherscan no bigquery
    def pweth
      puts("#{tct} #{peth}")
    end

    # insere dados novos etherscan no bigquery (output to file)
    def pceth
      File.open(FO, mode: 'a') { |o| o.puts("#{tct} #{pethc}") }
    end

    private

    # @return [String] linhas & tabelas afetadas
    def peth
      dml_out(apies, %w[ETH], %i[netht nethi nethp nethw nethk])
    end

    # @return [String] linhas & tabelas afetadas
    def pethc
      dml_out(apiesc, %w[ETH], %i[netht nethi nethp nethw nethk])
    end

    # @return [String] linhas & tabelas afetadas
    def pus
      dml_out(apius, %w[KRAKEN], %i[cust cusl])
    end

    # @return [String] linhas & tabelas afetadas
    def pde
      dml_out(apide, %w[BITCOINDE], %i[cdet cdel])
    end

    # @return [String] linhas & tabelas afetadas
    def peos
      dml_out(apigm, %w[EOS], %i[neost])
    end

    # cria job bigquery & verifica execucao
    # @param cmd (see sql)
    # @return [Boolean] job ok?
    def job?(cmd)
      @job = api.query_job(cmd, priority: 'BATCH')
      job.wait_until_done!
      return true unless job.failed?

      puts("BigQuery Error: #{job.error['message']}")
      false
    end

    # cria Structured Query Language (SQL) job bigquery
    # @param [String] cmd comando SQL a executar
    # @param [String] res resultado quando SQL tem erro
    # @return [Google::Cloud::Bigquery::Data] resultado do SQL
    def sql(cmd, res = [])
      @sqr = job?(cmd) ? job.data : res
    end

    # cria Data Manipulation Language (DML) job bigquery
    # @param cmd (see sql)
    # @return [Integer] numero linhas afetadas
    def dml(cmd)
      job?(cmd) ? job.num_dml_affected_rows : 0
    end

    # @return [Etherscan] API blockchain ETH
    def apiesg(prx)
      Etherscan.new(
        {
          wb: sql("SELECT * FROM #{BD}.wet#{prx[-1]} ORDER BY ax"),
          nt: sql("SELECT * FROM #{BD}.#{prx}t#{TL[:est]}"),
          ni: sql("SELECT * FROM #{BD}.#{prx}i#{TL[:esi]}"),
          np: sql("SELECT * FROM #{BD}.#{prx}p#{TL[:esp]}"),
          nw: sql("SELECT * FROM #{BD}.#{prx}w#{TL[:esw]}"),
          nk: sql("SELECT * FROM #{BD}.#{prx}k#{TL[:esk]}")
        },
        ops
      )
    end

    # @return [Etherscan] API blockchain ETH
    def apies
      @apies ||= apiesg('netb')
    end

    # @return [Etherscan] API blockchain ETH (cron)
    def apiesc
      @apiesc ||= apiesg('netc')
    end

    # @return [Greymass] API blockchain EOS
    def apigm
      @apigm ||= Greymass.new({wb: sql("select * from #{BD}.weos ORDER BY ax"), nt: sql("select * from #{BD}.neosx#{TL[:gmt]}")}, ops)
    end

    # @return [Kraken] API exchange kraken
    def apius
      @apius ||= Kraken.new({sl: sql("select * from #{BD}.cuss").first, nt: sql("select * from #{BD}.cust#{TL[:ust]}"), nl: sql("select * from #{BD}.cusl#{TL[:usl]}")}, ops)
    end

    # @return [Bitcoinde] API exchange bitcoinde
    def apide
      @apide ||= Bitcoinde.new({sl: sql("select * from #{BD}.cdes").first, nt: sql("select * from #{BD}.cdet#{TL[:det]}"), nl: sql("select * from #{BD}.cdel#{TL[:del]}")}, ops)
    end

    # @return [String] comando insert SQL formatado
    def ins_sql(tbl, lin)
      # para testes bigquery
      if TL[:exo]
        exl = lin.map { |i| send("#{tbl}_val", i)[1..-2] }
        exi = exl.map { |f| f.split(',').first }.join(',')
        exo = "SELECT #{TB[tbl].join(',')} FROM #{BD}.#{tbl} WHERE #{TB[tbl].first} IN (#{exi}) union all select "
        puts(exo + exl.join(' union all select ') + ' order by 1')
      end
      "#{TL[:ins]} #{BD}.#{tbl} (#{TB[tbl].join(',')}) VALUES #{lin.map { |i| send("#{tbl}_val", i) }.join(',')}"
    end

    # @return [String] relatorio execucao dml
    def dml_out(src, str, ltb)
      str.concat(
        ltb.filter_map do |i|
          novx = src.send("nov#{i}")
          next if novx.empty?

          format(' %<n>i %<t>s', n: dml(ins_sql(i, novx)), t: "#{i}")
        end
      )
      str.join
    end

    # @return [String] escapes SQL user input strings
    def escape_sql(value)
      value.gsub("'", "''").gsub('\\', '\\\\')
    end

    # @return [String] SQL string formatting
    def fqt(value)
      value.nil? || value.empty? ? 'null' : "'#{value}'"
    end

    # @return [String] SQL string formatting with improved safety
    def fqe(value)
      value.to_s.empty? ? 'null' : "'#{escape_sql(value.to_s)}'"
    end

    # @return [String] SQL numeric formatting
    def fnm(value)
      "CAST(#{value.to_d} AS NUMERIC)"
    rescue StandardError
      'CAST(0 AS NUMERIC)'
    end

    # @return [String] SQL integer formatting
    def fin(value)
      value.to_i.to_s
    end

    # @return [String] SQL timestamp formatting
    def fts(value)
      value.nil? ? 'null' : "DATETIME(TIMESTAMP('#{value.iso8601}'))"
    end

    # @return [String] formated SQL values for tables
    def fvals(hsh, kys, idx = nil)
      vls =
        kys.map do |k|
          case k
          when :account_action_seq, :block_num, :blockNumber, :global_action_seq, :isError, :nonce, :timeStamp, :tokenDecimal, :transactionIndex, :nxid, :txreceipt_status, :validatorIndex, :withdrawalIndex then fin(hsh[k])
          when :amount, :btc, :cost, :fee, :gas, :gasPrice, :gasUsed, :margin, :price, :quantity, :value, :vol, :eur, :blockReward, :qtd then fnm(hsh[k])
          when :block_time, :successfully_finished_at, :time, :trade_marked_as_paid_at then fts(hsh[k])
          when :memo, :input, :misc then fqe(hsh[k])
          else fqt(hsh[k])
          end
        end
      vls << fin(ops.dig(:h, hsh[idx].to_s)) if idx
      "(#{vls.join(',')})"
    end

    # @param [Hash] htx transacao norml etherscan
    # @return [String] valores formatados netht
    def netht_val(htx)
      fvals(htx, TB[:hetht], :hash)
    end

    # @param [Hash] htx transacao internas etherscan
    # @return [String] valores formatados nethi
    def nethi_val(htx)
      fvals(htx, TB[:hethi], :hash)
    end

    # @param [Hash] htx transacao block etherscan
    # @return [String] valores formatados nethp
    def nethp_val(htx)
      fvals(htx, TB[:hethp], :blockNumber)
    end

    # @param [Hash] htx transacao withdrawals etherscan
    # @return [String] valores formatados nethw
    def nethw_val(htx)
      fvals(htx, TB[:hethw], :withdrawalIndex)
    end

    # @param [Hash] htx token etherscan
    # @return [String] valores formatados nethk
    def nethk_val(htx)
      fvals(htx, TB[:hethk], :hash)
    end

    # @param [Hash] htx ledger greymass
    # @return [String] valores formatados neost
    def neost_val(htx)
      fvals(htx, TB[:heost], :global_action_seq)
    end

    # @param [Hash] htx trades bitcoinde
    # @return [String] valores formatados cdet
    def cdet_val(htx)
      fvals(htx, TB[:hdet], :trade_id)
    end

    # @param [Hash] htx ledger (deposits + withdrawals) bitcoinde
    # @return [String] valores formatados cdel
    def cdel_val(htx)
      fvals(htx, TB[:hdel])
    end

    # @param [Hash] htx trades kraken
    # @return [String] valores formatados cust
    def cust_val(htx)
      fvals(htx.merge(ledgers: apius.uskl.select { |o| o[:refid] == htx[:txid] }.map { |t| t[:txid] }.join(',')), TB[:hust], :txid)
    end

    # @param [Hash] htx ledger kraken
    # @return [String] valores formatados cusl
    def cusl_val(htx)
      fvals(htx, TB[:husl])
    end
  end
end
