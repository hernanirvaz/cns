# frozen_string_literal: true

require('google/cloud/bigquery')
require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  BD = 'hernanirvaz.coins'

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
    # @return [Bigquery] API bigquery & kraken/bitcoinde/paymium/therock
    def initialize(pop)
      # usa env GOOGLE_APPLICATION_CREDENTIALS para obter credentials
      # @see https://cloud.google.com/bigquery/docs/authentication/getting-started
      @api = Google::Cloud::Bigquery.new
      @ops = pop
    end

    # situacao completa entre kraken/bitcoinde/paymium/therock & bigquery
    def mostra_tudo
      apius.mostra_resumo
      apide.mostra_resumo
      apifr.mostra_resumo
      apimt.mostra_resumo
      apies.mostra_resumo
      apigm.mostra_resumo
    end

    # insere (caso existam) transacoes novas do kraken/bitcoinde/paymium/therock no bigquery
    def processa_tudo
      processa_us
      processa_de
      processa_fr
      processa_mt
      processa_eos
      processa_eth
    end

    # insere transacoes kraken novas nas tabelas ust (trades), usl (ledger)
    def processa_us
      puts(format("%<n>2i TRADES KRAKEN INSERIDAS #{BD}.ust", n: apius.trades.empty? ? 0 : dml(ust_ins)))
      puts(format("%<n>2i LEDGER KRAKEN INSERIDAS #{BD}.usl", n: apius.ledger.empty? ? 0 : dml(usl_ins)))
    end

    # insere transacoes bitcoinde novas nas tabelas det (trades), del (ledger)
    def processa_de
      puts(format("%<n>2i TRADES BITCOINDE INSERIDAS #{BD}.det", n: apide.trades.empty? ? 0 : dml(det_ins)))
      puts(format("%<n>2i LEDGER BITCOINDE INSERIDAS #{BD}.del", n: apide.ledger.empty? ? 0 : dml(del_ins)))
    end

    # insere transacoes paymium novas na tabela fr (ledger)
    def processa_fr
      puts(format("%<n>2i LEDGER PAYMIUM INSERIDAS #{BD}.fr", n: apifr.ledger.empty? ? 0 : dml(frl_ins)))
    end

    # insere transacoes paymium novas na tabela mt (ledger)
    def processa_mt
      puts(format("%<n>2i LEDGER THEROCK INSERIDAS #{BD}.mt", n: apimt.ledger.empty? ? 0 : dml(mtl_ins)))
    end

    # insere transacoes novas na tabela eos
    def processa_eos
      puts(format("%<n>2i LINHAS INSERIDAS #{BD}.eos ", n: apigm.novax.count.positive? ? dml(eost_ins) : 0))
    end

    # insere transacoes novas nas tabelas etht (trx normais), ethk (trx token)
    def processa_eth
      puts(format("%<n>2i LINHAS INSERIDAS #{BD}.etht", n: apies.novtx.count.positive? ? dml(etht_ins) : 0))
      puts(format("%<n>2i LINHAS INSERIDAS #{BD}.ethk", n: apies.novkx.count.positive? ? dml(ethk_ins) : 0))
    end

    # cria job bigquery & verifica execucao
    #
    # @param cmd (see sql)
    # @return [Boolean] job ok?
    def job?(cmd)
      @job = api.query_job(cmd)
      @job.wait_until_done!
      puts(@job.error['message']) if @job.failed?
      @job.failed?
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
  end
end
