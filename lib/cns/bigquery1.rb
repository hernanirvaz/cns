# frozen_string_literal: true

require('google/cloud/bigquery')
require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  BD = 'hernanirvaz.coins'

  # (see Bigquery)
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

    # mostra situacao completa entre kraken/bitcoinde/paymium/therock/etherscan/greymass/beaconchain & bigquery
    def mostra_tudo
      apius.mostra_resumo
      apide.mostra_resumo
      apifr.mostra_resumo
      apimt.mostra_resumo
      apies.mostra_resumo
      apigm.mostra_resumo
      apibc.mostra_resumo
    end

    # insere (caso existam) dados novos kraken/bitcoinde/paymium/therock/etherscan/greymass/beaconchain no bigquery
    def processa_tudo
      processa_us
      processa_de
      processa_frmt
      processa_eth
      processa_eos
      processa_bc
    end

    private

    # insere transacoes exchange kraken novas nas tabelas ust (trades), usl (ledger)
    def processa_us
      puts(format("%<n>4i TRADES KRAKEN INSERIDAS #{BD}.ust", n: apius.trades.empty? ? 0 : dml(ust_ins)))
      puts(format("%<n>4i LEDGER KRAKEN INSERIDAS #{BD}.usl", n: apius.ledger.empty? ? 0 : dml(usl_ins)))
    end

    # insere transacoes exchange bitcoinde novas nas tabelas det (trades), del (ledger)
    def processa_de
      puts(format("%<n>4i TRADES BITCOINDE INSERIDAS #{BD}.det", n: apide.trades.empty? ? 0 : dml(det_ins)))
      puts(format("%<n>4i LEDGER BITCOINDE INSERIDAS #{BD}.del", n: apide.ledger.empty? ? 0 : dml(del_ins)))
    end

    # insere transacoes exchange paymium/therock  novas na tabela fr/mt (ledger)
    def processa_frmt
      puts(format("%<n>4i LEDGER PAYMIUM INSERIDAS #{BD}.fr", n: apifr.ledger.empty? ? 0 : dml(frl_ins)))
      puts(format("%<n>4i LEDGER THEROCK INSERIDAS #{BD}.mt", n: apimt.ledger.empty? ? 0 : dml(mtl_ins)))
    end

    # insere transacoes blockchain novas nas tabelas etht (norml), ethk (token)
    def processa_eth
      puts(format("%<n>4i TRANSACOES ETH INSERIDAS #{BD}.etht", n: apies.novtx.empty? ? 0 : dml(etht_ins)))
      puts(format("%<n>4i TOKEN EVENTS ETH INSERIDAS #{BD}.ethk", n: apies.novkx.empty? ? 0 : dml(ethk_ins)))
    end

    # insere transacoes blockchain novas na tabela eos
    def processa_eos
      puts(format("%<n>4i TRANSACOES EOS INSERIDAS #{BD}.eos ", n: apigm.novax.empty? ? 0 : dml(eost_ins)))
    end

    # insere historico sados novos na tabela eth2bh
    def processa_bc
      # puts(format("%<n>4i ATTESTATIONS INSERIDAS #{BD}.eth2at", n: apibc.novtx.empty? ? 0 : dml(eth2at_ins)))
      # puts(format("%<n>4i PROPOSALS INSERIDAS #{BD}.eth2pr", n: apibc.novkx.empty? ? 0 : dml(eth2pr_ins)))
      puts(format("%<n>4i BALANCES INSERIDOS #{BD}.eth2bh", n: apibc.nov.empty? ? 0 : dml(eth2bh_ins)))
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
  end
end
