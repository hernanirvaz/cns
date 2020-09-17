# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar saldos & transacoes trades e ledger
  class Kraken
    # @return [Apius] API kraken
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :dbq
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes trades & ledger?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    # @return [Kraken] API kraken - obter saldos & transacoes trades e ledger
    def initialize(dad, pop)
      # API kraken base
      @api = Apius.new
      @dbq = dad
      @ops = pop
    end

    # @return [Hash] dados exchange kraken - saldos & transacoes trades e ledger
    def exd
      @exd ||= {
        sl: api.account,
        kt: api.trades,
        kl: api.ledger
      }
    end

    # @return [Array<String>] lista txid de transacoes trades
    def kyt
      @kyt ||= exd[:kt].keys - (ops[:t] ? [] : dbq[:nt].map { |e| e[:txid].to_sym })
    end

    # @return [Array<String>] lista txid de transacoes ledger
    def kyl
      @kyl ||= exd[:kl].keys - (ops[:t] ? [] : dbq[:nl].map { |e| e[:txid].to_sym })
    end

    # @return [Hash] transacoes trades
    def trades
      @trades ||= exd[:kt].select { |k, _| kyt.include?(k) }
    end

    # @return [Hash] transacoes ledger
    def ledger
      @ledger ||= exd[:kl].select { |k, _| kyl.include?(k) }
    end

    # @example (see Apius#account)
    # @param [String] moe codigo kraken da moeda
    # @param [BigDecimal] sal saldo kraken da moeda
    # @return [String] texto formatado saldos (kraken/bigquery) & iguais/ok/nok?
    def formata_saldos(moe, sal)
      t = dbq[:sl][moe.downcase.to_sym].to_d
      format(
        '%<mo>-5.5s %<kr>21.9f %<bq>21.9f %<ok>3.3s',
        mo: moe.upcase,
        kr: sal,
        bq: t,
        ok: t == sal ? 'OK' : 'NOK'
      )
    end

    # @example (see Apius#trades)
    # @param (see Bigquery#ust_val1)
    # @return [String] texto formatado transacao trade
    def formata_trades(idx, htx)
      format(
        '%<ky>-6.6s %<dt>19.19s %<ty>-10.10s %<mo>-8.8s %<pr>8.2f %<vl>15.7f %<co>8.2f',
        ky: idx,
        dt: Time.at(htx[:time]),
        ty: "#{htx[:type]}/#{htx[:ordertype]}",
        mo: htx[:pair].upcase,
        pr: htx[:price].to_d,
        vl: htx[:vol].to_d,
        co: htx[:cost].to_d
      )
    end

    # @example (see Apius#ledger)
    # @param (see Bigquery#usl_val)
    # @return [String] texto formatado transacao ledger
    def formata_ledger(idx, hlx)
      format(
        '%<ky>-6.6s %<dt>19.19s %<ty>-10.10s %<mo>-4.4s %<pr>18.7f %<vl>18.7f',
        ky: idx,
        dt: Time.at(hlx[:time]),
        ty: hlx[:type],
        mo: hlx[:asset].upcase,
        pr: hlx[:amount].to_d,
        vl: hlx[:fee].to_d
      )
    end

    # @return [String] texto saldos & transacoes & ajuste dias
    def mostra_resumo
      puts("\nKRAKEN\nmoeda          saldo kraken        saldo bigquery")
      exd[:sl].each { |k, v| puts(formata_saldos(k, v.to_d)) }

      mostra_trades
      mostra_ledger
      return unless trades.count.positive?

      puts("\nstring ajuste dias dos trades\n-h=#{kyt.map { |e| "#{e}:0" }.join(' ')}")
    end

    # @return [String] texto transacoes trades
    def mostra_trades
      return unless ops[:v] && trades.count.positive?

      puts("\ntrade  data       hora     tipo       par      ---preco ---------volume ---custo")
      trades.sort { |a, b| b[1][:time] <=> a[1][:time] }.each { |k, v| puts(formata_trades(k, v)) }
    end

    # @return [String] texto transacoes ledger
    def mostra_ledger
      return unless ops[:v] && ledger.count.positive?

      puts("\nledger data       hora     tipo       moeda -------quantidade -------------custo")
      ledger.sort { |a, b| b[1][:time] <=> a[1][:time] }.each { |k, v| puts(formata_ledger(k, v)) }
    end
  end
end
