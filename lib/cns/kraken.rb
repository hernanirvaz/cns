# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes trades/ledger do kraken
  class Kraken
    # @return [Apius] API kraken
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :bqd
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes trades & ledger?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    # @return [Kraken] API kraken - obter saldos & transacoes trades e ledger
    def initialize(dad, pop)
      @api = Apice.new
      @bqd = dad
      @ops = pop
    end

    # @return [Hash] trades kraken novos
    def trades
      @trades ||= exd[:kt].select { |k, _| kyt.include?(k) }
    end

    # @return [Hash] ledger kraken novos
    def ledger
      @ledger ||= exd[:kl].select { |k, _| kyl.include?(k) }
    end

    # @return [String] texto saldos & transacoes & ajuste dias
    def mostra_resumo
      puts("\nKRAKEN\ntipo                 kraken              bigquery")
      exd[:sl].each { |k, v| puts(formata_saldos(k, v)) }
      mostra_totais

      mostra_trades
      mostra_ledger
      return if trades.empty?

      puts("\nstring ajuste dias dos trades\n-h=#{kyt.map { |e| "#{e}:0" }.join(' ')}")
    end

    # @return [Hash] dados exchange kraken - saldos & transacoes trades e ledger
    def exd
      @exd ||= {
        sl: api.account_us,
        kt: api.trades_us,
        kl: api.ledger_us
      }
    end

    # @return [Array<String>] lista txid dos trades novos
    def kyt
      @kyt ||= exd[:kt].keys - (ops[:t] ? [] : bqd[:nt].map { |e| e[:txid].to_sym })
    end

    # @return [Array<String>] lista txid dos ledger novos
    def kyl
      @kyl ||= exd[:kl].keys - (ops[:t] ? [] : bqd[:nl].map { |e| e[:txid].to_sym })
    end

    # @example (see Apice#account_us)
    # @param [String] moe codigo kraken da moeda
    # @param [BigDecimal] sal saldo kraken da moeda
    # @return [String] texto formatado saldos
    def formata_saldos(moe, sal)
      t = bqd[:sl][moe.downcase.to_sym].to_d
      format(
        '%<mo>-5.5s %<kr>21.9f %<bq>21.9f %<ok>3.3s',
        mo: moe.upcase,
        kr: sal.to_d,
        bq: t,
        ok: t == sal.to_d ? 'OK' : 'NOK'
      )
    end

    # @example (see Apice#trades_us)
    # @param (see Bigquery#ust_val1)
    # @return [String] texto formatado trade
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

    # @example (see Apice#ledger_us)
    # @param (see Bigquery#usl_val)
    # @return [String] texto formatado ledger
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

    # @return [String] texto totais numero de transacoes
    def mostra_totais
      a = exd[:kt].count
      b = bqd[:nt].count
      c = exd[:kl].count
      d = bqd[:nl].count

      puts("TRADES #{format('%<a>20i %<b>21i %<o>3.3s', a: a, b: b, o: a == b ? 'OK' : 'NOK')}")
      puts("LEDGER #{format('%<c>20i %<d>21i %<o>3.3s', c: c, d: d, o: c == d ? 'OK' : 'NOK')}")
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
