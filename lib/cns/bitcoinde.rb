# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes trades/ledger do bitcoinde
  class Bitcoinde
    # @return [Apius] API bitcoinde
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
    # @return [Bitcoinde] API bitcoinde - obter saldos & transacoes trades e ledger
    def initialize(dad, pop)
      @api = Apice.new
      @bqd = dad
      @ops = pop
    end

    # @return [Array<Hash>] lista trades bitcoinde novos
    def trades
      @trades ||= exd[:tt].select { |obj| kyt.include?(obj[:trade_id]) }
    end

    # @return [Array<Hash>] lista ledger (deposits + withdrawals) bitcoinde novos
    def ledger
      @ledger ||= exd[:tl].select { |obj| kyl.include?(obj[:txid]) }
    end

    # @return [String] texto saldos & transacoes & ajuste dias
    def mostra_resumo
      puts("\nBITCOINDE\ntipo              bitcoinde              bigquery")
      exd[:sl].each { |key, val| puts(formata_saldos(key, val)) }
      mostra_totais

      mostra_trades
      mostra_ledger
      return if trades.empty?

      puts("\nstring ajuste dias dos trades\n-h=#{kyt.map { |obj| "#{obj}:0" }.join(' ')}")
    end

    # @return [Hash] dados exchange bitcoinde - saldos & trades & deposits & withdrawals
    def exd
      @exd ||= {
        sl: api.account_de,
        tt: api.trades_de,
        tl: api.deposits_de + api.withdrawals_de
      }
    end

    # @return [Array<String>] lista txid dos trades novos
    def kyt
      @kyt ||= exd[:tt].map { |oex| oex[:trade_id] }.flatten - (ops[:t] ? [] : bqd[:nt].map { |obq| obq[:txid] })
    end

    # @return [Array<Integer>] lista txid dos ledger novos
    def kyl
      @kyl ||= exd[:tl].map { |oex| oex[:txid] }.flatten - (ops[:t] ? [] : bqd[:nl].map { |obq| obq[:txid] })
    end

    # @example (see Apice#account_de)
    # @param [String] moe codigo bitcoinde da moeda
    # @param [Hash] hsx saldo bitcoinde da moeda
    # @return [String] texto formatado saldos
    def formata_saldos(moe, hsx)
      vbq = bqd[:sl][moe.downcase.to_sym].to_d
      vex = hsx[:total_amount].to_d
      format(
        '%<mo>-5.5s %<ex>21.9f %<bq>21.9f %<ok>3.3s',
        mo: moe.upcase,
        ex: vex,
        bq: vbq,
        ok: vex == vbq ? 'OK' : 'NOK'
      )
    end

    # @example (see Apice#trades_de)
    # @param (see Bigquery#det_val1)
    # @return [String] texto formatado trade
    def formata_trades(htx)
      format(
        '%<ky>-6.6s %<dt>19.19s %<dp>10.10s %<ty>-5.5s %<mo>-8.8s %<vl>18.8f %<co>8.2f',
        ky: htx[:trade_id],
        dt: Time.parse(htx[:successfully_finished_at]),
        dp: Time.parse(htx[:trade_marked_as_paid_at]),
        ty: htx[:type],
        mo: htx[:trading_pair].upcase,
        vl: htx[:amount_currency_to_trade].to_d,
        co: htx[:volume_currency_to_pay].to_d
      )
    end

    # @example (see Apice#deposits_unif_de)
    # @example (see Apice#withdrawals_unif_de)
    # @param (see Bigquery#del_val)
    # @return [String] texto formatado ledger
    def formata_ledger(hlx)
      format(
        '%<ky>6i %<dt>19.19s %<ty>-10.10s %<mo>-3.3s %<pr>19.8f %<vl>18.8f',
        ky: hlx[:txid],
        dt: hlx[:time],
        ty: hlx[:tp],
        mo: hlx[:moe].upcase,
        pr: hlx[:qt].to_d,
        vl: hlx[:fee].to_d
      )
    end

    # @return [String] texto numero de transacoes
    def mostra_totais
      vtt = exd[:tt].count
      vnt = bqd[:nt].count
      vtl = exd[:tl].count
      vnl = bqd[:nl].count

      puts("TRADES #{format('%<a>20i %<b>21i %<o>3.3s', a: vtt, b: vnt, o: vtt == vnt ? 'OK' : 'NOK')}")
      puts("LEDGER #{format('%<c>20i %<d>21i %<o>3.3s', c: vtl, d: vnl, o: vtl == vnl ? 'OK' : 'NOK')}")
    end

    # @return [String] texto transacoes trades
    def mostra_trades
      return unless ops[:v] && !trades.empty?

      puts("\ntrades data       hora     dt criacao tipo  par                     qtd      eur")
      trades
        .sort { |ant, prx| Time.parse(prx[:successfully_finished_at]) <=> Time.parse(ant[:successfully_finished_at]) }
        .each { |obj| puts(formata_trades(obj)) }
    end

    # @return [String] texto transacoes ledger
    def mostra_ledger
      return unless ops[:v] && !ledger.empty?

      puts("\nledger data       hora     tipo       moe          quantidade              custo")
      ledger.sort { |ant, prx| prx[:time] <=> ant[:time] }.each { |obj| puts(formata_ledger(obj)) }
    end
  end
end
