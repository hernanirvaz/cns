# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar saldos & transacoes trades e ledger do bitcoinde
  class Bitcoinde
    # @return [Apius] API bitcoinde
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
    # @return [Bitcoinde] API bitcoinde - obter saldos & transacoes trades e ledger
    def initialize(dad, pop)
      # API bitcoinde base
      @api = Apide.new
      @dbq = dad
      @ops = pop
    end

    # @return [Hash] dados exchange bitcoinde - saldos & transacoes trades e ledger
    def exd
      @exd ||= {
        sl: api.account,
        tt: api.trades,
        tl: api.deposits + api.withdrawals
      }
    end

    # @return [Array<String>] lista txid de transacoes trades
    def kyt
      @kyt ||= exd[:tt].map { |h| h[:trade_id] }.flatten - (ops[:t] ? [] : dbq[:nt].map { |e| e[:txid] })
    end

    # @return [Array<Integer>] lista txid de transacoes ledger
    def kyl
      @kyl ||= exd[:tl].map { |h| h[:lgid] }.flatten - (ops[:t] ? [] : dbq[:nl].map { |e| e[:txid] })
    end

    # @return [Hash] transacoes trades
    def trades
      @trades ||= exd[:tt].select { |h| kyt.include?(h[:trade_id]) }
    end

    # @return [Hash] transacoes ledger
    def ledger
      @ledger ||= exd[:tl].select { |h| kyl.include?(h[:lgid]) }
    end

    # @example (see Apide#account)
    # @param [String] moe codigo bitcoinde da moeda
    # @param [Hash] hsx saldo bitcoinde da moeda
    # @return [String] texto formatado saldos (bitcoinde)
    def formata_saldos(moe, hsx)
      b = dbq[:sl][moe.downcase.to_sym].to_d
      e = hsx[:total_amount].to_d
      format(
        '%<mo>-5.5s %<ex>21.9f %<bq>21.9f %<ok>3.3s',
        mo: moe.upcase,
        ex: e,
        bq: b,
        ok: e == b ? 'OK' : 'NOK'
      )
    end

    # @example (see Apide#trades)
    # @param (see Bigquery#det_val1)
    # @return [String] texto formatado transacao trade
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

    # @example (see Apide#deposit_hash)
    # @example (see Apide#withdrawal_hash)
    # @param (see Bigquery#del_val)
    # @return [String] texto formatado transacao ledger
    def formata_ledger(hlx)
      format(
        '%<ky>6i %<dt>19.19s %<ty>-10.10s %<mo>-3.3s %<pr>19.8f %<vl>18.8f',
        ky: hlx[:lgid],
        dt: hlx[:time],
        ty: hlx[:tp],
        mo: hlx[:qtxt].upcase,
        pr: hlx[:qt].to_d,
        vl: hlx[:fee].to_d
      )
    end

    # @return [String] texto saldos & transacoes & ajuste dias
    def mostra_resumo
      puts("\nBITCOINDE\nmoeda       saldo bitcoinde        saldo bigquery")
      exd[:sl].each { |k, v| puts(formata_saldos(k, v)) }

      mostra_trades
      mostra_ledger
      return unless trades.count.positive?

      puts("\nstring ajuste dias dos trades\n-h=#{kyt.map { |e| "#{e}:0" }.join(' ')}")
    end

    # @return [String] texto transacoes trades
    def mostra_trades
      return unless ops[:v] && trades.count.positive?

      puts("\ntrades data       hora     dt criacao tipo  par      ---------------qtd -----eur")
      trades.sort { |a, b| Time.parse(b[:successfully_finished_at]) <=> Time.parse(a[:successfully_finished_at]) }
            .each { |h| puts(formata_trades(h)) }
    end

    # @return [String] texto transacoes ledger
    def mostra_ledger
      return unless ops[:v] && ledger.count.positive?

      puts("\nledger data       hora     tipo       moe ---------quantidade -------------custo")
      ledger.sort { |a, b| b[:time] <=> a[:time] }.each { |h| puts(formata_ledger(h)) }
    end
  end
end
