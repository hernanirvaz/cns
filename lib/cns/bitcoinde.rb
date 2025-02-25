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
      @ops = pop.transform_keys(&:to_sym)
    end

    # @return [Hash] dados exchange bitcoinde - saldos & trades & deposits & withdrawals
    def exd
      @exd ||= { sl: pdea(api.account_de), tt: pdet(api.trades_de), tl: pdel(api.deposits_de + api.withdrawals_de) }
    end

    # @return [Array<Hash>] lista trades bitcoinde novos
    def novcdet
      @novcdet ||= exd[:tt].select { |obj| kyt.include?(obj[:trade_id]) }
    end

    # @return [Array<Hash>] lista ledger (deposits + withdrawals) bitcoinde novos
    def novcdel
      @novcdel ||= exd[:tl].select { |obj| kyl.include?(obj[:txid]) }
    end

    # @return [String] texto saldos & transacoes & ajuste dias
    def mresumo
      puts("\nBITCOINDE\ntipo              bitcoinde              bigquery")
      exd[:sl].sort.each { |key, val| puts(formata_saldos(key, val)) }
      mtotais

      mtrades
      mledger
      return if novcdet.empty?

      puts("\nstring ajuste dias dos trades\n-h=#{kyt.map { |obj| "#{obj}:0" }.join(' ')}")
    end

    private

    def show_all?
      ops[:t] || false
    end

    def bqkyt
      show_all? ? [] : (bqd[:nt]&.map { |t| t[:txid] } || [])
    end

    def bqkyl
      show_all? ? [] : (bqd[:nl]&.map { |l| l[:txid] } || [])
    end

    # @return [Array<String>] lista txid dos trades novos
    def kyt
      @kyt ||= exd[:tt].map { |oex| oex[:trade_id] } - bqkyt
    end

    # @return [Array<Integer>] lista txid dos ledger novos
    def kyl
      @kyl ||= exd[:tl].map { |oex| oex[:txid] } - bqkyl
    end

    # @param [String] moe codigo bitcoinde da moeda
    # @param [Hash] hsx saldo bitcoinde da moeda
    # @return [String] texto formatado saldos
    def formata_saldos(moe, hsx)
      vbq = bqd[:sl][moe.downcase.to_sym].to_d
      vex = hsx[:total_amount]
      format(
        '%<mo>-5.5s %<ex>21.9f %<bq>21.9f %<ok>3.3s',
        mo: moe.upcase,
        ex: vex,
        bq: vbq,
        ok: vex == vbq ? 'OK' : 'NOK'
      )
    end

    # @param (see Bigquery#det_val1)
    # @return [String] texto formatado trade
    def formata_trades(htx)
      format(
        '%<ky>-6.6s %<dt>19.19s %<dp>10.10s %<ty>-5.5s %<mo>-8.8s %<vl>18.8f %<co>8.2f',
        ky: htx[:trade_id],
        dt: htx[:successfully_finished_at].strftime('%F %T'),
        dp: htx[:trade_marked_as_paid_at].strftime('%F'),
        ty: htx[:type],
        mo: htx[:trading_pair],
        vl: htx[:amount_currency_to_trade],
        co: htx[:volume_currency_to_pay]
      )
    end

    # @param (see Bigquery#del_val)
    # @return [String] texto formatado ledger
    def formata_ledger(hlx)
      format(
        '%<ky>6i %<dt>19.19s %<ty>-10.10s %<mo>-3.3s %<pr>19.8f %<vl>18.8f',
        ky: hlx[:txid],
        dt: hlx[:time].strftime('%F %T'),
        ty: hlx[:tp],
        mo: hlx[:moe],
        pr: hlx[:qt],
        vl: hlx[:fee]
      )
    end

    # @return [String] texto numero de transacoes
    def mtotais
      vtt = exd[:tt].count
      vnt = bqd[:nt].count
      vtl = exd[:tl].count
      vnl = bqd[:nl].count

      puts("TRADES #{format('%<a>20i %<b>21i %<o>3.3s', a: vtt, b: vnt, o: vtt == vnt ? 'OK' : 'NOK')}")
      puts("LEDGER #{format('%<c>20i %<d>21i %<o>3.3s', c: vtl, d: vnl, o: vtl == vnl ? 'OK' : 'NOK')}")
    end

    # @return [String] texto transacoes trades
    def mtrades
      return unless ops[:v] && !novcdet.empty?

      puts("\ntrades data       hora     dt criacao tipo  par                     qtd      eur")
      novcdet.sort_by { |itm| -itm[:srx] }.each { |obj| puts(formata_trades(obj)) }
    end

    # @return [String] texto transacoes ledger
    def mledger
      return unless ops[:v] && !novcdel.empty?

      puts("\nledger data       hora     tipo       moe          quantidade              custo")
      novcdel.sort_by { |itm| -itm[:srx] }.each { |obj| puts(formata_ledger(obj)) }
    end

    # Processa os trades para garantir que as datas estejam no formato correto
    def pdea(itm)
      itm.select { |ky1, _| EM.include?(ky1) }.transform_values { |o| o.merge(total_amount: o[:total_amount].to_d) }
    end

    # Processa os trades para garantir que as datas estejam no formato correto
    def pdet(itm)
      itm.map do |t|
        t.merge(
          successfully_finished_at: srx = ptm(t[:successfully_finished_at]),
          srx: Integer(srx),
          trade_marked_as_paid_at: ptm(t[:trade_marked_as_paid_at]),
          amount_currency_to_trade: t[:amount_currency_to_trade].to_d,
          volume_currency_to_pay: t[:volume_currency_to_pay].to_d,
          trading_pair: t[:trading_pair].upcase
        )
      end
    end

    # Processa os ledger entries para garantir que as datas estejam no formato correto
    def pdel(itm)
      itm.map { |t| t.merge(time: srx = ptm(t[:time]), srx: Integer(srx), qt: t[:qt].to_d, fee: t[:fee].to_d, moe: t[:moe].upcase) }
    end

    # Processa time field somtimes is string
    def ptm(itm)
      itm.is_a?(String) ? Time.parse(itm) : itm
    end
  end
end
