# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes trades/ledger do bitcoinde
  class Bitcoinde
    # @return [Apius] API bitcoinde
    # @return [Array<Hash>] todos os dados bigquery
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :api, :bqd, :ops

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

    # @return [String] texto saldos & transacoes & ajuste dias
    def mresumo
      puts("\nBITCOINDE\ntipo              bitcoinde              bigquery")
      ded[:sl].sort.each { |k, v| puts(fos(k, v)) }
      mtotais

      mtrades
      mledger
      return if novcdet.empty?

      puts("\nstring ajuste dias dos trades\n-h=#{novcdet.sort_by { |i| -i[:srx] }.map { |o| "#{o[:trade_id]}:0" }.join(' ')}")
    end

    private

    # mosta contadores transacoes
    def mtotais
      vtt = ded[:tt].count
      vnt = bqd[:nt].count
      vtl = ded[:tl].count
      vnl = bqd[:nl].count

      puts("TRADES #{format('%<a>20i %<b>21i %<o>3.3s', a: vtt, b: vnt, o: vtt == vnt ? 'OK' : 'NOK')}")
      puts("LEDGER #{format('%<c>20i %<d>21i %<o>3.3s', c: vtl, d: vnl, o: vtl == vnl ? 'OK' : 'NOK')}")
    end

    # mosta transacoes trades
    def mtrades
      return unless ops[:v] && novcdet.any?

      puts("\ntrades data       hora     dt criacao tipo  par                     btc      eur")
      novcdet.sort_by { |i| -i[:srx] }.each { |o| puts(fot(o)) }
    end

    # mosta transacoes ledger
    def mledger
      return unless ops[:v] && novcdel.any?

      puts("\nledger data       hora     tipo       moe          quantidade              custo")
      novcdel.sort_by { |i| -i[:srx] }.each { |o| puts(fol(o)) }
    end

    # @param [String] moe codigo bitcoinde da moeda
    # @param [Hash] hsx saldo bitcoinde da moeda
    # @return [String] texto formatado saldos
    def fos(moe, hsx)
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

    # @param [Hash] htn trades bitcoinde
    # @return [String] texto formatado trade
    def fot(htx)
      format(
        '%<ky>-6.6s %<dt>19.19s %<dp>10.10s %<ty>-5.5s %<mo>-8.8s %<vl>18.8f %<co>8.2f',
        ky: htx[:trade_id],
        dt: htx[:successfully_finished_at].strftime('%F %T'),
        dp: htx[:trade_marked_as_paid_at].strftime('%F'),
        ty: htx[:type],
        mo: htx[:trading_pair],
        vl: htx[:btc],
        co: htx[:eur]
      )
    end

    # @param [Hash] htn ledger bitcoinde
    # @return [String] texto formatado ledger
    def fol(hlx)
      format(
        '%<ky>6i %<dt>19.19s %<ty>-10.10s %<mo>-3.3s %<pr>19.8f %<vl>18.8f',
        ky: hlx[:nxid],
        dt: hlx[:time].strftime('%F %T'),
        ty: hlx[:tp],
        mo: hlx[:moe],
        pr: hlx[:qtd],
        vl: hlx[:fee]
      )
    end

    # @return [Boolean] mostra todas/novas transacoes
    def show_all?
      ops[:t] || false
    end

    # @param [Hash] itm recursos bitcoinde
    # @return [Hash<BigDecimal>] moedas & sados
    def pdea(itm)
      itm.select { |k, _| EM.include?(k) }.transform_values { |o| o.merge(total_amount: o[:total_amount].to_d) }
    end

    # @param [Object] val time bitcoinde
    # @return [Time] processa time (somtimes is string)
    def ptm(val)
      val.is_a?(String) ? Time.parse(val) : val
    end

    # @param [Hash] itm transacao bitcoinde
    # @return [Hash] transaccao filtrada
    def pdes(key, itm)
      tym = ptm(itm[key])
      itm.merge(srx: tym.to_i, key => tym)
    end

    # @param [Array<Hash>] htx trade bitcoinde
    # @return [Array<Hash>] transaccao filtrada
    def pdet(htx)
      htx.map do |t|
        pdes(:successfully_finished_at, t).merge(
          trade_marked_as_paid_at: ptm(t[:trade_marked_as_paid_at]),
          username: t[:trading_partner_information][:username],
          btc: t[:type] == 'buy' ? t[:amount_currency_to_trade_after_fee].to_d : -1 * t[:amount_currency_to_trade].to_d,
          eur: t[:volume_currency_to_pay_after_fee].to_d,
          trading_pair: t[:trading_pair].upcase
        )
      end
    end

    # @param [Array<Hash>] hlx ledger bitcoinde
    # @return [Array<Hash>] transaccao filtrada
    def pdel(hlx)
      hlx.map { |t| pdes(:time, t) }
    end

    # @return [Hash] dados exchange bitcoinde - saldos & trades & deposits & withdrawals
    def ded
      @ded ||= {sl: pdea(api.account_de), tt: pdet(api.trades_de), tl: pdel(api.deposits_de + api.withdrawals_de)}
    end

    # @return [Array<String>] indices trades bigquery
    def bqkyt
      @bqkyt ||= show_all? ? [] : bqd[:nt].map { |t| t[:txid] }
    end

    # @return [Array<Integer>] indices ledger bigquery
    def bqkyl
      @bqkyl ||= show_all? ? [] : bqd[:nl].map { |l| l[:txid] }
    end

    # @return [Array<String>] lista txid trades novos
    def kyt
      @kyt ||= ded[:tt].map { |t| t[:trade_id] } - bqkyt
    end

    # @return [Array<Integer>] lista nxid ledger novos
    def kyl
      @kyl ||= ded[:tl].map { |t| t[:nxid] } - bqkyl
    end

    # @return [Array<Hash>] lista trades novos bitcoinde
    def novcdet
      @novcdet ||= ded[:tt].select { |o| kyt.include?(o[:trade_id]) }
    end

    # @return [Array<Hash>] lista ledgers (deposits + withdrawals) novos bitcoinde
    def novcdel
      @novcdel ||= ded[:tl].select { |o| kyl.include?(o[:nxid]) }
    end
  end
end
