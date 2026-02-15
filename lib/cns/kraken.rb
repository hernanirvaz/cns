# frozen_string_literal: true

require('bigdecimal/util')
require('memoist')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes trades/ledger do kraken
  class Kraken
    extend Memoist

    # @return [Array<Hash>] todos os dados bigquery
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :bqd, :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes trades & ledger?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    def initialize(dad, pop)
      @bqd = dad
      @ops = pop.transform_keys(&:to_sym)
    end

    # mosta resumo saldos & transacoes & ajuste dias
    def mresumo
      puts("\nKRAKEN\ntipo                 kraken              bigquery")
      exd[:sl].transform_keys { |k| k.downcase.to_sym }.sort_by { |_, v| v }.each { |k, v| puts(fos(k, v)) }
      mtotais

      mtrades
      mledger
      return if novxt.empty?

      puts("\nstring ajuste dias dos trades\n-h=#{novxt.sort_by { |i| -i[:srx] }.map { |o| "#{o[:txid]}:0" }.join(' ')}")
    end

    # @return [Hash] ledgers exchange kraken
    def uskl
      exd[:kl]
    end

    private

    # mosta contadores transacoes
    def mtotais
      vkt, vnt = exd[:kt].count, bqd[:nt].count
      vkl, vnl = exd[:kl].count, bqd[:nl].count

      puts("TRADES #{format('%<a>20i %<b>21i %<o>3.3s', a: vkt, b: vnt, o: vkt == vnt ? 'OK' : 'NOK')}") if vkt.positive?
      puts("LEDGER #{format('%<c>20i %<d>21i %<o>3.3s', c: vkl, d: vnl, o: vkl == vnl ? 'OK' : 'NOK')}")
    end

    # mosta transacoes trades
    def mtrades
      return unless ops[:v] && novxt.any?

      puts("\ntrade  data       hora     tipo       par         preco     volume         custo")
      novxt.sort_by { |i| -i[:srx] }.each { |o| puts(fot(o)) }
    end

    # mosta transacoes ledger
    def mledger
      return unless ops[:v] && novxl.any?

      puts("\nledger data       hora     tipo       moeda        quantidade              custo")
      novxl.sort_by { |i| -i[:srx] }.each { |o| puts(fol(o)) }
    end

    # @param [Symbol] moe codigo kraken da moeda
    # @return [String] moeda formatada
    def normalize_moe(moe)
      {xeth: 'ETH', xxbt: 'BTC', zeur: 'EUR'}[moe] || moe.to_s.upcase
    end

    # @param [Symbol] moe (see normalize_moe)
    # @param [BigDecimal] sal saldo kraken da moeda
    # @return [String] texto formatado saldos
    def fos(moe, sal)
      vbq = (bqd[:sl]&.fetch(moe, nil) || 0).to_d
      format(
        '%<mo>-5.5s %<kr>21.9f %<bq>21.9f %<ok>3.3s',
        mo: normalize_moe(moe),
        kr: sal,
        bq: vbq,
        ok: vbq.round(9) == sal.round(9) ? 'OK' : 'NOK'
      )
    end

    # @param [Hash] htn trades kraken
    # @return [String] texto formatado trade
    def fot(htx)
      format(
        '%<ky>-6.6s %<dt>19.19s %<ty>-10.10s %<mo>-8.8s %<pr>8.2f %<vl>10.4f %<co>13.2f',
        ky: htx[:txid],
        dt: htx[:time].strftime('%F %T'),
        ty: "#{htx[:type]}/#{htx[:ordertype]}",
        mo: htx[:pair],
        pr: htx[:price],
        vl: htx[:vol],
        co: htx[:cost]
      )
    end

    # @param [Hash] hln ledger kraken
    # @return [String] texto formatado ledger
    def fol(hlx)
      format(
        '%<ky>-6.6s %<dt>19.19s %<ty>-10.10s %<mo>-4.4s %<pr>18.7f %<vl>18.7f',
        ky: hlx[:txid],
        dt: hlx[:time].strftime('%F %T'),
        ty: hlx[:type],
        mo: hlx[:asset],
        pr: hlx[:amount],
        vl: hlx[:fee]
      )
    end

    # @return [Boolean] mostra todas/novas transacoes
    def show_all?
      ops[:t] || false
    end

    # @param [Hash] itm recursos kraken
    # @return [Hash<BigDecimal>] moedas & sados
    def pusa(itm)
      itm.select { |k, _| %i[XETH ZEUR XXBT].include?(k) }.transform_values { |v| v.to_d }
    end

    # @param [Array<Hash>] htx trades kraken
    # @return [Array<Hash>] transaccoes filtradas
    def pust(htx)
      htx.map { |t| t.merge(pair: t[:pair].upcase, price: t[:price].to_d, vol: t[:vol].to_d, cost: t[:cost].to_d) }
    end

    # @param [Array<Hash>] hlx ledgers kraken
    # @return [Array<Hash>] transaccoes filtradas
    def pusl(hlx)
      hlx.map { |t| t.merge(asset: t[:asset].upcase, amount: t[:amount].to_d, fee: t[:fee].to_d) }
    end

    # Lazy kraken API Initialization decorated with rate limiting logic
    # @return [Kraken] API - obter saldos & transacoes trades e ledger
    memoize def api
      Apice.new.tap do |t|
        # Rate limiting to this specific instance (0.5s in Kraken)
        t.define_singleton_method(:rcrl) do |c, u, **o|
          sleep(@lapi - Time.now + 0.5) if @lapi && Time.now - @lapi < 0.5
          super(c, u, **o)
          @lapi = Time.now
        end
      end
    end

    # @return [Hash] dados exchange kraken - saldos & transacoes trades e ledger
    memoize def exd
      # unix timestamp para obter transacoes 24x60x60 = 86400 segundos
      tsp = ops&.[](:d)&.positive? ? Integer(Time.now - (ops[:d] * 86_400)) : nil
      {sl: pusa(api.account_us), kt: pust(api.trades_us(tsp)), kl: pusl(api.ledger_us(tsp))}
    end

    # @return [Array<String>] indices trades bigquery
    memoize def bqkyt
      show_all? ? [] : bqd[:nt].map { |t| t[:txid] }
    end

    # @return [Array<Integer>] indices ledger bigquery
    memoize def bqkyl
      show_all? ? [] : bqd[:nl].map { |l| l[:txid] }
    end

    # @return [Array<String>] lista txid trades novos
    memoize def exkyt
      exd[:kt].map { |t| t[:txid] } - bqkyt
    end

    # @return [Array<String>] lista txid ledger novos
    memoize def exkyl
      exd[:kl].map { |t| t[:txid] } - bqkyl
    end

    # @return [Array<Hash>] trades novos kraken
    memoize def novxt
      exd[:kt].select { |o| exkyt.include?(o[:txid]) }
    end

    # @return [Array<Hash>] ledgers novos kraken
    memoize def novxl
      exd[:kl].select { |o| exkyl.include?(o[:txid]) }
    end
  end
end
