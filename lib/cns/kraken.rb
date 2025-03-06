# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes trades/ledger do kraken
  class Kraken
    # @return [Apius] API kraken
    # @return [Array<Hash>] todos os dados bigquery
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :api, :bqd, :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes trades & ledger?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    # @return [Kraken] API kraken - obter saldos & transacoes trades e ledger
    def initialize(dad, pop)
      @api = Apice.new
      @bqd = dad
      @ops = pop.transform_keys(&:to_sym)
    end

    # mosta resumo saldos & transacoes & ajuste dias
    def mresumo
      puts("\nKRAKEN\ntipo                 kraken              bigquery")
      usd[:sl].sort.each { |key, val| puts(fos(key, val)) }
      mtotais

      mtrades
      mledger
      return if novcust.empty?

      puts("\nstring ajuste dias dos trades\n-h=#{novcust.sort_by { |i| -i[:srx] }.map { |o| "#{o[:txid]}:0" }.join(' ')}")
    end

    # @return [Hash] ledgers exchange kraken
    def uskl
      usd[:kl]
    end

    private

    # mosta contadores transacoes
    def mtotais
      vkt = usd[:kt].count
      vnt = bqd[:nt].count
      vkl = usd[:kl].count
      vnl = bqd[:nl].count

      puts("TRADES #{format('%<a>20i %<b>21i %<o>3.3s', a: vkt, b: vnt, o: vkt == vnt ? 'OK' : 'NOK')}")
      puts("LEDGER #{format('%<c>20i %<d>21i %<o>3.3s', c: vkl, d: vnl, o: vkl == vnl ? 'OK' : 'NOK')}")
    end

    # mosta transacoes trades
    def mtrades
      return unless ops[:v] && novcust.any?

      puts("\ntrade  data       hora     tipo       par         preco     volume         custo")
      novcust.sort_by { |i| -i[:srx] }.each { |o| puts(fot(o)) }
    end

    # mosta transacoes ledger
    def mledger
      return unless ops[:v] && novcusl.any?

      puts("\nledger data       hora     tipo       moeda        quantidade              custo")
      novcusl.sort_by { |i| -i[:srx] }.each { |o| puts(fol(o)) }
    end

    # @param [String] moe codigo kraken da moeda
    # @param [BigDecimal] sal saldo kraken da moeda
    # @return [String] texto formatado saldos
    def fos(moe, sal)
      vbq = (bqd[:sl]&.fetch(moe.downcase.to_sym, nil) || 0).to_d
      format(
        '%<mo>-5.5s %<kr>21.9f %<bq>21.9f %<ok>3.3s',
        mo: moe.upcase,
        kr: sal,
        bq: vbq,
        ok: vbq == sal ? 'OK' : 'NOK'
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
      itm.select { |k, _| EM.include?(k) }.transform_values { |v| v.to_d }
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

    # @return [Hash] dados exchange kraken - saldos & transacoes trades e ledger
    def usd
      @usd ||= {sl: pusa(api.account_us), kt: pust(api.trades_us), kl: pusl(api.ledger_us)}
    end

    # @return [Array<String>] indices trades bigquery
    def bqkyt
      @bqkyt ||= show_all? ? [] : bqd[:nt].map { |t| t[:txid] }
    end

    # @return [Array<String>] indices ledger bigquery
    def bqkyl
      @bqkyl ||= show_all? ? [] : bqd[:nl].map { |l| l[:txid] }
    end

    # @return [Array<String>] lista txid trades novos
    def kyt
      @kyt ||= usd[:kt].map { |t| t[:txid] } - bqkyt
    end

    # @return [Array<String>] lista txid ledger novos
    def kyl
      @kyl ||= usd[:kl].map { |t| t[:txid] } - bqkyl
    end

    # @return [Array<Hash>] trades novos kraken
    def novcust
      @novcust ||= usd[:kt].select { |o| kyt.include?(o[:txid]) }
    end

    # @return [Array<Hash>] ledgers novos kraken
    def novcusl
      @novcusl ||= usd[:kl].select { |o| kyl.include?(o[:txid]) }
    end
  end
end
