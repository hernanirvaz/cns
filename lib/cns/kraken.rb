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
      @ops = pop.transform_keys(&:to_sym)
    end

    # @return [Hash] dados exchange kraken - saldos & transacoes trades e ledger
    def exd
      @exd ||= {sl: pusa(api.account_us), kt: pust(api.trades_us), kl: pusl(api.ledger_us)}
    end

    # @return [String] texto saldos & transacoes & ajuste dias
    def mresumo
      puts("\nKRAKEN\ntipo                 kraken              bigquery")
      exd[:sl].sort.each { |key, val| puts(formata_saldos(key, val)) }
      mtotais

      mtrades
      mledger
      return if novcust.empty?

      puts("\nstring ajuste dias dos trades\n-h=#{novcust.sort_by { |_, v| -v[:srx] }.map { |k, _v| "#{k}:0" }.join(' ')}")
    end

    private

    def show_all?
      ops[:t] || false
    end

    def bqkyt
      @bqkyt ||= show_all? ? [] : (bqd[:nt]&.map { |t| t[:txid].to_sym } || [])
    end

    def bqkyl
      @bqkyl ||= show_all? ? [] : (bqd[:nl]&.map { |l| l[:txid].to_sym } || [])
    end

    # @return [Array<String>] lista txid dos trades novos
    def kyt
      @kyt ||= exd[:kt].keys - bqkyt
    end

    # @return [Array<String>] lista txid dos ledger novos
    def kyl
      @kyl ||= exd[:kl].keys - bqkyl
    end

    # @return [Hash] trades kraken novos
    def novcust
      @novcust ||= exd[:kt].slice(*kyt)
    end

    # @return [Hash] ledger kraken novos
    def novcusl
      @novcusl ||= exd[:kl].slice(*kyl)
    end

    # @example (see Apice#account_us)
    # @param [String] moe codigo kraken da moeda
    # @param [BigDecimal] sal saldo kraken da moeda
    # @return [String] texto formatado saldos
    def formata_saldos(moe, sal)
      vbq = bqd[:sl][moe.downcase.to_sym].to_d
      format(
        '%<mo>-5.5s %<kr>21.9f %<bq>21.9f %<ok>3.3s',
        mo: moe.upcase,
        kr: sal,
        bq: vbq,
        ok: vbq == sal ? 'OK' : 'NOK'
      )
    end

    # @example (see Apice#trades_us)
    # @param (see Bigquery#ust_val1)
    # @return [String] texto formatado trade
    def formata_trades(idx, htx)
      format(
        '%<ky>-6.6s %<dt>19.19s %<ty>-10.10s %<mo>-8.8s %<pr>8.2f %<vl>10.4f %<co>13.2f',
        ky: idx,
        dt: htx[:time].strftime('%F %T'),
        ty: "#{htx[:type]}/#{htx[:ordertype]}",
        mo: htx[:pair],
        pr: htx[:price],
        vl: htx[:vol],
        co: htx[:cost]
      )
    end

    # @example (see Apice#ledger_us)
    # @param (see Bigquery#usl_val)
    # @return [String] texto formatado ledger
    def formata_ledger(idx, hlx)
      format(
        '%<ky>-6.6s %<dt>19.19s %<ty>-10.10s %<mo>-4.4s %<pr>18.7f %<vl>18.7f',
        ky: idx,
        dt: hlx[:time].strftime('%F %T'),
        ty: hlx[:type],
        mo: hlx[:asset],
        pr: hlx[:amount],
        vl: hlx[:fee]
      )
    end

    # @return [String] texto totais numero de transacoes
    def mtotais
      vkt = exd[:kt].count
      vnt = bqd[:nt].count
      vkl = exd[:kl].count
      vnl = bqd[:nl].count

      puts("TRADES #{format('%<a>20i %<b>21i %<o>3.3s', a: vkt, b: vnt, o: vkt == vnt ? 'OK' : 'NOK')}")
      puts("LEDGER #{format('%<c>20i %<d>21i %<o>3.3s', c: vkl, d: vnl, o: vkl == vnl ? 'OK' : 'NOK')}")
    end

    # @return [String] texto transacoes trades
    def mtrades
      return unless ops[:v] && novcust.any?

      puts("\ntrade  data       hora     tipo       par         preco     volume         custo")
      novcust.sort_by { |_, v| -v[:srx] }.each { |k, t| puts(formata_trades(k, t)) }
    end

    # @return [String] texto transacoes ledger
    def mledger
      return unless ops[:v] && novcusl.any?

      puts("\nledger data       hora     tipo       moeda        quantidade              custo")
      novcusl.sort_by { |_, v| -v[:srx] }.each { |k, t| puts(formata_ledger(k, t)) }
    end

    # Processa accounts para garantir formato correto
    def pusa(itm)
      itm.select { |k, _| EM.include?(k) }.transform_values { |v| v.to_d }
    end

    # Processa campos comuns para garantir formato correto
    def pusk(itm)
      itm.map do |k, v|
        t = Integer(v[:time])
        [k, v.merge(txid: k.to_s, srx: t, time: Time.at(t))]
      end.to_h
    end

    # Processa trades para garantir formato correto
    def pust(itm)
      pusk(itm).transform_values { |t| t.merge(pair: t[:pair].upcase, price: t[:price].to_d, vol: t[:vol].to_d, cost: t[:cost].to_d) }
    end

    # Processa ledgers para garantir formato correto
    def pusl(itm)
      pusk(itm).transform_values { |t| t.merge(asset: t[:asset].upcase, amount: t[:amount].to_d, fee: t[:fee].to_d) }
    end
  end
end
