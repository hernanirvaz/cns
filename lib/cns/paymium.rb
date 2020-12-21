# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes ledger do paymium
  class Paymium
    # @return [Apius] API paymium
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
    # @return [Paymium] API paymium - obter saldos & transacoes ledger
    def initialize(dad, pop)
      @api = Apice.new
      @bqd = dad
      @ops = pop
    end

    # @return [Array<Hash>] lista ledger paymium novos
    def ledger
      @ledger ||= exd[:kl].map { |map| map[:account_operations].select { |obj| kyl.include?(obj[:uuid]) } }.flatten
    end

    # @return [String] texto saldos & transacoes & ajuste dias
    def mostra_resumo
      puts("\nPAYMIUM\ntipo                paymium              bigquery")
      puts(formata_saldos(:btc))
      puts(formata_saldos(:eur))
      mostra_totais

      mostra_ledger
      return unless ledger.count.positive?

      puts("\nstring ajuste dias da ledger\n-h=#{kyl.map { |obj| "#{obj}:0" }.join(' ')}")
    end

    # @return [Hash] dados exchange paymium - saldos & transacoes ledger
    def exd
      @exd ||= {
        sl: api.account_fr,
        kl: api.ledger_fr
      }
    end

    # @return [Array<String>] lista txid dos ledger novos
    def kyl
      @kyl ||= exd[:kl].map { |oex| oex[:account_operations].map { |obj| obj[:uuid] } }.flatten -
               (ops[:t] ? [] : bqd[:nl].map { |obq| obq[:txid] })
    end

    # @example (see Apice#account_fr)
    # @param [Symbol] bqm symbol paymium da moeda
    # @return [String] texto formatado saldos
    def formata_saldos(bqm)
      vbq = bqd[:sl][bqm].to_d
      vkr = exd[:sl]["balance_#{bqm}".to_sym].to_d
      format(
        '%<mo>-5.5s %<kr>21.9f %<bq>21.9f %<ok>3.3s',
        mo: bqm.upcase,
        kr: vkr,
        bq: vbq,
        ok: vkr == vbq ? 'OK' : 'NOK'
      )
    end

    # @example (see Apice#ledger_fr)
    # @param (see Bigquery#frl_val)
    # @return [String] texto formatado ledger
    def formata_ledger(hlx)
      format(
        '%<ky>-18.18s %<dt>19.19s %<ty>-17.17s %<mo>-4.4s %<vl>18.7f',
        ky: formata_uuid(hlx[:uuid], 18),
        dt: Time.at(hlx[:created_at_int]),
        ty: hlx[:name],
        mo: hlx[:currency].upcase,
        vl: hlx[:amount].to_d
      )
    end

    # @param [String] uid identificacor da ledger
    # @param [Integer] max chars a mostrar
    # @return [String] texto formatado identificacor da ledger
    def formata_uuid(uid, max)
      int = Integer(max / 2)
      max < 7 ? 'erro' : "#{uid[0, int]}#{uid[-int..]}"
    end

    # @return [String] texto totais numero de transacoes
    def mostra_totais
      vkl = exd[:kl].map { |obj| obj[:account_operations].count }.flatten.inject(:+)
      vnl = bqd[:nl].count

      puts("LEDGER #{format('%<c>20i %<d>21i %<o>3.3s', c: vkl, d: vnl, o: vkl == vnl ? 'OK' : 'NOK')}")
    end

    # @return [String] texto transacoes ledger
    def mostra_ledger
      return unless ops[:v] && ledger.count.positive?

      puts("\nledger             data       hora     tipo              moeda        quantidade")
      ledger.sort { |ant, prx| prx[:created_at_int] <=> ant[:created_at_int] }.each { |obj| puts(formata_ledger(obj)) }
    end
  end
end
