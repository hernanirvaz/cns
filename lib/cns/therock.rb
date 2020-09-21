# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes ledger do therock
  class TheRock
    # @return [Apius] API therock
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
    # @return [TheRock] API therock - obter saldos & transacoes ledger
    def initialize(dad, pop)
      @api = Apice.new
      @bqd = dad
      @ops = pop
    end

    # @return [Array<Hash>] lista ledger therock novos
    def ledger
      @ledger ||= exd[:kl].select { |o| kyl.include?(o[:id]) }
    end

    # @return [String] texto saldos & transacoes & ajuste dias
    def mostra_resumo
      puts("\nTHEROCK\ntipo                therock              bigquery")
      exd[:sl].each { |h| puts(formata_saldos(h)) }
      mostra_totais

      mostra_ledger
      return unless ledger.count.positive?

      puts("\nstring ajuste dias da ledger\n-h=#{kyl.map { |e| "#{e}:0" }.join(' ')}")
    end

    # @return [Hash] dados exchange therock - saldos & transacoes ledger
    def exd
      @exd ||= {
        sl: api.account_mt,
        kl: api.ledger_mt
      }
    end

    # @return [Array<String>] lista txid dos ledger novos
    def kyl
      @kyl ||= exd[:kl].map { |h| h[:id] } - (ops[:t] ? [] : bqd[:nl].map { |e| e[:txid] })
    end

    # @example (see Apice#account_mt)
    # @param [Hash] hsl saldo therock da moeda
    # @return [String] texto formatado saldos
    def formata_saldos(hsl)
      b = bqd[:sl][hsl[:currency].downcase.to_sym].to_d
      k = hsl[:balance].to_d
      format(
        '%<mo>-5.5s %<kr>21.9f %<bq>21.9f %<ok>3.3s',
        mo: hsl[:currency].upcase,
        kr: k,
        bq: b,
        ok: k == b ? 'OK' : 'NOK'
      )
    end

    # @example (see Apice#ledger_mt)
    # @param (see Bigquery#mtl_val1)
    # @return [String] texto formatado ledger
    def formata_ledger(hlx)
      format(
        '%<ky>6i %<dt>19.19s %<ty>-27.27s %<mo>-4.4s %<vl>20.7f',
        ky: hlx[:id],
        dt: Time.parse(hlx[:date]),
        ty: hlx[:type],
        mo: hlx[:currency].upcase,
        vl: hlx[:price].to_d
      )
    end

    # @return [String] texto totais numero de transacoes
    def mostra_totais
      c = exd[:kl].count
      d = bqd[:nl].count

      puts("LEDGER #{format('%<c>20i %<d>21i %<o>3.3s', c: c, d: d, o: c == d ? 'OK' : 'NOK')}")
    end

    # @return [String] texto transacoes ledger
    def mostra_ledger
      return unless ops[:v] && ledger.count.positive?

      puts("\nledger data       hora     tipo                        moeda ---------quantidade")
      ledger.sort { |a, b| b[:id] <=> a[:id] }.each { |o| puts(formata_ledger(o)) }
    end
  end
end
