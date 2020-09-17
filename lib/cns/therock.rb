# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar saldos & transacoes ledger
  class TheRock
    # @return [Apius] API therock
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
    # @return [TheRock] API therock - obter saldos & transacoes ledger
    def initialize(dad, pop)
      # API therock base
      @api = Apimt.new
      @dbq = dad
      @ops = pop
    end

    # @return [Hash] dados exchange therock - saldos & transacoes ledger
    def exd
      @exd ||= {
        sl: api.account,
        kl: api.ledger
      }
    end

    # @return [Array<String>] lista txid de transacoes ledger
    def kyl
      @kyl ||= exd[:kl].map { |h| h[:id] } - (ops[:t] ? [] : dbq[:nl].map { |e| e[:txid] })
    end

    # @return [Hash] transacoes ledger
    def ledger
      @ledger ||= exd[:kl].select { |o| kyl.include?(o[:id]) }
    end

    # @example (see Apimt#account)
    # @param [Hash] hsl saldo therock da moeda
    # @return [String] texto formatado saldos (therock/bigquery) & iguais/ok/nok?
    def formata_saldos(hsl)
      b = dbq[:sl][hsl[:currency].downcase.to_sym].to_d
      k = hsl[:balance].to_d
      format(
        '%<mo>-5.5s %<kr>21.9f %<bq>21.9f %<ok>3.3s',
        mo: hsl[:currency].upcase,
        kr: k,
        bq: b,
        ok: k == b ? 'OK' : 'NOK'
      )
    end

    # @example (see Apimt#ledger)
    # @param (see Bigquery#mtl_val1)
    # @return [String] texto formatado transacao ledger
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

    # @return [String] texto saldos & transacoes & ajuste dias
    def mostra_resumo
      puts("\nTHEROCK\nmoeda         saldo therock        saldo bigquery")
      exd[:sl].each { |h| puts(formata_saldos(h)) }

      mostra_ledger
      return unless ledger.count.positive?

      puts("\nstring ajuste dias da ledger\n-h=#{kyl.map { |e| "#{e}:0" }.join(' ')}")
    end

    # @return [String] texto transacoes ledger
    def mostra_ledger
      return unless ops[:v] && ledger.count.positive?

      puts("\nledger data       hora     tipo                        moeda ---------quantidade")
      ledger.sort { |a, b| b[:id] <=> a[:id] }.each { |o| puts(formata_ledger(o)) }
    end
  end
end
