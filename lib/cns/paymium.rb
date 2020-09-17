# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar saldos & transacoes ledger
  class Paymium
    # @return [Apius] API paymium
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
    # @return [Paymium] API paymium - obter saldos & transacoes ledger
    def initialize(dad, pop)
      # API paymium base
      @api = Apifr.new
      @dbq = dad
      @ops = pop
    end

    # @return [Hash] dados exchange paymium - saldos & transacoes ledger
    def exd
      @exd ||= {
        sl: api.account,
        kl: api.ledger
      }
    end

    # @return [Array<String>] lista txid de transacoes ledger
    def kyl
      @kyl ||= exd[:kl].map { |h| h[:account_operations].map { |o| o[:uuid] } }.flatten -
               (ops[:t] ? [] : dbq[:nl].map { |e| e[:txid] })
    end

    # @return [Hash] transacoes ledger
    def ledger
      @ledger ||= exd[:kl].map { |h| h[:account_operations].select { |o| kyl.include?(o[:uuid]) } }.flatten
    end

    # @example (see Apifr#account)
    # @param [Symbol] bqm symbol paymium da moeda
    # @return [String] texto formatado saldos (paymium/bigquery) & iguais/ok/nok?
    def formata_saldos(bqm)
      b = dbq[:sl][bqm].to_d
      t = exd[:sl]["balance_#{bqm}".to_sym].to_d
      format(
        '%<mo>-5.5s %<kr>21.9f %<bq>21.9f %<ok>3.3s',
        mo: bqm.upcase,
        kr: t,
        bq: b,
        ok: t == b ? 'OK' : 'NOK'
      )
    end

    # @example (see Apifr#ledger)
    # @param (see Bigquery#frl_val)
    # @return [String] texto formatado transacao ledger
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

    # @example (see Apifr#ledger)
    # @param [String] uid identificacor da ledger apifr
    # @param [Integer] max chars a mostrar
    # @return [String] texto formatado identificacor da ledger apifr
    def formata_uuid(uid, max)
      i = Integer(max / 2)
      max < 7 ? 'erro' : "#{uid[0, i]}#{uid[-i..]}"
    end

    # @return [String] texto saldos & transacoes & ajuste dias
    def mostra_resumo
      puts("\nPAYMIUM\nmoeda         saldo paymium        saldo bigquery")
      puts(formata_saldos(:btc))
      puts(formata_saldos(:eur))

      mostra_ledger
      return unless ledger.count.positive?

      puts("\nstring ajuste dias da ledger\n-h=#{kyl.map { |e| "#{e}:0" }.join(' ')}")
    end

    # @return [String] texto transacoes ledger
    def mostra_ledger
      return unless ops[:v] && ledger.count.positive?

      puts("\nledger             data       hora     tipo              moeda -------quantidade")
      ledger.sort { |a, b| b[:created_at_int] <=> a[:created_at_int] }.each { |o| puts(formata_ledger(o)) }
    end
  end
end
