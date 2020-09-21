# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # (see Greymass)
  class Greymass
    # @return [Apibc] API blockchains
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :bqd
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    # @return [Greymass] API greymass - processar transacoes
    def initialize(dad, pop)
      @api = Apibc.new
      @bqd = dad
      @ops = pop
    end

    # @return [Array<Hash>] lista transacoes novas
    def novax
      @novax ||= bcd.map { |e| e[:tx].select { |s| idt.include?(s[:itx]) } }.flatten
    end

    # @return [Array<String>] lista dos meus enderecos
    def lax
      @lax ||= bqd[:wb].map { |h| h[:ax] }
    end

    # @return [Array<Hash>] todos os dados greymass - saldos & transacoes
    def bcd
      @bcd ||= bqd[:wb].map { |e| base_bc(e) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & greymass
    def dados
      @dados ||= bqd[:wb].map { |b| bq_bc(b, bcd.select { |s| b[:ax] == s[:ax] }.first) }
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def idt
      @idt ||= (bcd.map { |e| e[:tx].map { |n| n[:itx] } }.flatten - (ops[:t] ? [] : bqd[:nt].map { |t| t[:itx] }))
    end

    # @example (see Apibc#account_gm)
    # @param [Hash] wbq wallet bigquery
    # @return [Hash] dados greymass - address, saldo & transacoes
    def base_bc(wbq)
      a = wbq[:ax]
      {
        ax: a,
        sl: greymass_sl(a).inject(:+),
        tx: filtrar_tx(a, api.ledger_gm(a))
      }
    end

    # @param wbq (see base_bc)
    # @param [Hash] hbc dados greymass - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & greymass
    def bq_bc(wbq, hbc)
      {
        id: wbq[:id],
        ax: wbq[:ax],
        bs: wbq[:sl],
        bt: bqd[:nt].select { |t| t[:iax] == wbq[:ax] },
        es: hbc[:sl],
        et: hbc[:tx]
      }
    end

    # @param (see filtrar_tx)
    # @return [Array<BigDecimal>] lista recursos - liquido, net, spu
    def greymass_sl(add)
      v = api.account_gm(add)
      [
        v[:core_liquid_balance].to_d,
        v[:total_resources][:net_weight].to_d,
        v[:total_resources][:cpu_weight].to_d
      ]
    end

    # @param add (see Apibc#account_gm)
    # @param [Array<Hash>] ary lista transacoes
    # @return [Array<Hash>] lista transacoes filtrada
    def filtrar_tx(add, ary)
      # elimina transferencia from: (lax) to: (add) - esta transferencia aparece em from: (add) to: (lax)
      # adiciona chave indice itx & adiciona identificador da carteira iax
      ary.delete_if { |h| act_data(h)[:to] == add && lax.include?(act_data(h)[:from]) }
         .map { |h| h.merge(itx: h[:global_action_seq], iax: add) }
    end

    # @return [Array<Hash>] lista ordenada transacoes novas
    def sorax
      novax.sort { |a, b| b[:itx] <=> a[:itx] }
    end
  end
end
