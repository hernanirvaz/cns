# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # (see Greymass)
  class Greymass
    # @return [Apigm] API greymass
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :dbq
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    # @return [Greymass] API greymass - processar transacoes
    def initialize(dad, pop)
      @api = Apigm.new
      @dbq = dad
      @ops = pop
    end

    # @return [Array<String>] lista dos meus enderecos
    def lax
      @lax ||= dbq[:wb].map { |h| h[:ax] }
    end

    # @return [Array<Hash>] todos os dados greymass - saldos & transacoes
    def dbc
      @dbc ||= dbq[:wb].map { |e| base_bc(e) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & greymass
    def dados
      @dados ||= dbq[:wb].map { |b| bq_bc(b, dbc.select { |s| b[:ax] == s[:ax] }.first) }
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def bnt
      @bnt ||= (dbc.map { |e| e[:tx].map { |n| n[:itx] } }.flatten - (ops[:t] ? [] : dbq[:nt].map { |t| t[:itx] }))
    end

    # @return [Array<Hash>] lista transacoes novas
    def novax
      @novax ||= dbc.map { |e| e[:tx].select { |s| bnt.include?(s[:itx]) } }.flatten
    end

    # @param [Hash] hbq dados bigquery wallet
    # @return [Hash] dados greymass - address, saldo & transacoes
    def base_bc(hbq)
      a = hbq[:ax]
      {
        ax: a,
        sl: greymass_sl(a).inject(:+),
        tx: filtrar_tx(a, api.all_tx(a))
      }
    end

    # @param hbq (see base_bc)
    # @param [Hash] hbc dados greymass
    # @return [Hash] dados juntos bigquery & greymass
    def bq_bc(hbq, hbc)
      {
        id: hbq[:id],
        ax: hbq[:ax],
        bs: hbq[:sl],
        bt: dbq[:nt].select { |t| t[:iax] == hbq[:ax] },
        es: hbc[:sl],
        et: hbc[:tx]
      }
    end

    # @param (see filtrar_tx)
    # @return [Array<BigDecimal>] lista recursos - liquido, net, spu
    def greymass_sl(add)
      v = api.account(account_name: add)
      [
        v[:core_liquid_balance].to_d,
        v[:total_resources][:net_weight].to_d,
        v[:total_resources][:cpu_weight].to_d
      ]
    end

    # @param [String] add endereco carteira EOS
    # @param [Array<Hash>] ary lista das transacoes
    # @return [Array<Hash>] lista transacoes ligadas a uma carteira filtrada
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
