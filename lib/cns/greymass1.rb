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
      @novax ||= bcd.map { |obc| obc[:tx].select { |obj| idt.include?(obj[:itx]) } }.flatten
    end

    # @return [Array<String>] lista dos meus enderecos
    def lax
      @lax ||= bqd[:wb].map { |obj| obj[:ax] }
    end

    # @return [Array<Hash>] todos os dados greymass - saldos & transacoes
    def bcd
      @bcd ||= bqd[:wb].map { |obj| base_bc(obj) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & greymass
    def dados
      @dados ||= bqd[:wb].map { |obq| bq_bc(obq, bcd.select { |obj| obq[:ax] == obj[:ax] }.first) }
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def idt
      @idt ||= bcd.map { |obc| obc[:tx].map { |obj| obj[:itx] } }.flatten -
               (ops[:t] ? [] : bqd[:nt].map { |obq| obq[:itx] })
    end

    # @example (see Apibc#account_gm)
    # @param [Hash] wbq wallet bigquery
    # @return [Hash] dados greymass - address, saldo & transacoes
    def base_bc(wbq)
      xbq = wbq[:ax]
      {
        ax: xbq,
        sl: greymass_sl(xbq).inject(:+),
        tx: filtrar_tx(xbq, api.ledger_gm(xbq))
      }
    end

    # @param wbq (see base_bc)
    # @param [Hash] hbc dados greymass - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & greymass
    def bq_bc(wbq, hbc)
      xbq = wbq[:ax]
      {
        id: wbq[:id],
        ax: xbq,
        bs: wbq[:sl],
        bt: bqd[:nt].select { |obj| obj[:iax] == xbq },
        es: hbc[:sl],
        et: hbc[:tx]
      }
    end

    # @param (see filtrar_tx)
    # @return [Array<BigDecimal>] lista recursos - liquido, net, spu
    def greymass_sl(add)
      hac = api.account_gm(add)
      htr = hac[:total_resources]
      [
        hac[:core_liquid_balance].to_d,
        htr[:net_weight].to_d,
        htr[:cpu_weight].to_d
      ]
    end

    # @param add (see Apibc#account_gm)
    # @param [Array<Hash>] ary lista transacoes
    # @return [Array<Hash>] lista transacoes filtrada
    def filtrar_tx(add, ary)
      # elimina transferencia from: (lax) to: (add) - esta transferencia aparece em from: (add) to: (lax)
      # adiciona chave indice itx & adiciona identificador da carteira iax
      (ary.delete_if do |odl|
        adt = odl[:action_trace][:act][:data]
        adt[:to] == add && lax.include?(adt[:from])
      end).map { |omp| omp.merge(itx: omp[:global_action_seq], iax: add) }
    end

    # @return [Array<Hash>] lista ordenada transacoes novas
    def sorax
      novax.sort { |ant, prx| prx[:itx] <=> ant[:itx] }
    end
  end
end
