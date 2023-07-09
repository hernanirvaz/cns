# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # chaves a eliminar da API - resultado deve ser ignirado pois muda a cada pedido API feito
  DL = %i[cumulativeGasUsed confirmations].freeze

  # (see Etherscan)
  class Etherscan
    # @return [Apibc] API blockchains
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :bqd
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes normais & tokens?
    # @return [Etherscan] API etherscan - processar transacoes normais e tokens
    def initialize(dad, pop)
      @api = Apibc.new
      @bqd = dad
      @ops = pop
    end

    # @return [Array<Hash>] lista transacoes normais novas
    def novtx
      @novtx ||= bcd.map { |obc| obc[:tx].select { |obj| idt.include?(obj[:itx]) } }.flatten
    end

    # @return [Array<Hash>] lista transacoes internas novas
    def novix
      @novix ||= bcd.map { |obc| obc[:ix].select { |obj| idi.include?(obj[:itx]) } }.flatten
    end

    # @return [Array<Hash>] lista transacoes block novas
    def novpx
      @novpx ||= bcd.map { |obc| obc[:px].select { |obj| idp.include?(obj[:itx]) } }.flatten
    end

    # @return [Array<Hash>] lista transacoes withdrawals novas
    def novwx
      @novwx ||= bcd.map { |obc| obc[:wx].select { |obj| idw.include?(obj[:itx]) } }.flatten
    end

    # @return [Array<Hash>] lista transacoes token novas
    def novkx
      @novkx ||= bcd.map { |obc| obc[:kx].select { |obj| idk.include?(obj[:itx]) } }.flatten
    end

    # @return [Array<String>] lista dos meus enderecos
    def lax
      @lax ||= bqd[:wb].map { |obj| obj[:ax] }
    end

    # @return [Array<Hash>] todos os dados etherscan - saldos & transacoes
    def bcd
      @bcd ||= api.account_es(lax).map { |obj| base_bc(obj) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & etherscan
    def dados
      @dados ||= bqd[:wb].map { |obq| bq_bc(obq, bcd.select { |obc| obq[:ax] == obc[:ax] }.first) }
    end

    # @return [Array<Integer>] lista indices transacoes normais novas
    def idt
      @idt ||= bcd.map { |obc| obc[:tx].map { |obj| obj[:itx] } }.flatten -
               (ops[:t] ? [] : bqd[:nt].map { |obq| obq[:itx] })
    end

    # @return [Array<Integer>] lista indices transacoes internas novas
    def idi
      @idi ||= bcd.map { |obc| obc[:ix].map { |obj| obj[:itx] } }.flatten -
               (ops[:t] ? [] : bqd[:ni].map { |obq| obq[:itx] })
    end

    # @return [Array<Integer>] lista indices transacoes block novas
    def idp
      @idp ||= bcd.map { |obc| obc[:px].map { |obj| obj[:itx] } }.flatten -
               (ops[:t] ? [] : bqd[:np].map { |obq| obq[:itx] })
    end

    # @return [Array<Integer>] lista indices transacoes withdrawals novas
    def idw
      @idw ||= bcd.map { |obc| obc[:wx].map { |obj| obj[:itx] } }.flatten -
               (ops[:t] ? [] : bqd[:nw].map { |obq| obq[:itx] })
    end

    # @return [Array<Integer>] lista indices transacoes token novas
    def idk
      @idk ||= bcd.map { |obc| obc[:kx].map { |obj| obj[:itx] } }.flatten -
               (ops[:t] ? [] : bqd[:nk].map { |obq| obq[:itx] })
    end

    # @example (see Apibc#account_es)
    # @param [Hash] abc account etherscan
    # @return [Hash] dados etherscan - address, saldo & transacoes
    def base_bc(abc)
      acc = abc[:account].downcase
      {
        ax: acc,
        sl: (abc[:balance].to_d / 10**18),
        tx: filtrar_tx(acc, api.norml_es(acc)),
        ix: filtrar_tx(acc, api.inter_es(acc)),
        px: filtrar_px(acc, api.block_es(acc)),
        wx: filtrar_px(acc, api.withw_es(acc)),
        kx: filtrar_tx(acc, api.token_es(acc))
      }
    end

    # @param [Hash] wbq wallet bigquery
    # @param [Hash] hbc dados etherscan - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & etherscan
    def bq_bc(wbq, hbc)
      {
        id: wbq[:id],
        ax: xbq = wbq[:ax],
        bs: wbq[:sl],
        bt: bqd[:nt].select { |ont| ont[:iax] == xbq },
        bi: bqd[:ni].select { |oni| oni[:iax] == xbq },
        bp: bqd[:np].select { |onp| onp[:iax] == xbq },
        bw: bqd[:nw].select { |onw| onw[:iax] == xbq },
        bk: bqd[:nk].select { |onk| onk[:iax] == xbq },
        es: hbc[:sl],
        et: hbc[:tx],
        ei: hbc[:ix],
        ep: hbc[:px],
        ew: hbc[:wx],
        ek: hbc[:kx]
      }
    end

    # @param add (see Apibc#norml_es)
    # @param [Array<Hash>] ary lista transacoes/token events
    # @return [Array<Hash>] lista transacoes/token events filtrada
    def filtrar_tx(add, ary)
      # elimina transferencia from: (lax) to: (add) - esta transferencia aparece em from: (add) to: (lax)
      # elimina chaves irrelevantes (DL) & adiciona chave indice itx & adiciona identificador da carteira iax
      ary.delete_if { |odl| add.casecmp?(odl[:to]) && lax.include?(odl[:from].downcase) }
         .map { |omp| omp.delete_if { |key, _| DL.include?(key) }.merge(itx: Integer(omp[:blockNumber]), iax: add) }
    end

    # @param add (see Apibc#norml_es)
    # @param [Array<Hash>] ary lista blocks events
    # @return [Array<Hash>] lista blocks events filtrada
    def filtrar_px(add, ary)
      # adiciona chave indice itx & adiciona identificador da carteira iax
      ary.map { |omp| omp.merge(itx: Integer(omp[:blockNumber]), iax: add) }
    end

    # @return [Array<Hash>] lista ordenada transacoes normais novas
    def sortx
      novtx.sort { |ant, prx| ant[:itx] <=> prx[:itx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes internas novas
    def sorix
      novix.sort { |ant, prx| ant[:itx] <=> prx[:itx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes block novas
    def sorpx
      novpx.sort { |ant, prx| ant[:itx] <=> prx[:itx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes withdrawals novas
    def sorwx
      novwx.sort { |ant, prx| ant[:itx] <=> prx[:itx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes token novas
    def sorkx
      novkx.sort { |ant, prx| ant[:itx] <=> prx[:itx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes (normais & token) novas
    def sorax
      (novtx + novkx).sort { |ant, prx| ant[:itx] <=> prx[:itx] }
    end
  end
end
