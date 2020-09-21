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
      @novtx ||= bcd.map { |e| e[:tx].select { |n| idt.include?(n[:itx]) } }.flatten
    end

    # @return [Array<Hash>] lista transacoes token novas
    def novkx
      @novkx ||= bcd.map { |e| e[:kx].select { |n| idk.include?(n[:itx]) } }.flatten
    end

    # @return [Array<String>] lista dos meus enderecos
    def lax
      @lax ||= bqd[:wb].map { |h| h[:ax] }
    end

    # @return [Array<Hash>] todos os dados etherscan - saldos & transacoes
    def bcd
      @bcd ||= api.account_es(lax).map { |e| base_bc(e) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & etherscan
    def dados
      @dados ||= bqd[:wb].map { |b| bq_bc(b, bcd.select { |s| b[:ax] == s[:ax] }.first) }
    end

    # @return [Array<Integer>] lista indices transacoes normais novas
    def idt
      @idt ||= (bcd.map { |e| e[:tx].map { |n| n[:itx] } }.flatten - (ops[:t] ? [] : bqd[:nt].map { |t| t[:itx] }))
    end

    # @return [Array<Integer>] lista indices transacoes token novas
    def idk
      @idk ||= (bcd.map { |e| e[:kx].map { |n| n[:itx] } }.flatten - (ops[:t] ? [] : bqd[:nk].map { |t| t[:itx] }))
    end

    # @example (see Apibc#account_es)
    # @param [Hash] abc account etherscan
    # @return [Hash] dados etherscan - address, saldo & transacoes
    def base_bc(abc)
      a = abc[:account]
      {
        ax: a,
        sl: (abc[:balance].to_d / 10**18).round(10),
        tx: filtrar_tx(a, api.norml_es(a)),
        kx: filtrar_tx(a, api.token_es(a))
      }
    end

    # @param [Hash] wbq wallet bigquery
    # @param [Hash] hbc dados etherscan - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & etherscan
    def bq_bc(wbq, hbc)
      {
        id: wbq[:id],
        ax: wbq[:ax],
        bs: wbq[:sl],
        bt: bqd[:nt].select { |t| t[:iax] == wbq[:ax] },
        bk: bqd[:nk].select { |t| t[:iax] == wbq[:ax] },
        es: hbc[:sl],
        et: hbc[:tx],
        ek: hbc[:kx]
      }
    end

    # @param add (see Apibc#norml_es)
    # @param [Array<Hash>] ary lista transacoes/token events
    # @return [Array<Hash>] lista transacoes/token events filtrada
    def filtrar_tx(add, ary)
      # elimina transferencia from: (lax) to: (add) - esta transferencia aparece em from: (add) to: (lax)
      # elimina chaves irrelevantes (DL) & adiciona chave indice itx & adiciona identificador da carteira iax
      ary.delete_if { |h| h[:to] == add && lax.include?(h[:from]) }
         .map { |h| h.delete_if { |k, _| DL.include?(k) }.merge(itx: Integer(h[:blockNumber]), iax: add) }
    end

    # @return [Array<Hash>] lista ordenada transacoes normais novas
    def sortx
      novtx.sort { |a, b| a[:itx] <=> b[:itx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes token novas
    def sorkx
      novkx.sort { |a, b| a[:itx] <=> b[:itx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes (normais & token) novas
    def sorax
      (novtx + novkx).sort { |a, b| a[:itx] <=> b[:itx] }
    end
  end
end
