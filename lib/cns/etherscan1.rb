# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # chaves a eliminar da API - resultado deve ser ignirado pois muda a cada pedido API feito
  DL = %i[cumulativeGasUsed confirmations].freeze

  # (see Etherscan)
  class Etherscan
    # @return [Apies] API etherscan
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :dbq
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes normais & tokens?
    # @return [Etherscan] API etherscan - processar transacoes normais e tokens
    def initialize(dad, pop)
      @api = Apies.new
      @dbq = dad
      @ops = pop
    end

    # @return [Array<String>] lista dos meus enderecos
    def lax
      @lax ||= dbq[:wb].map { |h| h[:ax] }
    end

    # @return [Array<Hash>] todos os dados etherscan - saldos & transacoes
    def dbc
      @dbc ||= api.account(lax).map { |e| base_bc(e) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & etherscan
    def dados
      @dados ||= dbq[:wb].map { |b| bq_bc(b, dbc.select { |s| b[:ax] == s[:ax] }.first) }
    end

    # @return [Array<Integer>] lista indices transacoes normais novas
    def bnt
      @bnt ||= (dbc.map { |e| e[:tx].map { |n| n[:itx] } }.flatten - (ops[:t] ? [] : dbq[:nt].map { |t| t[:itx] }))
    end

    # @return [Array<Integer>] lista indices transacoes token novas
    def bnk
      @bnk ||= (dbc.map { |e| e[:kx].map { |n| n[:itx] } }.flatten - (ops[:t] ? [] : dbq[:nk].map { |t| t[:itx] }))
    end

    # @return [Array<Hash>] lista transacoes normais novas
    def novtx
      @novtx ||= dbc.map { |e| e[:tx].select { |n| bnt.include?(n[:itx]) } }.flatten
    end

    # @return [Array<Hash>] lista transacoes token novas
    def novkx
      @novkx ||= dbc.map { |e| e[:kx].select { |n| bnk.include?(n[:itx]) } }.flatten
    end

    # @param [Hash] hbc dados etherscan
    # @return [Hash] dados etherscan - address, saldo & transacoes
    def base_bc(hbc)
      a = hbc[:account]
      {
        ax: a,
        sl: (hbc[:balance].to_d / 10**18).round(10),
        tx: filtrar_tx(a, api.norml_tx(a)),
        kx: filtrar_tx(a, api.token_tx(a))
      }
    end

    # @param [Hash] hbq dados bigquery
    # @param hbc (see base_bc)
    # @return [Hash] dados juntos bigquery & etherscan
    def bq_bc(hbq, hbc)
      {
        id: hbq[:id],
        ax: hbq[:ax],
        bs: hbq[:sl],
        bt: dbq[:nt].select { |t| t[:iax] == hbq[:ax] },
        bk: dbq[:nk].select { |t| t[:iax] == hbq[:ax] },
        es: hbc[:sl],
        et: hbc[:tx],
        ek: hbc[:kx]
      }
    end

    # @param [String] add endereco carteira ETH
    # @param [Array<Hash>] ary lista das transacoes
    # @return [Array<Hash>] devolve lista de transacoes/token transfer events filtrada
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
