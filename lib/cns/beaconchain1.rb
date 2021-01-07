# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # (see Beaconchain)
  class Beaconchain
    # @return [Apibc] API blockchains
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :bqd
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Boolean] :v (false) mostra saldos?
    # @option pop [Boolean] :t (false) mostra todos saldos ou somente novos?
    # @return [Beaconchain] API beaconchain - processar historico saldos
    def initialize(dad, pop)
      @api = Apibc.new
      @bqd = dad
      @ops = pop
    end

    # @return [Array<Hash>] lista balancos novos
    def nov
      @nov ||= bcd.map { |obc| obc[:bx].select { |obj| idb.include?(itx(obj[:epoch], obj[:validatorindex])) } }.flatten
    end

    # @return [Array<Integer>] lista dos meus validators
    def lax
      @lax ||= bqd[:wb].map { |obj| obj[:id] }
    end

    # @return [Array<Hash>] todos os dados beaconchain - saldos & historico
    def bcd
      @bcd ||= api.data_bc("/api/v1/validator/#{lax.join(',')}").map { |obj| base_bc(obj) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & beaconchain
    def dados
      @dados ||= bqd[:wb].map { |obq| bq_bc(obq, bcd.select { |obc| obq[:id] == obc[:ax] }.first) }
    end

    # @return [Array<Integer>] lista historicos novos
    def idb
      @idb ||= bcd.map { |obc| obc[:bx].map { |obj| itx(obj[:epoch], obj[:validatorindex]) } }.flatten -
               (ops[:t] ? [] : bqd[:nb].map { |obq| obq[:itx] })
    end

    # @param [Integer] intum
    # @param [Integer] intdois
    # @return [Integer] szudzik pairing two integers
    def itx(intum, intdois)
      intum >= intdois ? intum * intum + intum + intdois : intum + intdois * intdois
    end

    # @example
    #  {
    #    activationeligibilityepoch: 0,
    #    activationepoch: 0,
    #    balance: 32_489_497_108,
    #    effectivebalance: 32_000_000_000,
    #    exitepoch: 9_223_372_036_854_775_807,
    #    lastattestationslot: 265_446,
    #    name: '',
    #    pubkey: '0x93bf23a587f11f9eca329a12ef51296b8a9848af8c0fe61201524b14cb85b0c6fbd3e427501cdfa3b28719bd1ed96fff',
    #    slashed: false,
    #    status: 'active_online',
    #    validatorindex: 11_766,
    #    withdrawableepoch: 9_223_372_036_854_775_807,
    #    withdrawalcredentials: '0x004f11be01cb72187715c55d6348c67c5a3880687cd42692306fdbc91ac2da9b'
    #  }
    # @param [Hash] abc account beaconchain
    # @return [Hash] dados beaconchain - index, saldo & historico
    def base_bc(abc)
      acc = abc[:validatorindex]
      {
        ax: acc,
        sl: (abc[:balance].to_d / 10**9).round(10),
        bx: api.data_bc("/api/v1/validator/#{acc}/balancehistory")
      }
    end

    # @param [Hash] wbq wallet bigquery
    # @param abc (see base_bc)
    # @return [Hash] dados juntos bigquery & beaconchain
    def bq_bc(wbq, abc)
      xbq = wbq[:id]
      {
        id: xbq,
        ax: wbq[:ax],
        bs: wbq[:sl],
        bb: bqd[:nb].select { |onb| onb[:iax] == xbq },
        es: abc[:sl],
        eb: abc[:bx]
      }
    end
  end
end
